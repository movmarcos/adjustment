CREATE OR REPLACE PROCEDURE DVLP_RAPTOR_NEWADJ.FACT.LOAD_VAR_ADJUSTMENT_UPLOAD("DEBUG_FLAG" VARCHAR(1))
RETURNS VARCHAR(100)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    function fix_binds(param) {
          return param === undefined ? null
              : typeof(param) == "object" ? (param instanceof Date ? param.toJSON() : JSON.stringify(param))
              : param;
    }

    try {   
        
        // Merge
        var sqlMerge = `
        MERGE INTO DIMENSION.ADJUSTMENT AS tgt
        USING (
                SELECT COBID,
                    ENTITY_CODE,
                    PROCESS_TYPE,
                    ADJUSTMENT_TYPE,
                    SUM(ADJUSTMENT_VALUE_IN_USD) ADJUSTMENT_VALUE_IN_USD,
                    USERNAME,
                    RUN_STATUS,
                    FILE_NAME,
                    GLOBAL_REFERENCE,
                    CONCAT(CATEGORY, ''|'', DETAIL) AS REASON,
                    COUNT(*) - SUM(IFF(IS_DELETED = TRUE, 1,0)) AS COUNT_ACTIVE,
                    MAX(SPLIT_PART(SPLIT_PART(FILE_NAME,''_'',-1), ''.'' ,1 )) AS MSSQL_ADJUSTMENT_ID,
					BOOK_CODE,
					DEPARTMENT_CODE
                FROM ADJUSTMENT.IDM_VAR_UPLOAD
                WHERE RUN_STATUS = ''Pending''
                GROUP BY COBID, ENTITY_CODE, PROCESS_TYPE, ADJUSTMENT_TYPE, USERNAME, FILE_NAME , RUN_STATUS, GLOBAL_REFERENCE, CATEGORY, DETAIL, BOOK_CODE, DEPARTMENT_CODE
              ) AS src
        ON src.GLOBAL_REFERENCE = tgt.GLOBAL_REFERENCE
        AND src.REASON = tgt.REASON
        WHEN MATCHED THEN UPDATE
        SET
            tgt.CREATED_DATE = CURRENT_TIMESTAMP(),
            tgt.RUN_STATUS = src.RUN_STATUS,
            tgt.ADJUSTMENT_VALUE_IN_USD = src.ADJUSTMENT_VALUE_IN_USD,
            tgt.RECORD_COUNT = src.COUNT_ACTIVE,
            tgt.IS_DELETED = IFF(src.COUNT_ACTIVE = 0, TRUE, FALSE),
            tgt.MSSQL_ADJUSTMENT_ID = src.MSSQL_ADJUSTMENT_ID
        WHEN NOT MATCHED THEN 
        INSERT (
            COBID, 
            ENTITY_CODE, 
            PROCESS_TYPE, 
            ADJUSTMENT_TYPE, 
            ADJUSTMENT_VALUE_IN_USD, 
            CREATED_DATE, 
            USERNAME, 
            RUN_STATUS, 
            REASON, 
            RECORD_COUNT,  
            GLOBAL_REFERENCE,
            IS_DELETED,
            FILE_NAME,
            MSSQL_ADJUSTMENT_ID,
			BOOK_CODE,
			DEPARTMENT_CODE)
        VALUES (
            src.COBID, 
            src.ENTITY_CODE, 
            src.PROCESS_TYPE, 
            src.ADJUSTMENT_TYPE, 
            src.ADJUSTMENT_VALUE_IN_USD, 
            CURRENT_TIMESTAMP(), 
            src.USERNAME, 
            src.RUN_STATUS, 
            src.REASON, 
            src.COUNT_ACTIVE, 
            src.GLOBAL_REFERENCE,
            IFF(src.COUNT_ACTIVE = 0, TRUE, FALSE),
            src.FILE_NAME,
            src.MSSQL_ADJUSTMENT_ID,
			src.BOOK_CODE,
			src.DEPARTMENT_CODE
            );`

        var rsMerge = snowflake.execute( {sqlText: sqlMerge} );

        // Merge SignedOff row
        var sqlMergeSO = `
        MERGE INTO DIMENSION.ADJUSTMENT AS tgt
        USING (
                SELECT COBID,
                    ENTITY_CODE,
                    PROCESS_TYPE,
                    ADJUSTMENT_TYPE,
                    SUM(ADJUSTMENT_VALUE_IN_USD) ADJUSTMENT_VALUE_IN_USD,
                    USERNAME,
                    RUN_STATUS,
                    FILE_NAME,
                    GLOBAL_REFERENCE,
                    CONCAT(CATEGORY, ''|'', DETAIL) AS REASON,
                    COUNT(*) - SUM(IFF(IS_DELETED = TRUE, 1,0)) AS COUNT_ACTIVE,
                    MAX(SPLIT_PART(SPLIT_PART(FILE_NAME,''_'',-1), ''.'' ,1 )) AS MSSQL_ADJUSTMENT_ID,
					BOOK_CODE,
					DEPARTMENT_CODE
                FROM ADJUSTMENT.IDM_VAR_UPLOAD
                WHERE RUN_STATUS = ''Rejected - SignedOff''
                GROUP BY COBID, ENTITY_CODE, PROCESS_TYPE, ADJUSTMENT_TYPE, USERNAME, FILE_NAME , RUN_STATUS, GLOBAL_REFERENCE, CATEGORY, DETAIL, BOOK_CODE, DEPARTMENT_CODE
              ) AS src
        ON src.GLOBAL_REFERENCE = tgt.GLOBAL_REFERENCE
        AND src.REASON = tgt.REASON
        WHEN NOT MATCHED THEN 
        INSERT (
            COBID, 
            ENTITY_CODE, 
            PROCESS_TYPE, 
            ADJUSTMENT_TYPE, 
            ADJUSTMENT_VALUE_IN_USD, 
            CREATED_DATE, 
            USERNAME, 
            RUN_STATUS, 
            REASON, 
            RECORD_COUNT,  
            GLOBAL_REFERENCE,
            IS_DELETED,
            FILE_NAME,
            MSSQL_ADJUSTMENT_ID,
			BOOK_CODE,
			DEPARTMENT_CODE )
        VALUES (
            src.COBID, 
            src.ENTITY_CODE, 
            src.PROCESS_TYPE, 
            src.ADJUSTMENT_TYPE, 
            src.ADJUSTMENT_VALUE_IN_USD, 
            CURRENT_TIMESTAMP(), 
            src.USERNAME, 
            src.RUN_STATUS, 
            src.REASON, 
            src.COUNT_ACTIVE, 
            src.GLOBAL_REFERENCE,
            IFF(src.COUNT_ACTIVE = 0, TRUE, FALSE),
            src.FILE_NAME,
            src.MSSQL_ADJUSTMENT_ID,
			src.BOOK_CODE,
			src.DEPARTMENT_CODE
            );`
        
        var rsMergeSO = snowflake.execute( {sqlText: sqlMergeSO} );
        
        // Set Run Status to Running
        snowflake.execute({
            sqlText:  `UPDATE DIMENSION.ADJUSTMENT SET RUN_STATUS = ''Running''
                   WHERE RUN_STATUS = ''Pending''
                   AND ADJUSTMENT_TYPE = ''Upload''
                   AND PROCESS_TYPE = ''VaR''`});
        
        // Run log Entry
        snowflake.execute({
            sqlText: `CREATE OR REPLACE TEMPORARY TABLE FACT.TEMP_UPLOAD_RUNLOG AS (
                SELECT BATCH.SEQ_RUN_LOG.nextval AS Run_log_id, COBID, TO_NUMBER(0) as RECORD_COUNT
                FROM (select DISTINCT COBID FROM ADJUSTMENT.IDM_VAR_UPLOAD WHERE RUN_STATUS = ''Pending''))`});
    
        snowflake.execute({
            sqlText: `INSERT INTO BATCH.RUN_LOG (RUN_LOG_ID, COBID, PROC_NAME, BATCH_ACTION_DAILY_KEY, RECORD_COUNT, ERROR, ERROR_MESSAGE)
                SELECT Run_log_id, COBID, ''FACT.LOAD_VAR_ADJUSTMENT_UPLOAD'', 0, 0, FALSE, ''''
                FROM FACT.TEMP_UPLOAD_RUNLOG`});

        // UPDATE THE RUN STATUS TO RUNNING FOR ALL ROW IN THE FILES
        snowflake.execute({
            sqlText:  `UPDATE ADJUSTMENT.IDM_VAR_UPLOAD A
                          SET RUN_STATUS = ''Running''
                        WHERE GLOBAL_REFERENCE IN (SELECT DISTINCT GLOBAL_REFERENCE 
                                                     FROM ADJUSTMENT.IDM_VAR_UPLOAD 
                                                    WHERE RUN_STATUS = ''Pending'')`});

        var sqlGetAdjustmentData = `CREATE OR REPLACE TEMPORARY TABLE FACT.TEMP_ADJUSTMENT_UPLOAD AS
        SELECT
            A.COBID,
            A.VAR_SUB_COMPONENT_ID,
            AD.ADJUSTMENT_ID,
            B.BOOK_KEY,
            IFNULL(TD.TRADE_KEY, -1) AS TRADE_KEY,
            A.CURRENCY_CODE,
            A.SOURCE_SYSTEM_CODE,
            A.ENTITY_CODE,
            IFNULL(CI.COMMON_INSTRUMENT_KEY,-1) AS COMMON_INSTRUMENT_KEY,
            A.SCENARIO_DATE_ID,
            A.ADJUSTMENT_VALUE_IN_USD AS PNL_VECTOR_VALUE_IN_USD,
            IFNULL(A.IS_DELETED,FALSE) AS IS_DELETED,
            AD.GLOBAL_REFERENCE,
            A.VAR_UPLOAD_ID
        FROM DIMENSION.ADJUSTMENT AD
            INNER JOIN ADJUSTMENT.IDM_VAR_UPLOAD A ON AD.GLOBAL_REFERENCE = A.GLOBAL_REFERENCE
            LEFT JOIN DIMENSION.TRADE TD 
                ON (
                     (TD.TRADE_CODE = A.TRADE_CODE)
                        OR 
                     (IFNULL(A.TRADE_CODE,''N/A'') = ''N/A'' AND TD.TRADE_CODE = CONCAT(A.BOOK_CODE, ''/ADJUSTMENT''))
                    )
                AND TD.BOOK_CODE  = A.BOOK_CODE
                AND TD.ENTITY_CODE = A.ENTITY_CODE
                AND TD.IS_CURRENT_ROW = 1
            INNER JOIN DIMENSION.BOOK B 
                ON (
					 (B.BOOK_CODE = A.BOOK_CODE)
						OR
					 (IFNULL(A.BOOK_CODE, ''N/A'') = ''N/A'' AND B.BOOK_CODE = CONCAT(A.DEPARTMENT_CODE, ''/ADJUSTMENT''))
					)
                AND B.IS_CURRENT_ROW =1
            LEFT JOIN DIMENSION.COMMON_INSTRUMENT CI
                ON CI.INSTRUMENT_KEY = TD.INSTRUMENT_KEY 
                AND CI.IS_CURRENT_ROW = TRUE
        WHERE AD.RUN_STATUS =''Running''`;
        
        snowflake.execute({sqlText: sqlGetAdjustmentData});
        
        //Delete existing data if adjustment has been changed
        snowflake.execute({sqlText: `
            DELETE FROM FACT.VAR_MEASURES_ADJUSTMENT v
            WHERE 
                EXISTS (SELECT 1 
                        FROM DIMENSION.ADJUSTMENT a 
                        WHERE a.ADJUSTMENT_ID = v.ADJUSTMENT_ID
                        AND a.COBID = v.COBID
                        AND a.RUN_STATUS = ''Running''
                        AND a.ADJUSTMENT_TYPE = ''Upload''
                        AND a.PROCESS_TYPE = ''VaR'')`});
    
        snowflake.execute({sqlText: `
            DELETE FROM FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY v
            WHERE EXISTS (SELECT 1 
                FROM DIMENSION.ADJUSTMENT a 
                WHERE a.ADJUSTMENT_ID = v.ADJUSTMENT_ID
                AND a.COBID = v.COBID
                AND a.RUN_STATUS = ''Running''
                AND a.ADJUSTMENT_TYPE = ''Upload''
                AND a.PROCESS_TYPE = ''VaR'')`});
    
        // Insert new upload data
        snowflake.execute({sqlText: `INSERT INTO FACT.VAR_MEASURES_ADJUSTMENT (
        COBID,
        VAR_SUBCOMPONENT_ID,
        ADJUSTMENT_ID,
        BOOK_KEY,
        TRADE_KEY,
        CURRENCY_CODE,
        SOURCE_SYSTEM_CODE,
        ENTITY_CODE,
        INFLATION_INDEX_ID,
        COMMON_INSTRUMENT_KEY,
        UNDERLYING_COMMON_INSTRUMENT_KEY,
        IS_OFFICIAL_SOURCE,
        REGION_AREA_KEY,
        SCENARIO_DATE_ID,
        PNL_VECTOR_VALUE_IN_USD,
        RUN_LOG_ID
        )
        SELECT
            t.COBID,
            VAR_SUB_COMPONENT_ID,
            ADJUSTMENT_ID,
            BOOK_KEY,
            TRADE_KEY,
            IFNULL(CURRENCY_CODE,''N/A''),
            IFNULL(SOURCE_SYSTEM_CODE,''QP''),
            ENTITY_CODE,
            -1 AS INFLATION_INDEX_ID,
            IFNULL(COMMON_INSTRUMENT_KEY,-1),
            -1 AS UNDERLYING_COMMON_INSTRUMENT_KEY,
            TRUE,
            -1 AS REGION_AREA_KEY,
            SCENARIO_DATE_ID,
            PNL_VECTOR_VALUE_IN_USD,
            rl.Run_log_id
        FROM FACT.TEMP_ADJUSTMENT_UPLOAD t
        LEFT JOIN FACT.TEMP_UPLOAD_RUNLOG rl
            ON rl.COBID = t.COBID
        WHERE IS_DELETED = FALSE
        AND PNL_VECTOR_VALUE_IN_USD <> 0`});
    
        // Load to VAR MEASURES SUMMARY
        snowflake.execute({
              sqlText: `INSERT INTO FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY (
            COBID,
            VAR_SUBCOMPONENT_ID,
            ADJUSTMENT_ID,
            BOOK_KEY,
            CURRENCY_CODE,
            SOURCE_SYSTEM_CODE,
            ENTITY_CODE,
            INFLATION_INDEX_ID,
            COMMON_INSTRUMENT_KEY,
            UNDERLYING_COMMON_INSTRUMENT_KEY,
            REGION_AREA_KEY,
            SCENARIO_DATE_ID,
            PNL_VECTOR_VALUE_IN_USD,
            RUN_LOG_ID
            )
            SELECT
                t.COBID,
                VAR_SUB_COMPONENT_ID,
                ADJUSTMENT_ID,
                BOOK_KEY,
                IFNULL(CURRENCY_CODE,''N/A''),
                IFNULL(SOURCE_SYSTEM_CODE,''QP''),
                ENTITY_CODE,
                -1 AS INFLATION_INDEX_ID,
                IFNULL(COMMON_INSTRUMENT_KEY,-1),
                -1 AS UNDERLYING_COMMON_INSTRUMENT_KEY,
                -1 AS REGION_AREA_KEY,
                SCENARIO_DATE_ID,
                SUM(PNL_VECTOR_VALUE_IN_USD),
                rl.Run_log_id AS RUN_LOG_ID
            FROM FACT.TEMP_ADJUSTMENT_UPLOAD t
                LEFT JOIN FACT.TEMP_UPLOAD_RUNLOG rl
                ON rl.COBID = t.COBID
            WHERE t.IS_DELETED = FALSE
            GROUP BY 
                t.COBID,
                VAR_SUB_COMPONENT_ID,
                ADJUSTMENT_ID,
                BOOK_KEY,
                CURRENCY_CODE,
                SOURCE_SYSTEM_CODE,
                ENTITY_CODE,
                COMMON_INSTRUMENT_KEY,
                SCENARIO_DATE_ID,
                rl.Run_log_id 
            HAVING SUM(PNL_VECTOR_VALUE_IN_USD)<>0`});

        // Set Run Status to Processed
        snowflake.execute({
            sqlText:  `UPDATE DIMENSION.ADJUSTMENT a
            SET RUN_STATUS = IFF(RECORD_COUNT = 0, ''Deleted'',''Processed'')
            , PROCESS_DATE = CURRENT_TIMESTAMP()
            , USERNAME = LEFT(CURRENT_USER,50)
            FROM FACT.TEMP_ADJUSTMENT_UPLOAD t 
            WHERE T.ADJUSTMENT_ID = a.ADJUSTMENT_ID 
            AND T.COBID = a.COBID
            AND a.ADJUSTMENT_TYPE = ''Upload''
            AND RUN_STATUS = ''Running''
            AND a.PROCESS_TYPE = ''VaR''`});
        
        snowflake.execute({
            sqlText:  `
            UPDATE ADJUSTMENT.IDM_VAR_UPLOAD A
            SET RUN_STATUS = ''Processed''
            FROM FACT.TEMP_ADJUSTMENT_UPLOAD TP 
            WHERE TP.VAR_UPLOAD_ID = A.VAR_UPLOAD_ID 
            AND TP.COBID = A.COBID
            AND A.RUN_STATUS = ''Running'';`});

        // Find Record count
        snowflake.execute({
            sqlText:  `UPDATE FACT.TEMP_UPLOAD_RUNLOG 
                SET RECORD_COUNT = x.RECORD_COUNT 
                FROM  (
                    SELECT COBID, count(*) as RECORD_COUNT  
                    FROM FACT.TEMP_ADJUSTMENT_UPLOAD 
                    WHERE IFNULL(IS_DELETED,FALSE) = FALSE
                    AND PNL_VECTOR_VALUE_IN_USD <> 0
                    GROUP BY COBID
                    ) AS x
                WHERE x.COBID = FACT.TEMP_UPLOAD_RUNLOG.COBID;`})
    
        // End Run Log
        snowflake.execute({
            sqlText:  `UPDATE BATCH.RUN_LOG
                SET END_TIME=CURRENT_TIMESTAMP()
                    , RECORD_COUNT = FACT.TEMP_UPLOAD_RUNLOG.RECORD_COUNT 
                FROM FACT.TEMP_UPLOAD_RUNLOG
                WHERE FACT.TEMP_UPLOAD_RUNLOG.COBID = BATCH.RUN_LOG.COBID
                AND FACT.TEMP_UPLOAD_RUNLOG.RUN_LOG_ID = BATCH.RUN_LOG.RUN_LOG_ID`})


        // Set powerBi Publish info
        var rs = snowflake.execute({sqlText: "SELECT CURRENT_TIMESTAMP()"});
        rs.next();
        var END_TIME = rs.getColumnValue(1);

        var sqlUpdateAdjTime = `
        MERGE INTO METADATA.POWERBI_PUBLISH_INFO t USING 
        ( 
        SELECT distinct cobid,''var'' as data_group_name, :1 AS last_updated_adj,''RaptorReporting'' AS dataset_name
        FROM FACT.TEMP_ADJUSTMENT_UPLOAD
        ) s 
        ON
        ( 
            s.dataset_name = t.dataset_name 
            AND s.data_group_name = t.data_group_name 
            AND s.cobid = t.cobid
        ) 
        WHEN MATCHED THEN 
        UPDATE SET t.last_updated_adj = s.last_updated_adj 
        WHEN NOT MATCHED THEN 
        INSERT 
        (
            cobid, data_group_name, last_updated_adj, dataset_name
        ) 
        VALUES 
        (
            s.cobid, s.data_group_name, s.last_updated_adj, s.dataset_name
        );`;
            
        snowflake.execute({sqlText: sqlUpdateAdjTime,binds: [END_TIME]});
    
        var sqlInsertAdjDetail = `
        INSERT INTO METADATA.POWERBI_PUBLISH_DETAIL 
        ( 
            COBID,
            INSERT_SOURCE,
            MAX_RUN_LOG_ID,
            INSERT_TIME,
            POWERBI_OBJECT_TYPE,
            POWERBI_OBJECT_NAME,
            COMMENTS
        )
        SELECT RL.COBID,RL.INSERT_SOURCE,RL.MAX_RUN_LOG_ID,RL.INSERT_TIME,I.POWERBI_OBJECT_TYPE,I.POWERBI_OBJECT_NAME,RL.COMMENTS
        FROM (
        SELECT
            COBID,
            ''LOAD_VAR_ADJUSTMENT_UPLOAD'' AS INSERT_SOURCE,
            RUN_LOG_ID as MAX_RUN_LOG_ID,
            :1 as INSERT_TIME,
            NULL AS COMMENTS
        FROM FACT.TEMP_UPLOAD_RUNLOG
        GROUP BY COBID,RUN_LOG_ID ) RL
        INNER JOIN METADATA.POWERBI_INSERT_SOURCES I
        ON RL.INSERT_SOURCE = I.INSERT_SOURCE;`;
        
        snowflake.execute({sqlText: sqlInsertAdjDetail, binds: [END_TIME]});
    
        var sqlInsertProcessing = `
        INSERT INTO METADATA.POWERBI_ACTION
        (
            DATASET_NAME,
            ACTION_TYPE,
            POLICY_EFFECTIVE_DATE,
            COBID,
            WORKSPACE_NAME,
            OBJECT_TYPE,
            PARTITION_TYPE,
            OBJECT_NAME,
            INSERT_SOURCE,
            MAX_RUN_LOG_ID,
            MAX_RUN_LOG_TIME,
            APPLY_POLICY,
            PUBLISH_INFO
        )
        SELECT
            DATASET_NAME,
            ACTION_TYPE,
            POLICY_EFFECTIVE_DATE,
            PS.COBID,
            WORKSPACE_NAME,
            OBJECT_TYPE,
            PARTITION_TYPE,
            OBJECT_NAME,
            INSERT_SOURCE,
            MAX_RUN_LOG_ID,
            MAX_RUN_LOG_TIME,
            APPLY_POLICY,
            PUBLISH_INFO
        FROM METADATA.VW_POWERBI_ACTION_INSERT_SOURCE PS
        INNER JOIN ( SELECT MAX(COBID) AS COBID FROM FACT.TEMP_ADJUSTMENT_UPLOAD ) TAR
            ON TAR.COBID = PS.COBID
            AND TAR.COBID = PS.ORIGINAL_COBID
        WHERE INSERT_SOURCE = ''LOAD_VAR_ADJUSTMENT_UPLOAD'';`;
        
        snowflake.execute({sqlText: sqlInsertProcessing});

        result = "Success";
    }

    catch(err){
                
        var ErrorMessage = "Failed: Code: " + err.code + " State: " + err.state;
            ErrorMessage += " Message: " + err.message;
            ErrorMessage += " Stack Trace:" + err.stackTraceTxt;

        // Set Run Status to Processed
        var AdjustmentErrorMessage = ["VaR Upload Load Error", err.message].join(" : ")
        snowflake.execute({
            sqlText:  `UPDATE DIMENSION.ADJUSTMENT fa SET RUN_STATUS = ''Error'', ERRORMESSAGE = :1
                       WHERE RUN_STATUS = ''Running''
                       AND ADJUSTMENT_TYPE = ''Upload''
                       AND PROCESS_TYPE = ''VaR''`,
            binds: [AdjustmentErrorMessage]
            });

         var sqlRunLog = `UPDATE BATCH.RUN_LOG
            SET ERROR=TRUE, ERROR_MESSAGE=:1
            FROM FACT.TEMP_UPLOAD_RUNLOG
            WHERE FACT.TEMP_UPLOAD_RUNLOG.COBID = BATCH.RUN_LOG.COBID
            AND FACT.TEMP_UPLOAD_RUNLOG.RUN_LOG_ID = BATCH.RUN_LOG.RUN_LOG_ID`;
    
       snowflake.execute({
            sqlText: sqlRunLog,
            binds: [ErrorMessage]
            });

        result = "ERROR: " + ErrorMessage;
    }

    return result;
 ';