CREATE OR REPLACE PROCEDURE DVLP_RAPTOR_NEWADJ.FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS("P_DATA_GROUP_NAME" VARCHAR(100), "P_DATASET_NAME" VARCHAR(100), "P_INSERT_SOURCE" VARCHAR(100), "P_RUN_LOG_IDS" VARCHAR(16777216), "DEBUG_FLAG" VARCHAR(1))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
     result = ''Error''
    
    if (P_RUN_LOG_IDS == "") {
        return "Error : no Run Log IDs supplied"
    }

    runLogIds = P_RUN_LOG_IDS.split(",")

    // Run log entries
    var sqlRunLogTable = `
        CREATE OR REPLACE TEMPORARY TABLE FACT.TEMP_ADJUSTMENT_RUNLOG AS (
            select cobid, proc_parameters data_group_name, max(end_time) last_updated_adj, :2 AS dataset_name, max(run_log_id) max_run_log_id
              from batch.run_log rl
             where proc_parameters = :1
               and error = false
               and run_log_id in (${runLogIds.join(",")})
               group by cobid, proc_parameters
        )`;
    
    snowflake.execute({sqlText: sqlRunLogTable
                      , binds : [P_DATA_GROUP_NAME, P_DATASET_NAME]});


    var sqlUpdateAdjTime = `
    MERGE INTO METADATA.POWERBI_PUBLISH_INFO t 
    USING ( 
        SELECT cobid, data_group_name, last_updated_adj, dataset_name
        FROM FACT.TEMP_ADJUSTMENT_RUNLOG
		UNION ALL
		SELECT cobid, ''sensitivity_detail'', last_updated_adj, dataset_name
        FROM FACT.TEMP_ADJUSTMENT_RUNLOG
		WHERE data_group_name = ''sensitivity''
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
    INSERT (
        cobid, data_group_name, last_updated_adj, dataset_name) 
    VALUES (
        s.cobid, s.data_group_name, s.last_updated_adj, s.dataset_name);`;
    
    snowflake.execute({sqlText: sqlUpdateAdjTime});

    var sqlInsertAdjDetail = `
    INSERT INTO METADATA.POWERBI_PUBLISH_DETAIL ( 
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
            :1 AS INSERT_SOURCE,
            MAX_RUN_LOG_ID,
            last_updated_adj as INSERT_TIME,
            NULL AS COMMENTS
        FROM FACT.TEMP_ADJUSTMENT_RUNLOG
        ) RL
    INNER JOIN METADATA.POWERBI_INSERT_SOURCES I
    ON RL.INSERT_SOURCE = I.INSERT_SOURCE;`;
    
    snowflake.execute({sqlText: sqlInsertAdjDetail, binds: [P_INSERT_SOURCE]});

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
    INNER JOIN ( SELECT MAX(COBID) AS COBID FROM FACT.TEMP_ADJUSTMENT_RUNLOG ) TAR
        ON TAR.COBID = PS.COBID
        AND TAR.COBID = PS.ORIGINAL_COBID
    WHERE INSERT_SOURCE = :1`;
    
    snowflake.execute({sqlText: sqlInsertProcessing
                      , binds : [P_INSERT_SOURCE]});

    result = "Success";

    return result;
    
$$;