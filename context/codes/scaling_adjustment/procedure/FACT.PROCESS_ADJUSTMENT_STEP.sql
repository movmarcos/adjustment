CREATE OR REPLACE PROCEDURE DVLP_RAPTOR_NEWADJ.FACT.PROCESS_ADJUSTMENT_STEP("P_SOURCE_TABLE" VARCHAR(250), "P_TARGET_TABLE" VARCHAR(250), "P_METRIC_NAME" VARCHAR(100), "P_RUN_LOG_ID" VARCHAR(100), "P_ADJUSTMENT_RECORD" VARCHAR(16777216), "P_AGGREGATE_RESULTS" VARCHAR(5), "P_DESCRIPTION" VARCHAR(16777216), "DEBUG_FLAG" VARCHAR(1))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var result = {
					status : "SUCCESS",
					description : P_DESCRIPTION
				}

 	const blankLineRegex = new RegExp(''(\\n+)|(\\t+)'',''gm'')
	const multipleSpacesRegex = new RegExp(''  +'',''gm'')

    try {
			var sourceTableColumnList = []
			var targetTableColumnList = []
			var commonTableColumnList = []

			const adjRec = JSON.parse(P_ADJUSTMENT_RECORD)

			const actionType = P_SOURCE_TABLE != null ? ''INSERT'' : ''DELETE''

			//result.action_type = actionType


			/* Always get target columns as needed for both DELETEs and INSERTs  */
			 var resultSet = snowflake.execute(	{
												    sqlText: "call BATCH.GET_COLUMNS(:1,:2,''Y'',''N'','''')" ,
												    binds:[P_TARGET_TABLE, '''']
												});
			resultSet.next();
			
			targetTableColumnList = resultSet.getColumnValue(1).split(",");

			/* remove P_METRIC_NAME,  RUN_LOG_ID and COBID fields from the list as these will be replaced later */
			targetTableColumnList = targetTableColumnList.filter(function(x) {
				if (x!== P_METRIC_NAME.toUpperCase() && x!== ''RUN_LOG_ID'' && x!== ''COBID'' && x!== ''ADJUSTMENT_ID'') {
					return x;
				}
			})

			/* if actionType = INSERT then get source, target and common columns, else just get target */
			if (actionType == ''INSERT'') {
	
				/* get the source table columns*/
				 var resultSet = snowflake.execute(	{
													    sqlText: "call BATCH.GET_COLUMNS(:1,:2,''Y'',''N'','''')" ,
													    binds:[P_SOURCE_TABLE, '''']
													});
				resultSet.next();
				
				sourceTableColumnList = resultSet.getColumnValue(1).split(",");
	
				/*remove P_METRIC_NAME,  RUN_LOG_ID and COBID fields from the list as these will be replaced later */
				sourceTableColumnList = sourceTableColumnList.filter(function(x) {
					if (x!== P_METRIC_NAME.toUpperCase() && x!== ''RUN_LOG_ID'' && x!== ''COBID'' && x!== ''ADJUSTMENT_ID'') {
						return x;
					}
				})
				
				/* get the columns in common between the source and target tables */
				 var resultSet = snowflake.execute(	{
													    sqlText: "call BATCH.GET_COLUMNS(:1,:2,''Y'',''N'','''')" ,
													    binds:[P_SOURCE_TABLE, P_TARGET_TABLE]
													});
				resultSet.next();
				
				commonTableColumnList = resultSet.getColumnValue(1).split(",");
	
				/* remove P_METRIC_NAME,  RUN_LOG_ID and COBID fields from the list as these will be replaced later */
				commonTableColumnList = commonTableColumnList.filter(function(x) {
					if (x!== P_METRIC_NAME.toUpperCase() && x!== ''RUN_LOG_ID'' && x!== ''COBID'' && x!== ''ADJUSTMENT_ID'') {
						return x;
					}
				})

			} 


		const bkQry =  `select book_key 
			              from dimension.book 
			             where equal_null(book_code , nvl(:7, book_code))
			               and equal_null(department_code , nvl(:8, department_code))
			               and equal_null(primary_trader_code , nvl(:9, primary_trader_code))
			               and equal_null(guaranteed_entity  , nvl(:10, guaranteed_entity))
			               and equal_null(region_key , nvl(:11, region_key))
						`

		const entityQry =  `select entity_key 
			                  from dimension.entity 
				             where equal_null(entity_code , nvl(:6, entity_code))
				           `

		const trdQry =   `select trade_key
				            from dimension.trade 
				           where equal_null(trade_code, nvl(:12, trade_code))
						     and equal_null(trade_typology , nvl(:13, trade_typology)) 
						     and equal_null(strategy, nvl(:14, strategy))
						 `

		const ciQry =   `select common_instrument_key
				           from dimension.common_instrument 
				          where equal_null(instrument_code, nvl(:15, instrument_code))
				        `

		const vscQry =   `select var_sub_component_id
				            from dimension.var_sub_component 
				           where equal_null(var_component_id, nvl(:16, var_component_id))
                             and equal_null(var_sub_component_id, nvl(:17,var_sub_component_id))
							 and equal_null(var_sub_component_day_type, nvl(:29,var_sub_component_day_type))
				         `

        const stQry =   `select shift_type_key
				  		   from dimension.shift_type
				  		  where equal_null(shift_type, nvl(:18,shift_type))
				  		`

		const rcQry =   `select risk_class_key
				           from dimension.risk_class 
				          where equal_null(risk_class, nvl(:19, risk_class))
                            and equal_null(risk_component, nvl(:26, risk_component))
                            and equal_null(risk_sub_component, nvl(:27, risk_sub_component))
				        `

		const lhQry =   `select liquidity_horizon_key
				           from dimension.liquidity_horizon 
				          where equal_null(liquidity_horizon, nvl(:20,liquidity_horizon))
						`

		const stressSimQry =  `select stress_simulation_key
				                 from dimension.stress_simulation 
				                where equal_null(stress_simulation_name, nvl(:25,stress_simulation_name))
                                AND equal_null(simulation_source, nvl(:28,simulation_source))
						      `

        const windowsQry =	`SELECT
                                     DISTINCT SCENARIO_DATE_ID
                                FROM fact.VAR_WINDOW_PNL_VECTOR_ELEMENT_ORDINAL
                                WHERE COB_ID = :2
                                  AND ENTITY_CODE = :6
                                  AND VAR_WINDOW_ID 
                                      IN (SELECT VAR_WINDOW_ID FROM dimension.VAR_WINDOW
                                          WHERE VAR_WINDOW_NAME IN 
                                         (''Stressed VaR'', ''1 Year VaR'', ''2 Year VaR'', ''3 Year VaR''))
				        	`
		
		commonTableColumnList.sort();

		const insertClause = `
			INSERT INTO ${P_TARGET_TABLE} (
							  run_log_id
							, cobid
			                , adjustment_id
			                , ${commonTableColumnList.join(", ")}
			                , ${P_METRIC_NAME}	
			                )
		`

		const selectClause = `
                    SELECT :1 run_log_id
                        , :2 cobid
						, ${P_TARGET_TABLE.match(/.*SUMMARY/i) ? `adjustment_id`: `:3 adjustment_id`}
						, ${commonTableColumnList.join(", ")}
		                , ${P_AGGREGATE_RESULTS == ''true'' ? `sum(${P_METRIC_NAME})` : `${P_METRIC_NAME}`} * :4 AS ${P_METRIC_NAME}

		             FROM  ${P_SOURCE_TABLE}`

		const groupByClause = `GROUP BY ${P_TARGET_TABLE.match(/.*SUMMARY/i) ? `adjustment_id, `: ``}${commonTableColumnList.join(", ")} HAVING SUM(${P_METRIC_NAME}) <> 0`

		const deleteClause = `
			DELETE FROM ${P_TARGET_TABLE}
		`

		/* set the variable to the array that should be checked when seeing if the filtered field exists
		 * if INSERT then you need to check that the field exists in the source table column list
         * otherwise it is a DELETE then you need to check the target table column list
         */ 
		const tableColumnsToFilter = actionType == ''INSERT'' ? sourceTableColumnList : targetTableColumnList

		/* 
		 * generate the where clause
		 * For each condition, check whether any filters have been specified in the adjRec, and then check to make sure that field exists for the table that is being filtered
	     * An assumption has been made that if this is an aggregation that DO NOT add the trade filter
		 */
		const whereClause = `
			  WHERE 1=1
					${actionType == ''INSERT'' ? `AND ${P_METRIC_NAME} != 0` : ``}
                    AND ${actionType == ''INSERT'' ? `COBID = :5` : `COBID =:2`} 
                    ${"ENTITY_CODE" in adjRec
					 && tableColumnsToFilter.includes(''ENTITY_KEY'')
							? `AND entity_key in (${entityQry})` : ``}

					${"ENTITY_CODE" in adjRec
					 && tableColumnsToFilter.includes(''ENTITY_CODE'')
							? `AND ENTITY_CODE =:6` : ``}

					${("BOOK_CODE" in adjRec || "DEPARTMENT_CODE" in adjRec || "TRADER_CODE" in adjRec || "REGION_KEY" in adjRec || "GUARANTEED_ENTITY" in adjRec)
					 && tableColumnsToFilter.includes(''BOOK_KEY'')
							? `AND book_key in (${bkQry})` : ``}
					
 					${!(P_TARGET_TABLE.match(/.*SUMMARY/i)) && ("TRADE_CODE" in adjRec || "TRADE_TYPOLOGY" in adjRec || "STRATEGY" in adjRec)
					 && tableColumnsToFilter.includes(''TRADE_KEY'')
							? `AND trade_key in (${trdQry})` : ``} 
					
					${("INSTRUMENT_CODE" in adjRec)
					 && tableColumnsToFilter.includes(''COMMON_INSTRUMENT_KEY'')
							? `AND common_instrument_key in (${ciQry})` : ``}
					
					${( ("VAR_COMPONENT_ID" in adjRec) || ("VAR_SUB_COMPONENT_ID" in adjRec) || ("DAY_TYPE" in adjRec))
					 && tableColumnsToFilter.includes(''VAR_SUBCOMPONENT_ID'')
							? `AND (var_subcomponent_id in (${vscQry})
                                OR var_subcomponent_id in 
                                (select var_sub_component_id
				                        from dimension.var_sub_component
                                        where :16 = 11
                                        and var_sub_component_day_type = 10))` : ``}
					
					${("RISK_CLASS" in adjRec || "RISK_COMPONENT" in adjRec || "RISK_SUB_COMPONENT" in adjRec )
					 && tableColumnsToFilter.includes(''RISK_CLASS_KEY'')
							? `AND risk_class_key in (${rcQry})` : ``}
					
					${("SHIFT_TYPE" in adjRec) 
					 && tableColumnsToFilter.includes(''SHIFT_TYPE_KEY'')
							? `AND shift_type_key in (${stQry})` : ``}
					
					${("LIQUIDITY_HORIZON" in adjRec)
					 && tableColumnsToFilter.includes(''LIQUIDITY_HORIZON_KEY'')
							? `AND liquidity_horizon_key in (${lhQry})` : ``}
					
					${("CURRENCY_CODE" in adjRec) 
					 && tableColumnsToFilter.includes(''CURRENCY_CODE'')  
							? "AND CURRENCY_CODE = :21" : "" }

					${("CURRENCY_CODE" in adjRec) 
					 && tableColumnsToFilter.includes(''TRADE_CURRENCY'') 
							? "AND TRADE_CURRENCY = :21" : "" }

					${("SOURCE_SYSTEM_CODE" in adjRec)
                     && tableColumnsToFilter.includes(''SOURCE_SYSTEM_CODE'') 
							? "AND SOURCE_SYSTEM_CODE = :22" : ""}

					${("SCENARIO_DATE_ID" in adjRec) 
					 && tableColumnsToFilter.includes(''SCENARIO_DATE_ID'') 
							? "AND SCENARIO_DATE_ID = :23" : ""}

					${("IS_OFFICIAL_SOURCE" in adjRec) 
                     && tableColumnsToFilter.includes(''IS_OFFICIAL_SOURCE'')
							? "AND IS_OFFICIAL_SOURCE = :24" : ""}

					/*
					 * If we are not including all windows we need to add 2 filters
                     * the first filter restricts to just those scenario dates in the 4 windows
					 * the second filter only includes var sub components where the day type != 10, however we only add this if there is not a filter already on the var sub component dimension
                     */
					${("INCLUDE_ALL_WINDOWS" in adjRec) 
                     && adjRec[''INCLUDE_ALL_WINDOWS''] == false
					 && tableColumnsToFilter.includes(''VAR_SUBCOMPONENT_ID'')
					 && actionType != "DELETE"
							? `AND SCENARIO_DATE_ID in (${windowsQry})` : ``}

					${(("SIMULATION_NAME" in adjRec) || ("SIMULATION_SOURCE" in adjRec))
					 && tableColumnsToFilter.includes(''STRESS_SIMULATION_KEY'')
							? `AND stress_simulation_key in (${stressSimQry})` : ``}
		`  
        var sqlStage = `
				${actionType == ''INSERT'' ? `${insertClause} ${selectClause}` : `${deleteClause}`}
				${whereClause}
				${actionType == ''INSERT'' && P_AGGREGATE_RESULTS == ''true''? `${groupByClause}` : ``}
		`.replace(blankLineRegex,` `)
         .replace(multipleSpacesRegex,` `)

		var bindsArray = [    /* 1  */ "P_RUN_LOG_ID"
						 	, /* 2  */ "COBID"
							, /* 3  */ "ADJUSTMENT_ID"
							, /* 4  */ "SCALE_FACTOR"
							, /* 5  */ "SOURCE_COBID" 
							, /* 6  */ "ENTITY_CODE" 
							, /* 7  */ "BOOK_CODE" 
							, /* 8  */ "DEPARTMENT_CODE"
							, /* 9  */ "TRADER_CODE"
							, /* 10 */ "GUARANTEED_ENTITY"
							, /* 11 */ "REGION_KEY"
							, /* 12 */ "TRADE_CODE"
							, /* 13 */ "TRADE_TYPOLOGY"
							, /* 14 */ "STRATEGY"
							, /* 15 */ "INSTRUMENT_CODE"
							, /* 16 */ "VAR_COMPONENT_ID"
							, /* 17 */ "VAR_SUB_COMPONENT_ID"
							, /* 18 */ "SHIFT_TYPE" 
							, /* 19 */ "RISK_CLASS"
							, /* 20 */ "LIQUIDITY_HORIZON" 
							, /* 21 */ "CURRENCY_CODE"
							, /* 22 */ "SOURCE_SYSTEM_CODE" 
							, /* 23 */ "SCENARIO_DATE_ID"
							, /* 24 */ "IS_OFFICIAL_SOURCE"
							, /* 25 */ "SIMULATION_NAME"
							, /* 26 */ "RISK_COMPONENT"
							, /* 27 */ "RISK_SUB_COMPONENT"
                            , /* 28 */ "SIMULATION_SOURCE"
							, /* 29 */ "DAY_TYPE"
						]
			// build up the sqlBinds array for use in the SQL statement
			var sqlBinds = bindsArray.map ( (val) => {
				var retValue
				if (val == "P_RUN_LOG_ID") {
					retValue = P_RUN_LOG_ID 
				}		
				else { 
					retValue = (val in adjRec ? adjRec[val] : null ) 
				}
				return retValue;
			});

		result.sql = { sqlText : sqlStage }
		
		// this puts the bind number, field name and value in the json result object to ease debugging
		newDescriptiveBinds = bindsArray.map( (val, idx) => `/* ${idx+1} ${val} */ ${sqlBinds[idx]}`);

		result.sql.binds = newDescriptiveBinds;

        var rs = snowflake.execute({
            sqlText: sqlStage,
            binds: sqlBinds
        });

		var rc = rs.getNumRowsAffected()
		var queryId = rs.getQueryId()

		result.sql.num_rows_affected = rc
		result.sql.query_id = queryId

    }

    catch(err){
         
        result.error_message = "ERROR: " + err.message;
		result.status = "ERROR";

    }
    return result;
';