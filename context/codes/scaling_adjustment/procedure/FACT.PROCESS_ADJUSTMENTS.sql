CREATE OR REPLACE PROCEDURE DVLP_RAPTOR_NEWADJ.FACT.PROCESS_ADJUSTMENTS("P_PROCESS_TYPE" VARCHAR(30), "DEBUG_FLAG" VARCHAR(1))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

	function executeStep(v_source_table, v_target_table, v_metric_name, v_run_log_id, v_adj_rec, v_agg_results, v_description, v_steps) {
		var res = snowflake.execute({
		            sqlText: `CALL FACT.PROCESS_ADJUSTMENT_STEP (:1,:2,:3,:4,:5,:6,:7,:8)`,
		            binds:[ /* 1 P_SOURCE_TABLE */		v_source_table
		                  , /* 2 P_TARGET_TABLE */  	v_target_table
						  , /* 3 P_METRIC_NAME */		v_metric_name
						  , /* 4 P_RUN_LOG_ID */ 		v_run_log_id
						  , /* 5 P_ADJUSTMENT_RECORD */ JSON.stringify(v_adj_rec)
						  , /* 6 P_AGGREGATE_RESULTS */ v_agg_results
						  , /* 7 P_DESCRIPTION */		v_description
						  , ''0'']});
		
				res.next()
				step = res.getColumnValue(1)
				v_steps.push(step)
				if (step.status == "ERROR") {
					adjRes.status = "ERROR"
					adjRes.error_message = step.error_message
					throw {}
				}
	}

    var result = "Success"

	var fact_table = ``
	var fact_summary_table = ``
	var adjustments_detail_fact_table = ``
	var adjustments_summary_fact_table = ``
    var combined_table = ``
	var runLogIdList = []

	switch (P_PROCESS_TYPE) {
		case ''VAR'' : 
			fact_table = `fact.var_measures`;
			adjustments_table = `fact.var_measures_adjustment`;
			adjustments_summary_table = `fact.var_measures_adjustment_summary`;
			metric_name = `pnl_vector_value_in_usd`
			break;

		case ''ES'' : 
			fact_table = `fact.es_measures`;
			adjustments_table = `fact.es_measures_adjustment`;
			adjustments_summary_table = `fact.es_measures_adjustment_summary`;
			metric_name = `pnl_vector_value_in_usd`
			break;

		case ''STRESS'' : 
			fact_table = `fact.stress_measures`;
			adjustments_table = `fact.stress_measures_adjustment`;
			adjustments_summary_table = `fact.stress_measures_adjustment_summary`;
			metric_name = `simulation_pl_in_usd`
			break;
	}

    try {

		// Process any Stress Uploads
		if (P_PROCESS_TYPE == "STRESS"){
			snowflake.execute({sqlText: `call FACT.LOAD_STRESS_ADJUSTMENT_UPLOAD()`});
		};

	    // Get Pending Adjustments for the PROCESS TYPE i.e. VAR, ES, STRESS etc 
	    var sqlStageTable = `CREATE OR REPLACE TEMPORARY TABLE FACT.TEMP_STAGE_ADJUSTMENT AS(
	        SELECT *
	        FROM 
	          DIMENSION.ADJUSTMENT s
	        WHERE s.ADJUSTMENT_TYPE IN (''Flatten'', ''Scale'')
	          AND s.RUN_STATUS = ''Pending''
	          AND s.process_type = :1
	    );`;
	
	    snowflake.execute({sqlText: sqlStageTable, binds : [P_PROCESS_TYPE]});

	    // Set Run Status to Running for all adjustments in the temp table
	    var res = snowflake.execute({
	        sqlText:  `UPDATE DIMENSION.ADJUSTMENT fa 
						  SET RUN_STATUS = ''Running''
	                    WHERE fa.adjustment_id in (select adjustment_id from FACT.TEMP_STAGE_ADJUSTMENT) 
						  AND run_status = ''Pending''`
	    });

		// todo check number of records updated, if zero rows updated then this means that another session is running at the same time and has already set the status to RUNNING
		// this relies on the fact that the UPDATE statement locks the table so no other updates can run at the same time.
		var rowsUpdated = res.getNumRowsAffected()
		if (rowsUpdated == 0) { return rowsUpdated; }
	
	
		// loop over the adjustments in the temp table
		var adjSql = `SELECT  object_construct(a.*) adj_rec
	    				FROM fact.temp_stage_adjustment a
	    			ORDER BY a.cobid, a.created_date, a.adjustment_id`
	
		// cursor for the current set of adjustments that are being processed
		var adjResultSet = snowflake.execute({
		        sqlText: adjSql
		    });

		var description = ``
	
	    // loop over all identified adjustments
	    while (adjResultSet.next()) {
			var adjRes;
			try {
				adjRes = { status : "SUCCESS" }
				var steps = []
				adjRes[''steps''] = steps

				var adjRec = adjResultSet.getColumnValue(1);
				var origAdjRec = adjResultSet.getColumnValue(1);
				adjRes[''adjustment_record''] = origAdjRec;
	
				// create run log id 
			    var rs = snowflake.execute({sqlText: "SELECT BATCH.SEQ_RUN_LOG.nextval as x"});
			    rs.next();
			    var runLogId = rs.getColumnValue(1);   
		
				// insert run log into table
				var sqlRunlogInsert = `call batch.load_run_log(:1, :2, :3, :4, :5, :6, :7, :8)`
		
		    	snowflake.execute({
					sqlText: sqlRunlogInsert,
					binds : [ /* 1 RUN_LOG_ID */		runLogId
							, /* 2 COBID */ 			adjRec[''COBID'']
							, /* 3 PROC_NAME */			''FACT.PROCESS_ADJUSTMENT''
							, /* 4 PROC_PARAMETERS */	P_PROCESS_TYPE
							, /* 5 BATCH_ACTION_DAILY_KEY */	0
							, /* 6 RECORD_COUNT */				0
							, /* 7 ERROR */						''false''
							, /* 8 ERROR_MESSAGE */				''''
							]
					});
				
				// start transaction 
		        snowflake.execute({sqlText: `begin transaction`});

				adjRec[''INCLUDE_ALL_WINDOWS''] = true; // setting this to true makes sure that all the scenario dates are used i.e. do not filter by window

				// Delete existing BASE Adjustments : note the P_TARGET_TABLE value
				description = `Delete records from ${adjustments_table} that match the filter criteria`
		        executeStep(/* 1 SOURCE_TABLE */		null
		                  , /* 2 TARGET_TABLE */  		adjustments_table
						  , /* 3 METRIC_NAME */			metric_name
						  , /* 4 RUN_LOG_ID */ 			runLogId
						  , /* 5 ADJUSTMENT_RECORD */ 	adjRec
						  , /* 6 AGGREGATE_RESULTS */ 	null
						  , /* 7 DESCRIPTION */			description
						  , /* 8 STEPS */				steps);
		
		   		// Delete existing SUMMARY Adjustments : note the different P_TARGET_TABLE value
				description = `Delete records from ${adjustments_summary_table} that match the filter criteria`
		        executeStep(/* 1 SOURCE_TABLE */		null
		                  , /* 2 TARGET_TABLE */  		adjustments_summary_table
						  , /* 3 METRIC_NAME */			metric_name
						  , /* 4 RUN_LOG_ID */ 			runLogId
						  , /* 5 ADJUSTMENT_RECORD */ 	adjRec
						  , /* 6 AGGREGATE_RESULTS */ 	null
						  , /* 7 DESCRIPTION */			description
						  , /* 8 STEPS */				steps);
		
				// if the adjustment is not marked as DELETED then we need to calculate and apply the adjustments
		        if (!(adjRec[''IS_DELETED'']))
		        {
					//if source cobid not set the set to the same as cobid
					if (!("SOURCE_COBID" in adjRec)) {
						adjRec[''SOURCE_COBID'']= adjRec[''COBID'']
					}
		
					var action;
					if (adjRec[''ADJUSTMENT_TYPE''] == "Scale" && adjRec[''COBID''] == adjRec[''SOURCE_COBID'']) {
						action = ''SCALE CURRENT COB''
					} 
					else if (adjRec[''ADJUSTMENT_TYPE''] == "Scale" && adjRec[''COBID''] != adjRec[''SOURCE_COBID'']) {
						action = ''SCALE SOURCE COB''
					}
					else {
						action = ''FLATTEN TARGET COB''
					}

					var full_entity_roll = true;
			
					if ("BOOK_CODE" in adjRec ||
						"DEPARTMENT_CODE" in adjRec ||
						"TRADER_CODE" in adjRec ||
						"GUARANTEED_ENTITY" in adjRec ||
						"REGION_KEY" in adjRec ||
						"TRADE_CODE" in adjRec ||
						"TRADE_TYPOLOGY" in adjRec ||
						"STRATEGY" in adjRec ||
						"INSTRUMENT_CODE" in adjRec ||
						"VAR_COMPONENT_ID" in adjRec ||
						"VAR_SUB_COMPONENT_ID" in adjRec ||
						"SHIFT_TYPE" in adjRec ||
						"RISK_CLASS" in adjRec ||
						"LIQUIDITY_HORIZON" in adjRec ||
						"CURRENCY_CODE" in adjRec ||
						"SOURCE_SYSTEM_CODE" in adjRec ||
						"SCENARIO_DATE_ID" in adjRec ||
						"IS_OFFICIAL_SOURCE" in adjRec ||
						"SIMULATION_NAME" in adjRec ||
						"RISK_COMPONENT" in adjRec ||
						"RISK_SUB_COMPONENT" in adjRec)
					{
			            full_entity_roll = false
					}
		
					switch (action) {
		
						// FLATTEN : (src cob == tgt cob ) insert records into tgt table for tgt cob, selecting records from src table for src cob multiplying metric by -1 (so that fact + adjustments nets to zero)
						case ''FLATTEN TARGET COB'' :
						
							description = `Action is ${action}.  Setting scale factor to -1 and inserting records into ${adjustments_table} for records that match the filter criteria`

							adjRec[''SCALE_FACTOR''] = -1
							adjRec[''SOURCE_COBID''] = adjRec[''COBID'']
							adjRec[''INCLUDE_ALL_WINDOWS''] = (full_entity_roll == true ? false : true); // if we are doing a full entity roll then only flatten the 4 windows in scope

					        executeStep(/* 1 P_SOURCE_TABLE */		fact_table
					                  , /* 2 P_TARGET_TABLE */  	adjustments_table
									  , /* 3 P_METRIC_NAME */		metric_name
									  , /* 4 P_RUN_LOG_ID */ 		runLogId
									  , /* 5 P_ADJUSTMENT_RECORD */ adjRec
									  , /* 6 P_AGGREGATE_RESULTS */ ''false''
						  			  , /* 7 P_DESCRIPTION */		description
									  , /* 8 STEPS */				steps);
			
							break;
		
						// SCALE : (src cob == tgt cob)    insert records into tgt table for tgt cob, selecting records from src table for src cob multiplying metric by "[scale factor] - 1".  i.e. if scale factor = 3.1, then inserted records should have scale factor of 3.1 - 1 = 2.1 (to give 3.1 when fact is added to adjustment
						case ''SCALE CURRENT COB'' :

							description = `Action is ${action}.  Setting scale factor to [scale factor] - 1 and inserting records into ${adjustments_table} for records that match the filter criteria`

							// set scale factor to scale factor - 1
							adjRec[''SCALE_FACTOR''] =  adjRec[''SCALE_FACTOR''] - 1;
							//adjRec[''INCLUDE_ALL_WINDOWS''] = true; // setting this to true makes sure that all the scenario dates are used i.e. do not filter by window
							adjRec[''INCLUDE_ALL_WINDOWS''] = (full_entity_roll == true ? false : true); // if we are doing a full entity roll then only flatten the 4 windows in scope
					        executeStep(  /* 1 P_SOURCE_TABLE */		fact_table
						                , /* 2 P_TARGET_TABLE */  		adjustments_table
										, /* 3 P_METRIC_NAME */			metric_name
										, /* 4 P_RUN_LOG_ID */ 			runLogId
										, /* 5 P_ADJUSTMENT_RECORD */ 	adjRec
										, /* 6 P_AGGREGATE_RESULTS */ 	''false''
							  			, /* 7 P_DESCRIPTION */			description
						  				, /* 8 STEPS */					steps);
		
							break;
		
						// SCALE SOURCE COB : (src cob != tgt cob)     insert records into tgt table for tgt cob, selecting records from src table for tgt cob multiplying metric by -1 (so that fact + adjustments nets to zero) (i.e. flatten tgt cob )
						//								   insert records into tgt table for tgt cob, selecting records from src table for src cob, multiplying metric by [scale factor]
						case ''SCALE SOURCE COB'' :
		
							// take a copy of the original value of these attributes as they will be manipulated later
							var sourceCobId = adjRec[''SOURCE_COBID'']
							var targetCobId = adjRec[''COBID'']
							var scaleFactor = adjRec[''SCALE_FACTOR'']
		
							// set source cobid to the same as tgt cobid as, in this instance, we want to FLATTEN the tgt cobid.  The source cobid is used in the where condition of the select. and in this case we want to select the target cobid
							adjRec[''SOURCE_COBID''] = targetCobId
							adjRec[''SCALE_FACTOR''] = -1
							adjRec[''INCLUDE_ALL_WINDOWS''] = (full_entity_roll == true ? false : true); // if we are doing a full entity roll then only flatten the 4 windows in scope

							description = `Action is ${action}.  Step 1 - flatten ${adjRec[''COBID'']} for records that match the filter criteria`							
							
							// flatten the tgt cob id
					        executeStep( /* 1 P_SOURCE_TABLE */			fact_table
					                   , /* 2 P_TARGET_TABLE */  		adjustments_table
									   , /* 3 P_METRIC_NAME */			metric_name
									   , /* 4 P_RUN_LOG_ID */ 			runLogId
									   , /* 5 P_ADJUSTMENT_RECORD */ 	adjRec
									   , /* 6 P_AGGREGATE_RESULTS */ 	''false''
						  			   , /* 7 P_DESCRIPTION */			description
									   , /* 8 STEPS */					steps);
							
							// reset scale factor and source cob id back to original and set INCLUDE_ALL_WINDOWS to false as we only want to roll scenario dates that are in the target cobid and that belong to standard 4 windows
							adjRec[''SCALE_FACTOR''] = scaleFactor;
							adjRec[''INCLUDE_ALL_WINDOWS''] = false
							adjRec[''SOURCE_COBID''] = sourceCobId

							description = `Action is ${action}.  Step 2 - extract base source records for ${adjRec[''SOURCE_COBID'']} that match the filter criteria and scale by scale factor`	
							
							// get base data for the source cob, Source is the fact table and target is the adjustments table
					        executeStep( /* 1 P_SOURCE_TABLE */			fact_table
					                   , /* 2 P_TARGET_TABLE */  		adjustments_table
									   , /* 3 P_METRIC_NAME */			metric_name
									   , /* 4 P_RUN_LOG_ID */ 			runLogId
									   , /* 5 P_ADJUSTMENT_RECORD */ 	adjRec
									   , /* 6 P_AGGREGATE_RESULTS */ 	''false''
						  			   , /* 7 P_DESCRIPTION */			description
									   , /* 8 STEPS */					steps);

							// get any adjustments that were applied from the source cob. Source table and target table are both set to the ADJUSTMENTS table
							description = `Action is ${action}.  Step 3 - extract adjustment records for ${adjRec[''SOURCE_COBID'']} that match the filter criteria and scale by scale factor`	
					        executeStep( /* 1 P_SOURCE_TABLE */			adjustments_table
					                   , /* 2 P_TARGET_TABLE */  		adjustments_table
									   , /* 3 P_METRIC_NAME */			metric_name
									   , /* 4 P_RUN_LOG_ID */ 			runLogId
									   , /* 5 P_ADJUSTMENT_RECORD */ 	adjRec
									   , /* 6 P_AGGREGATE_RESULTS */ 	''true''
						  			   , /* 7 P_DESCRIPTION */			description
									   , /* 8 STEPS */					steps);
		
							break;
						}
			
			        }
		
					// bring summary table up-to-date
			
					// set scale factor to 1 as we do not want any scaling from fact to summary tables
					adjRec[''SCALE_FACTOR''] = 1;
					// add is_official_source filter as the aggregate tables should only contain records where is_official_source is true
					adjRec[''IS_OFFICIAL_SOURCE''] = ''true'';
					// set source cob to target cob as the source cobid is used for the selects
					adjRec[''SOURCE_COBID''] = adjRec[''COBID''];
					//set adjRec[''INCLUDE_ALL_WINDOWS''] = true as we want to aggrgate everything that is in the base table
					adjRec[''INCLUDE_ALL_WINDOWS''] = true

			        // Re-calc summary table
					description = `Re-calculate summary adjustment tables ${adjustments_summary_table}`	
			        aggregateResults= ''true''
			        executeStep(/* 1 P_SOURCE_TABLE */		adjustments_table
			                  , /* 2 P_TARGET_TABLE */  	adjustments_summary_table
							  , /* 3 P_METRIC_NAME */		metric_name
							  , /* 4 P_RUN_LOG_ID */ 		runLogId
							  , /* 5 P_ADJUSTMENT_RECORD */ adjRec
							  , /* 6 P_AGGREGATE_RESULTS */ ''true''
							  , /* 7 P_DESCRIPTION */		description
							  , /* 8 STEPS */				steps);

			        sqlRowCount = `SELECT ZEROIFNULL(COUNT(*)) FROM IDENTIFIER(:1) WHERE ADJUSTMENT_ID=:2`;
			        sqlStmt = snowflake.createStatement( {sqlText: sqlRowCount, binds:[adjustments_table, adjRec[''ADJUSTMENT_ID'']]});
			        rs = sqlStmt.execute();
			        rs.next();
			        var recordCount = rs.getColumnValue(1);			
			
			        // Set Run Status to Processed
			        snowflake.execute({
			            sqlText:  `UPDATE DIMENSION.ADJUSTMENT fa SET RUN_STATUS = ''Processed'', ErrorMessage = null, RECORD_COUNT = :2, PROCESS_DATE = CURRENT_TIMESTAMP() WHERE ADJUSTMENT_ID = :1`,
			            binds:[adjRec[''ADJUSTMENT_ID''], recordCount]
			        });
			
					// commit transaction 
			        snowflake.execute({sqlText: `commit`});
			
					// insert run log into table
					 sqlRunlogInsert = `call batch.LOAD_RUN_LOG_END_WITH_DETAIL(:1, :2)`
			
			    	snowflake.execute({
						sqlText: sqlRunlogInsert,
						binds : [ /* 1 RUN_LOG_ID */		runLogId
								, /* 2 DETAIL */ 			`${JSON.stringify(adjRes)}`
								]
						});

					runLogIdList.push(runLogId)

		    } catch (err) {

					//rollback the current adjustment id DMLs, log the error, and then move onto the next adjustment ID in the loop
				 	snowflake.execute({sqlText: `rollback`});
				
			        // Set Run Status to Error
			        snowflake.execute({
			            sqlText:  `UPDATE DIMENSION.ADJUSTMENT fa SET RUN_STATUS = ''Error'', errormessage = :2 WHERE ADJUSTMENT_ID = :1`,
			            binds:[adjRec[''ADJUSTMENT_ID''], adjRes.error_message]
			        });
	
					// update the run log id to ERROR, storing the current adjRes record in the run log table
			    	snowflake.execute({
						sqlText: `call batch.LOAD_RUN_LOG_END_WITH_DETAIL(:1, :2)`,
						binds : [ /* 1 RUN_LOG_ID */		runLogId
								, /* 2 DETAIL */ 			JSON.stringify(adjRes)
								]
						})

					result = "WARNING";
							 
			} // end try
	    } // While Loop ended

    	snowflake.execute({
			sqlText: `call FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(:1 /* P_DATA_GROUP_NAME */
       														  ,:2 /* P_DATASET_NAME */
															  ,:3 /* P_INSERT_SOURCE */
															  ,:4 /* P_RUN_LOG_IDS */ 
															  ,:5 /* DEBUG_FLAG */
															  )`,
			binds : [P_PROCESS_TYPE, ''RaptorReporting'',`LOAD_${P_PROCESS_TYPE}_ADJUSTMENT`, `${runLogIdList.join(", ")}`,''0'']
			})
	} // end outer try

    catch(err){

        var ErrorMessage = "ERROR : Failed: Code: " ; // err.code;
          ErrorMessage += " State: " + err.state;
          ErrorMessage += "  Message: " + err.message;
          ErrorMessage += " Stack Trace:" + err.stackTraceTxt;
 		
		result = ErrorMessage
		

    }
    return result;
$$;
