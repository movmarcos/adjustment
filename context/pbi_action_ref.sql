SELECT 
	COBID,
	CASE 
		WHEN START_TIME IS NULL THEN 'Queued'			
		WHEN COMPLETE_TIME IS NULL THEN 'Running'
		ELSE 'Completed'
	END AS STATUS,
 	WORKSPACE_NAME , 
 	CASE 
 		WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE 'VAR%' THEN 'VaR'
 		WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE 'Stress%' THEN 'Stress'
 		WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE 'SENSITIVITY%' THEN 'Sensitivity'
 		WHEN OBJECT_NAME = 'VaR Adjustment Summary Import' THEN 'VaR Adjustment'
  		WHEN OBJECT_NAME = 'Stress Measures Adjustment Import' THEN 'Stress Adjustment'
  		WHEN OBJECT_NAME = 'Sensitivity Summary Adjustment Import' THEN 'Sensitivity Adjustment'
  		WHEN OBJECT_NAME IN ('Stress Measures Import', 'Stress Measures Import Cyclic') THEN 'Stress Adhoc'
  		WHEN OBJECT_NAME IN ('VaR Book Import', 'VAR_SUMMARY_IMPORT_CYCLIC') THEN 'VaR Adhoc'
  		WHEN OBJECT_NAME = 'Sensitivity Summary Import' THEN 'Sensitivity Adhoc'  		
  		WHEN OBJECT_NAME = 'VAR_SUMMARY_IMPORT_MAX' THEN 'VaR 10Day/Maximiser'
  	END AS PROCESS_TYPE,
 	OBJECT_NAME, 
 	OBJECT_TYPE,
 	INSERT_SOURCE, 
 	convert_timezone( 'UTC','Europe/London', REQUEST_TIME::timestamp_ntz) REQUEST_TIME, 
    convert_timezone( 'UTC','Europe/London', START_TIME::timestamp_ntz) START_TIME, 
    convert_timezone( 'UTC','Europe/London', COMPLETE_TIME::timestamp_ntz) COMPLETE_TIME
FROM METADATA.POWERBI_ACTION;