I will not guess-and-patch again — that's the 3-hour loop. Snowflake recorded the exact statement it rejected. Run
  this in a worksheet (role that can read ACCOUNT_USAGE, or use the INFORMATION_SCHEMA variant below):

  -- A) The exact SQL Snowflake rejected (this names the relation with the lone "status" column)
  SELECT QUERY_TEXT, ERROR_MESSAGE, START_TIME, QUERY_TYPE
  FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
          RESULT_LIMIT => 1000,
          END_TIME_RANGE_START => DATEADD('hour', -3, CURRENT_TIMESTAMP())))
  WHERE ERROR_MESSAGE ILIKE '%CURRENCY_CODE%'
  ORDER BY START_TIME DESC
  LIMIT 5;

  -- B) What the deployed settings actually say (resolves the ADJUSTMENT_BASE_TABLE contradiction)
  SELECT PROCESS_TYPE, FACT_TABLE, FACT_ADJUSTED_TABLE, ADJUSTMENTS_TABLE,
         ADJUSTMENTS_SUMMARY_TABLE, ADJUSTMENT_BASE_TABLE, FACT_TABLE_PK
  FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS;

  -- C) The full stored error for the failed Flatten
  SELECT ADJ_ID, PROCESS_TYPE, ADJUSTMENT_TYPE, RUN_STATUS, ERRORMESSAGE
  FROM ADJUSTMENT_APP.ADJ_HEADER
  WHERE RUN_STATUS = 'Failed'
  ORDER BY PROCESS_DATE DESC
  LIMIT 5;

  If QUERY_HISTORY() returns nothing for the child statement, use account usage (≤45 min lag):

  SELECT QUERY_TEXT, ERROR_MESSAGE, START_TIME
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE ERROR_MESSAGE ILIKE '%CURRENCY_CODE%'
    AND START_TIME > DATEADD('hour', -3, CURRENT_TIMESTAMP())
  ORDER BY START_TIME DESC
  LIMIT 5;