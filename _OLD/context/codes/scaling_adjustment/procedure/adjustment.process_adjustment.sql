CREATE OR REPLACE PROCEDURE adjustment.process_adjustment(process_type STRING, adjustment_action STRING, cobid INT)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark.functions import col, lit, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, TimestampType
from datetime import datetime
import pytz
import json
import pandas as pd
import numpy as np

def check_columns(df_sf, column_list, metric_name, metric_usd_name):
    df = df_sf.to_pandas()    
    # Create a dictionary to store column data
    column_data = {}    
    for col in column_list:
        if col in df.columns:
            column_data[col] = df[col]
        else:
            column_data[col] = -1 if col.split('_')[-1].upper() == 'KEY' else np.nan
    
    # Add additional columns
    column_data[metric_name] = df["ADJUSTMENT_VALUE"]
    column_data[metric_usd_name] = df["ADJUSTMENT_VALUE_IN_USD"]
    column_data["IS_DELETED"] = df["IS_DELETED"]    
    # Create the DataFrame all at once
    temp = pd.DataFrame(column_data)    
    return temp

def update_adjustment_status(session,df_sf,cobid):
  # Filter Pending adjustments
  adj_dim_table = session.table('dimension.adjustment')
  adj_dim_table.update(
    {
      "RUN_STATUS": "Processed", 
      "PROCESS_DATE": session.sql("select CONVERT_TIMEZONE('Europe/London',CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)").collect()[0][0]
    }, 
    (
      (adj_dim_table["ADJUSTMENT_ID"] == df_sf["ADJUSTMENT_ID"]) &
      (adj_dim_table["COBID"] == cobid) &
      (adj_dim_table["RUN_STATUS"] == "Pending")
    ),
    df_sf
  )

def surrogate_key(list_key, key_name):
  list_key_coalesce = []
  for k in list_key:
    coalesce_key = f"coalesce(cast({k} as TEXT), '_dbt_utils_surrogate_key_null_')"
    list_key_coalesce.append(coalesce_key)
  join_list_key_coalesce = " || '-' || ".join(list_key_coalesce)
  return f"md5(cast({join_list_key_coalesce} as TEXT)) as {key_name}"


def main(session, process_type, adjustment_action, cobid):
  """
  Process adjustments based on the given parameters.

  This function handles the main logic for processing adjustments. It performs the following steps:
  1. Retrieves adjustment settings from the seed table.
  2. Sets up necessary variables and table references.
  3. Creates a new run log ID.
  4. Processes adjustments based on the adjustment action (Direct or Scale).
  5. For Direct adjustments:
      - Deletes existing adjustments from the fact adjustment table.
      - Inserts new adjustments into the fact adjustment table.
  6. For Scale adjustments:
      - Deletes existing adjustments from fact adjustment and summary tables.
      - Calculates and inserts new adjustments using Dynamic SQL commands.
  7. Optionally inserts summary data for scale adjustments.

  Parameters:
  - session: Snowflake session object
  - process_type: Type of process to run (seed table)
  - adjustment_action: Type of adjustment (Direct or Scale)
  - cobid: Close of Business ID
  """
  try:
    dict_return = {}
    dict_return["process_type"] = process_type
    dict_return["adjustment_type"] = adjustment_action
    dict_return["cobid"] = cobid

    # Get Adjustment Settings from seed table
    df_settings = session.table('adjustment.adjustments_settings').filter(
      (upper(col('process_type')) == process_type.upper())
    )
    list_settings = df_settings.to_pandas().to_dict(orient='records')

    if len(list_settings) == 0:
      raise Exception (f'No settings found in the table ADJUSTMENT.ADJUSTMENTS_SETTINGS for process type: {process_type}')

    # Set values from adjustment configuration seed table
    fact_tbl_name = list_settings[0]["FACT_TABLE"] # Source of adjustments
    fact_ajusted_tbl_name = list_settings[0]["FACT_AJUSTED_TABLE"] # Source of adjustments when source cob is different from target cob
    fact_tbl_pk = list_settings[0]["FACT_TABLE_PK"] # Primary key of the fact table
    fact_adj_tbl_name = list_settings[0]["ADJUSTMENTS_TABLE"] # Target of adjustments
    fact_adj_summary_tbl_name = list_settings[0]["ADJUSTMENTS_SUMMARY_TABLE"] # Target of adjustments summary (optional)
    adj_base_tbl_name = list_settings[0]["ADJUSTMENT_BASE_TABLE"] # Base table for adjustments
    metric_name = list_settings[0]["METRIC_NAME"].upper() # Metric name in the fact table
    metric_usd_name = list_settings[0]["METRIC_USD_NAME"].upper() # Metric USD name in the fact table
    
    list_fact_tbl_pk = fact_tbl_pk.split(';')
    if len(list_fact_tbl_pk) == 0:
      raise Exception (f'No primary key found in the table ADJUSTMENT.ADJUSTMENTS_SETTINGS for process type: {process_type}')
    elif len(list_fact_tbl_pk) > 1:
      key_name = (fact_tbl_name.split('.')[1]).split('measures')[0] + "key"
      fact_tbl_pk = surrogate_key(list_fact_tbl_pk,key_name)
    else:
      fact_tbl_pk = list_fact_tbl_pk[0]
      key_name = fact_tbl_pk

    # Create a new run log id
    run_log_id = session.sql("SELECT BATCH.SEQ_RUN_LOG.nextval as x").collect()[0][0]
    dict_return["run_log_id"] = run_log_id

    ##### The columns will be used to join between fact and adjustment tables #####

    # Get only adjustments
    df_adjustment = session.table(adj_base_tbl_name).filter(
      (col('cobid') == cobid) &
      ((upper(col('process_type')) == process_type) | (upper(col('process_type')) == 'FRTBALL'))
    )
    list_adj_base_columns = df_adjustment.columns
  
    # Read Fact Adjustment table
    df_fact_adj_tbl = session.table(fact_adj_tbl_name)
    list_fact_adj_columns = df_fact_adj_tbl.columns

    # Read Adjustment Summary table
    if fact_adj_summary_tbl_name:
      df_fact_adj_summary_tbl = session.table(fact_adj_summary_tbl_name)
      list_fact_adj_summary_columns = df_fact_adj_summary_tbl.columns

    # Read Fact table
    df_fact_tbl_name = session.table(fact_tbl_name)
    list_fact_columns = df_fact_tbl_name.columns

    if adjustment_action.lower() == 'direct':
      # Filter by adjustment action
      df_adjustment_direct = df_adjustment.filter(
        (col('adjustment_action') == 'Direct') & 
        (col('is_positive_adjustment')== True)
      )

      if df_adjustment_direct.count() == 0:
        dict_return["message"] = f'No pending {adjustment_action} adjustment found'

      else:
        # Create a pandas dataframe from the direct adjustment dataframe with the columns matched in the fact table
        df_pd_adjustment_direct = check_columns(df_adjustment_direct, list_fact_adj_columns, metric_name, metric_usd_name)

        # Delete the ajustment from fact adjustment table
        return_deleted_direct = df_fact_adj_tbl.delete(
          ((df_fact_adj_tbl["COBID"] == df_adjustment_direct["COBID"]) &
          (df_fact_adj_tbl["ADJUSTMENT_ID"] == df_adjustment_direct["ADJUSTMENT_ID"])
          ), df_adjustment_direct )
        row_count_deleted_direct = return_deleted_direct[0]

        # Exclude deleted adjustment
        df_pd_adjustment_direct_valid = df_pd_adjustment_direct[df_pd_adjustment_direct["IS_DELETED"] == False]
        row_count_deleted_source = df_pd_adjustment_direct[df_pd_adjustment_direct["IS_DELETED"] == True].shape[0]
        row_count_valid = df_pd_adjustment_direct_valid.shape[0]

        # Drop column IS_DELETED because it does not exists in target table
        df_pd_adjustment_direct_valid = df_pd_adjustment_direct_valid.drop(columns=["IS_DELETED"])

        # Add run log id
        df_pd_adjustment_direct_valid["RUN_LOG_ID"] = run_log_id

        # Insert into fact adjustment table
        return_direct_insert = session.write_pandas(
          df_pd_adjustment_direct_valid,
          auto_create_table=False,
          table_name=fact_adj_tbl_name.split('.')[-1].upper(),
          schema=fact_adj_tbl_name.split('.')[0].upper())

        # Update adjustment status to Completed
        update_adjustment_status(session,df_adjustment_direct,cobid)

        # Row count check
        if (row_count_valid + row_count_deleted_source) != row_count_deleted_direct and row_count_deleted_direct > 0:
          #raise Exception
          print(f'Error: row count not matched'
          + f' | (row_count_valid + row_count_deleted_source): ' + str(row_count_valid + row_count_deleted_source)
          + f' | (row_count_deleted_direct): ' + str(row_count_deleted_direct)
          )

    elif adjustment_action.lower() == 'scale':
      
      # All adjustments that are not direct will be SCALE
      # Flatten: scale_factor_adjusted = -1
      # Scale Current COB: scale_factor_adjusted = scale_factor -1
      # Scale Source COB: scale_factor_adjusted = scale_factor
      #                 + Flatten current COB

      # Filter adjustment action diff than Direct
      df_adjustment_scale = df_adjustment.filter(
        (col('adjustment_action') != 'Direct') & 
        (col('is_positive_adjustment') == True)
      )

      if df_adjustment_scale.count() == 0:
        dict_return["message"] = f'No pending {adjustment_action} adjustment found'

      else:
        
        # Get all columns that matches in fact table and adjustment table to use in the join between both
        remove_fields_join = ['COBID','IS_OFFICIAL_SOURCE', 'STRATEGY', 'LOAD_TIMESTAMP','REGION_AREA_KEY']
        list_join = [x for x in list_fact_columns if x in list_adj_base_columns and x not in remove_fields_join]

        # Get all columns in fact table removing the ones listed in remove_fields_select list
        ## metrics are transfromed with scale factor
        ## cobid is based on adjustment table
        ## raven filename and row number are not on fact adjustment table
        remove_fields_select = [metric_name, metric_usd_name, 'COBID','RAVEN_FILENAME','RAVEN_FILE_ROW_NUMBER', 'LOAD_TIMESTAMP', 'RUN_LOG_ID']
        list_fact_columns_non_metrics = [x.upper() for x in list_fact_columns if x not in remove_fields_select]
        list_fact_columns_non_metrics_match = [x.upper() for x in list_fact_columns_non_metrics if x in list_fact_adj_columns]

        # Select with alias for non-metric fields
        select_fields_non_metrics = 'fact.'+', fact.'.join(list_fact_columns_non_metrics_match)
        
        # Use the same fields without the metrics in the select statement
        insert_fields_non_metrics = ', '.join(list_fact_columns_non_metrics_match)

        # Multiple the metrics with the scale factor
        select_measure_fields = f"""
        fact.{metric_name} * adjust.scale_factor_adjusted AS {metric_name}, 
        fact.{metric_usd_name} * adjust.scale_factor_adjusted AS {metric_usd_name}"""

        # Select the fact rows based on adjustments
        select_cmd = f"""
        FROM {fact_tbl_name} fact 
        INNER JOIN {adj_base_tbl_name} adjust 
        WHERE adjust.COBID = {cobid}
        AND adjust.IS_DELETED = FALSE
        AND (adjust.PROCESS_TYPE = '{process_type}' OR adjust.PROCESS_TYPE = 'FRTBALL')
        AND {metric_usd_name} IS NOT NULL"""

        special_filer = ""
        # Special filter for department_code
        special_filer = special_filer + ("" if "BOOK_KEY" not in list_fact_columns_non_metrics_match 
        else "\n AND EXISTS (SELECT book_key FROM DIMENSION.BOOK bk WHERE bk.book_key = coalesce(fact.book_key,-1) AND (bk.department_code = adjust.department_code OR adjust.department_code IS NULL))")
        # Special filter for strategy
        special_filer = special_filer + ("" if "TRADE_KEY" not in list_fact_columns_non_metrics_match 
        else "\n AND EXISTS (SELECT trade_key FROM DIMENSION.TRADE td WHERE td.trade_key = coalesce(fact.trade_key,-1) AND (td.STRATEGY = adjust.STRATEGY OR adjust.STRATEGY IS NULL) AND (td.trade_typology = adjust.trade_typology OR adjust.trade_typology IS NULL))")
        # Special filter for trade_typology
        # special_filer = special_filer + ("" if "PRODUCT_CATEGORY_ATTRIBUTES_KEY" not in list_fact_columns_non_metrics_match 
        # else "\n AND EXISTS (SELECT product_category_attributes_key FROM DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES pca WHERE pca.product_category_attributes_key = coalesce(fact.product_category_attributes_key,-1) AND (pca.trade_typology = adjust.trade_typology OR adjust.trade_typology IS NULL))") #}
        
        # Condition for the special filters
        select_cmd = select_cmd + special_filer

        select_scale = f"SELECT distinct adjust.COBID, adjust.ADJUSTMENT_ID, adjust.CREATED_DATE, {select_fields_non_metrics}, {select_measure_fields} "
        condition_scale_current_cob = "AND fact.COBID = adjust.SOURCE_COBID AND adjust.COBID = adjust.SOURCE_COBID"
        condition_scale_other_cob = "AND fact.COBID = adjust.SOURCE_COBID AND adjust.COBID <> adjust.SOURCE_COBID"

        select_flatten = select_scale.replace('adjust.scale_factor_adjusted', '-1')
        condition_flatten = "AND fact.COBID = adjust.COBID AND adjust.COBID <> adjust.SOURCE_COBID"

        join_condition = ' '.join([f'AND (adjust.{c} = fact.{c} OR adjust.{c} is null)\n' for c in list_join])

        # Different select statement when the fact table has no primary key
        select_with_keys = "*" if key_name == fact_tbl_pk else f"{fact_tbl_pk}, *"
        adjustment_exclude_keys = "*" if key_name == fact_tbl_pk else f"* exclude ({key_name})"

        # Replace the fact table with the adjustment table
        replace_adj_cmd = select_cmd.replace(fact_tbl_name, fact_ajusted_tbl_name)

        # Select statement to insert the fact table with the adjustment
        insert_cmd = f"""
        CREATE OR REPLACE TEMPORARY TABLE {fact_adj_tbl_name}_temp 
        (
          COBID, ADJUSTMENT_ID, ADJUSTMENT_CREATED_TIMESTAMP, {insert_fields_non_metrics}, {metric_name}, {metric_usd_name}, RUN_LOG_ID, LOAD_TIMESTAMP
        ) AS 
        WITH cte AS (
          -- Scale current COBID
          {select_scale} {select_cmd} 
          {condition_scale_current_cob} {join_condition} UNION ALL
          -- Scale other COBID
          {select_scale} {replace_adj_cmd}
          {condition_scale_other_cob} {join_condition} UNION ALL
          -- Flatten current COBID
          {select_flatten} {select_cmd} 
          {condition_flatten} {join_condition}
        ),
        fact_key as (
            select 
                {select_with_keys}
            from cte
        ),
        adjustment as (
            select
                {adjustment_exclude_keys},
                DENSE_RANK() over (partition by {key_name} order by created_date desc, adjustment_id desc) as row_num
            from fact_key
        )
        select 
          adjustment.* exclude row_num,
          {run_log_id} AS RUN_LOG_ID,
          current_timestamp() AS LOAD_TIMESTAMP
        from adjustment
        where adjustment.row_num = 1
        and adjustment.{metric_usd_name} <> 0
        """  

        dict_return["insert_cmd"] = insert_cmd
        session.sql(insert_cmd).collect()

        # Delete the ajustment from fact Adjustment table
        print(f'Delete adjustment from fact adjustment table: {fact_adj_tbl_name}')
        df_fact_adj_tbl.delete(
           ((df_fact_adj_tbl["COBID"] == df_adjustment_scale["COBID"]) &
           (df_fact_adj_tbl["ADJUSTMENT_ID"] == df_adjustment_scale["ADJUSTMENT_ID"])
           ), df_adjustment_scale )

        # Delete the ajustment from fact Summary Adjustment table
        if fact_adj_summary_tbl_name:
          df_fact_adj_summary_tbl.delete(
            ((df_fact_adj_summary_tbl["COBID"] == df_adjustment_scale["COBID"])
            ), df_adjustment_scale )
        
        # Insert the adjustment to the fact table from temp table
        insert_cmd_perm = f"""
        INSERT INTO {fact_adj_tbl_name}
        (
          COBID, ADJUSTMENT_ID, ADJUSTMENT_CREATED_TIMESTAMP, {insert_fields_non_metrics}, {metric_name}, {metric_usd_name}, RUN_LOG_ID, LOAD_TIMESTAMP
        )
        SELECT
          COBID, ADJUSTMENT_ID, ADJUSTMENT_CREATED_TIMESTAMP, {insert_fields_non_metrics}, {metric_name}, {metric_usd_name}, RUN_LOG_ID, LOAD_TIMESTAMP
        FROM {fact_adj_tbl_name}_temp
        """
        session.sql(insert_cmd_perm).collect()

        # Drop temp table
        session.sql(f"DROP TABLE IF EXISTS {fact_adj_tbl_name}_temp").collect()

        ############## Rolling adjustments: COBID <> SOURCE_COBID > Update Trade Key, Common Instrument Key ##############
        cdm_select_update = f"""
          update {fact_adj_tbl_name} tgt
          set tgt.TRADE_KEY = src.TRADE_KEY_ADJ,
          tgt.COMMON_INSTRUMENT_KEY = src.COMMON_INSTRUMENT_KEY_ADJ, 
          tgt.COMMON_INSTRUMENT_FCD_KEY = src.COMMON_INSTRUMENT_FCD_KEY_ADJ
          from (
            with adj_cte as (
              select distinct 
                f.adjustment_id, f.cobid, f.book_key, f.trade_key, f.common_instrument_key,	f.common_instrument_fcd_key, ad.source_cobid
              from
                {fact_adj_tbl_name} f
              inner join {adj_base_tbl_name} ad on f.adjustment_id = ad.adjustment_id and f.cobid = ad.cobid
              where f.cobid = {cobid}
              and ad.cobid <> ad.source_cobid
            )
            select distinct 
              f.*,
              td2.trade_key as trade_key_adj,
              ci2.common_instrument_key as common_instrument_key_adj,
              cif.common_instrument_fcd_key as common_instrument_fcd_key_adj
            FROM adj_cte f
            inner join dimension.book b on f.book_key = b.book_key 
            inner join dimension.trade td on td.trade_key = f.trade_key
            inner join dimension.trade td2 on td.trade_code = td2.trade_code
                                          and b.book_code = td2.book_code
                                          and to_date(f.source_cobid::string,'yyyyMMdd') BETWEEN td2.effective_start_date and td2.effective_end_date
            inner join dimension.common_instrument ci on ci.common_instrument_key = f.common_instrument_key
            inner join dimension.common_instrument ci2 on ci.instrument_code = ci2.instrument_code
                                                      and to_date(f.source_cobid::string,'yyyyMMdd') BETWEEN ci2.effective_start_date and ci2.effective_end_date
            inner join dimension.common_instrument_fcd cif on ci2.instrument_key = cif.instrument_key
                                                      and to_date(f.source_cobid::string,'yyyyMMdd') BETWEEN cif.effective_start_date and cif.effective_end_date
            where (f.trade_key <> trade_key_adj or f.common_instrument_key <> common_instrument_key_adj or f.common_instrument_fcd_key <> common_instrument_fcd_key_adj)
          ) src
          where tgt.trade_key = src.trade_key
          and tgt.common_instrument_key = src.common_instrument_key
          and tgt.common_instrument_fcd_key = src.common_instrument_fcd_key
          and tgt.cobid = src.cobid
        """
        dict_return["update_cmd"] = cdm_select_update
        session.sql(cdm_select_update).collect()

        #################################################################################

        if fact_adj_summary_tbl_name:
          insert_summary_fields_non_metrics = ', '.join([ x for x in list_fact_adj_summary_columns if x not in [metric_name, metric_usd_name]])
          insert_summary_cmd = f"""
          INSERT INTO {fact_adj_summary_tbl_name} 
          (
            {insert_summary_fields_non_metrics}, {metric_name}, {metric_usd_name}
          )
          SELECT {insert_summary_fields_non_metrics}, SUM({metric_name}) AS {metric_name}, SUM({metric_usd_name}) AS {metric_usd_name}
          FROM {fact_adj_tbl_name}
          WHERE COBID = {cobid}
          AND IS_OFFICIAL_SOURCE = TRUE
          GROUP BY ALL
          """
          dict_return["insert_summary_cmd"] = insert_summary_cmd
          session.sql(insert_summary_cmd).collect()
      
        # Update adjustment status to Completed
        update_adjustment_status(session,df_adjustment_scale,cobid)

    else:      
      dict_return["message"] = f'Error: Invalid adjust action - {adjustment_action}'
    
    dict_return['message'] = f'Success: Adjustment table updated'

  except Exception as e:
    error_msg = str(e).replace("'","")
    print(f"Error: {error_msg}")
    dict_return['message'] = f"Error: {error_msg}"

  return dict_return

$$;
