import snowflake.snowpark.functions as f
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, TimestampType, VariantType
from datetime import datetime
import json, pytz
import ast

def model(dbt, session):

  # dbt configuration
  dbt.config(
      materialized="incremental",
      python_version="3.11",
      tags=["frtbsa_adjustment", "adjustment"]
  )

  # References to the base models
  df_adjustments_model = dbt.ref('adjustment__adjustments_base_frtb')

  schema_model = StructType([
      StructField("run_log_id", IntegerType()),
      StructField("cobid", IntegerType()),
      StructField("adjustment_type", StringType()),
      StructField("adjustment_status", StringType()),
      StructField("adjustment_result", VariantType()),
      StructField("adjustment_timestamp", TimestampType()),
      StructField("insert_cmd", StringType()),
      StructField("update_cmd", StringType()),
      StructField("insert_summary_cmd", StringType())
  ])

  # Validate and get configuration
  cobid = dbt.config.get("cobid")
  process_type = dbt.config.get("process_type")
  adjustment_actions = dbt.config.get("adjustment_actions")

  result_adj = []
  adj_action = ""
  try:
    
    if not all([cobid, process_type, adjustment_actions]):
      raise ValueError("Missing required configuration values")
      
    if cobid != 19000101:
      for adj_action in adjustment_actions:
        return_adj = session.call('adjustment.process_adjustment', process_type, adj_action, cobid)
        #dict_adj = json.loads(return_adj.replace("'", '"'))
        tmp0_adj = ast.literal_eval(return_adj)
        insert_cmd = ""
        update_cmd = ""
        insert_summary_cmd = ""
        for key, value in tmp0_adj.copy().items():
          if key == "insert_cmd":
              insert_cmd = tmp0_adj.pop(key)
          if key == "update_cmd":
              update_cmd = tmp0_adj.pop(key)
          if key == "insert_summary_cmd":
              insert_summary_cmd = tmp0_adj.pop(key)
        tmp1_adj = json.dumps(tmp0_adj)
        dict_adj = json.loads(tmp1_adj)
        message = dict_adj.get("message", "")
        adjustment_status = message.split(':', 1)[0] if ':' in message else "Unknown"
        #row_status = [dict_adj.get("run_log_id"), cobid, adj_action, adjustment_status, return_adj, datetime.now(pytz.timezone('Europe/London'))]
        row_status = [dict_adj.get("run_log_id"), cobid, adj_action, adjustment_status, return_adj, datetime.now(pytz.timezone('Europe/London')),insert_cmd,update_cmd,insert_summary_cmd]
        result_adj.append(row_status)

        if adjustment_status!= "Success":
          raise Exception(message)

  except json.JSONDecodeError as e:
    result_adj = [[-1, cobid, adj_action, "JSON Error", str(e), datetime.now(pytz.timezone('Europe/London'))]]
    raise
  except Exception as e:
    result_adj = [[-1, cobid, adj_action, "Error", str(e), datetime.now(pytz.timezone('Europe/London'))]]
    raise

  # Create dataframe
  df = session.create_dataframe(result_adj, schema=schema_model)

  df = df.select(
    df["run_log_id"],
    df["cobid"],
    f.expr("adjustment_type::VARCHAR(16777216) COLLATE 'en-ci'").alias("adjustment_type"),
    f.expr("adjustment_status::VARCHAR(16777216) COLLATE 'en-ci'").alias("adjustment_status"),
    df["adjustment_result"],
    df["adjustment_timestamp"],
    f.expr("insert_cmd::VARCHAR(16777216) COLLATE 'en-ci'").alias("insert_cmd"),
    f.expr("update_cmd::VARCHAR(16777216) COLLATE 'en-ci'").alias("update_cmd"),
    f.expr("insert_summary_cmd::VARCHAR(16777216) COLLATE 'en-ci'").alias("insert_summary_cmd")
  )
  return df
