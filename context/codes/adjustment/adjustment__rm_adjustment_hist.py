import snowflake.snowpark.functions as f
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, TimestampType, VariantType
from datetime import datetime
import json, pytz

def model(dbt, session):

    # dbt configuration
    dbt.config(
        materialized="incremental",
        python_version="3.11",
        tags=["publish_mrm_rm_adjustment", "sensitivity_adjustment", "adjustment"]
    )

    # Validate and get configuration
    cobid = dbt.config.get("cobid")
    # References to the base models
    df_adjustments_model = dbt.ref('fact__rm_sensitivity_measures_log')
    df_sensitivity_adjustment_hist = dbt.ref("adjustment__sensitivity_adjustment_hist")
    df_sensitivity_adjustment_hist = df_sensitivity_adjustment_hist.filter(f.col("COBID") == cobid)

    schema_model = StructType([
      StructField("run_log_id", IntegerType(),True),
      StructField("cobid", IntegerType(),True),
      StructField("adjustment_status", StringType(),True),
      StructField("adjustment_timestamp", TimestampType(),True)
    ])
    result_adj = []

    if cobid != 19000101:
        run_log_id = session.sql("select batch.seq_run_log.nextval").collect()[0][0]

        df_run_log = session.create_dataframe(
            [[run_log_id, cobid, datetime.now(pytz.timezone('Europe/London')),"LOAD_SENSITIVITY_ADJUSTMENT", "sensitivity", False]],
            schema=["RUN_LOG_ID","COBID","END_TIME","PROC_NAME","PROC_PARAMETERS","ERROR"])
        df_run_log.write.save_as_table("batch.RUN_LOG", mode="append",column_order="name")
        
        proc_return = session.sql(f"CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS ('sensitivity','RaptorReporting','LOAD_SENSITIVITY_ADJUSTMENT', '{run_log_id}','')").collect()[0][0]
        result_adj = [[run_log_id, cobid, proc_return, datetime.now(pytz.timezone('Europe/London'))]]

     # Create dataframe
    df = session.create_dataframe(result_adj, schema=schema_model)

    df = df.select(
        df["run_log_id"],
        df["cobid"],
        f.expr("adjustment_status::VARCHAR(16777216) COLLATE 'en-ci'").alias("adjustment_status"),
        df["adjustment_timestamp"]
    )
    return df



