# Azure Fabric notebook source
"""
Silver Layer: Process streaming events for validation and deduplication
This notebook processes events from Bronze realtime table or directly from KQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, row_number, current_timestamp, to_date
from pyspark.sql.window import Window
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import read_delta, write_delta
from purview_integration import PurviewMetadata
from data_quality import check_nulls
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

def main():
    logger = get_logger("silver_process_streaming")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "process_streaming_silver"):
        
        # Read from Bronze realtime events
        df_bronze_stream = read_delta(spark, "Tables/bronze_realtime_events")
        logger.info(f"Read {df_bronze_stream.count()} streaming events from Bronze")
        
        # Validation: remove invalid records
        df_valid = df_bronze_stream \
            .filter(col("claim_id").isNotNull()) \
            .filter(col("amount") > 0) \
            .filter(col("status").isNotNull())
        
        # Deduplication: keep latest event per claim_id
        window_spec = Window.partitionBy("claim_id").orderBy(col("event_time").desc())
        df_dedup = df_valid \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter("rn = 1") \
            .drop("rn")
        
        # Add processing timestamp for lineage and SLA tracking
        df_output = df_dedup \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp()))
        
        # Data quality check
        null_check = check_nulls(df_output, columns=["claim_id", "amount"], threshold=0.0)
        if not null_check["passed"]:
            logger.error(f"Streaming data quality failed: {null_check['violations']}")
        
        logger.info(f"Writing {df_output.count()} validated streaming records to Silver")
        
        # Write to Silver streaming table
        metadata = PurviewMetadata.get_silver_metadata("silver_realtime_claims", has_scd2=False, pii=False)
        write_delta(
            df=df_output,
            path="Tables/silver_realtime_claims",
            mode="overwrite",
            partition_by=["ingestion_date"],
            description=metadata["description"],
            tags=metadata["tags"]
        )
        
        logger.info("Streaming Silver processing completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
