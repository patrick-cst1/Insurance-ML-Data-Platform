# Databricks notebook source
"""
Bronze Layer: Ingest Policies CSV to Delta
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
import sys
import os

# Add framework libs to path
sys.path.append("/Workspace/framework/libs")

from delta_ops import write_delta
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

# Configuration
SOURCE_PATH = "Files/samples/batch/policies.csv"
TARGET_PATH = "Tables/bronze_policies"
PARTITION_COLUMN = "ingestion_date"

# COMMAND ----------

def main():
    """Ingest policies from CSV to Bronze Delta table."""
    
    logger = get_logger("bronze_ingest_policies")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "ingest_policies"):
        
        # Read source CSV
        logger.info(f"Reading policies from {SOURCE_PATH}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(SOURCE_PATH)
        
        # Add metadata columns
        df_enriched = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        logger.info(f"Read {df_enriched.count()} policies")
        
        # Write to Bronze Delta (append mode)
        logger.info(f"Writing to {TARGET_PATH}")
        write_delta(
            df=df_enriched,
            path=TARGET_PATH,
            mode="append",
            partition_by=[PARTITION_COLUMN],
            merge_schema=True
        )
        
        logger.info("Bronze ingestion completed successfully")

# COMMAND ----------

if __name__ == "__main__":
    main()
