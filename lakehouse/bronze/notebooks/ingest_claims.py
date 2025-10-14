# Databricks notebook source
"""
Bronze Layer: Ingest Claims CSV to Delta
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
import sys

sys.path.append("/Workspace/framework/libs")
from delta_ops import write_delta
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

SOURCE_PATH = "Files/samples/batch/claims.csv"
TARGET_PATH = "Tables/bronze_claims"
PARTITION_COLUMN = "ingestion_date"

# COMMAND ----------

def main():
    """Ingest claims from CSV to Bronze Delta table."""
    
    logger = get_logger("bronze_ingest_claims")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "ingest_claims"):
        
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(SOURCE_PATH)
        
        df_enriched = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        logger.info(f"Read {df_enriched.count()} claims")
        
        write_delta(
            df=df_enriched,
            path=TARGET_PATH,
            mode="append",
            partition_by=[PARTITION_COLUMN],
            merge_schema=True
        )
        
        logger.info("Bronze claims ingestion completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
