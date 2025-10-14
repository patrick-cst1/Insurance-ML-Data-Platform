# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import write_delta
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

def main():
    logger = get_logger("bronze_ingest_customers")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "ingest_customers"):
        df = spark.read.csv("Files/samples/batch/customers.csv", header=True, inferSchema=True)
        df_enriched = df.withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        write_delta(df_enriched, "Tables/bronze_customers", mode="append", partition_by=["ingestion_date"], merge_schema=True)
        logger.info(f"Ingested {df_enriched.count()} customers")

# COMMAND ----------

if __name__ == "__main__":
    main()
