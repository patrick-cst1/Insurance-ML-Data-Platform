# Databricks notebook source
"""
Silver Layer: Clean and standardize customers with SCD Type 2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import read_delta, write_delta
from purview_integration import PurviewMetadata
from data_quality import check_nulls, detect_duplicates
from feature_utils import create_scd2_features
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

BRONZE_PATH = "Tables/bronze_customers"
SILVER_PATH = "Tables/silver_customers"

# COMMAND ----------

def main():
    logger = get_logger("silver_clean_customers")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "clean_customers"):
        
        try:
            # Read from Bronze
            df_bronze = read_delta(spark, BRONZE_PATH)
            logger.info(f"Read {df_bronze.count()} records from Bronze")
            
            # Data cleaning
            df_cleaned = df_bronze \
                .dropDuplicates(["customer_id"]) \
                .filter(col("customer_id").isNotNull()) \
                .withColumn("gender", upper(trim(col("gender")))) \
                .withColumn("location", trim(col("location"))) \
                .withColumn("join_date", to_date(col("join_date"))) \
                .filter(col("age") > 0) \
                .filter(col("age") < 120)
            
            # Data quality checks
            null_check = check_nulls(df_cleaned, columns=["customer_id", "name"], threshold=0.01)
            if not null_check["passed"]:
                logger.warning(f"Null check violations: {null_check['violations']}")
            
            dup_check = detect_duplicates(df_cleaned, key_columns=["customer_id"])
            if not dup_check["passed"]:
                logger.error(f"Found {dup_check['duplicate_count']} duplicates!")
            
            # Apply SCD2 for dimension tracking
            df_scd2 = create_scd2_features(
                df_cleaned,
                entity_keys=["customer_id"],
                timestamp_column="ingestion_timestamp"
            )
            
            logger.info(f"Writing {df_scd2.count()} records to Silver")
            
            # Write to Silver with Purview metadata
            metadata = PurviewMetadata.get_silver_metadata("silver_customers", has_scd2=True, pii=True)
            write_delta(
                df=df_scd2,
                path=SILVER_PATH,
                mode="overwrite",
                description=metadata["description"],
                tags=metadata["tags"]
            )
            
            logger.info("Silver customers cleaning completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to clean customers: {str(e)}")
            raise

# COMMAND ----------

if __name__ == "__main__":
    main()
