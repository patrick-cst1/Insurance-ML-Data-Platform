# Databricks notebook source
"""
Silver Layer: Clean and standardize claims with SCD Type 2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, coalesce
import sys

sys.path.append("/Workspace/framework/libs")
from delta_ops import read_delta, write_delta
from purview_integration import PurviewMetadata
from data_quality import check_nulls, detect_duplicates
from feature_utils import create_scd2_features
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

BRONZE_PATH = "Tables/bronze_claims"
SILVER_PATH = "Tables/silver_claims"

# COMMAND ----------

def main():
    logger = get_logger("silver_clean_claims")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "clean_claims"):
        
        try:
            df_bronze = read_delta(spark, BRONZE_PATH)
            logger.info(f"Read {df_bronze.count()} records from Bronze")
            
            # Data cleaning with safe type casting
            df_cleaned = df_bronze \
                .dropDuplicates(["claim_id"]) \
                .filter(col("claim_id").isNotNull()) \
                .withColumn("claim_status", upper(trim(col("claim_status")))) \
                .withColumn("claim_type", upper(trim(col("claim_type")))) \
                .withColumn("claim_amount", 
                           when(col("claim_amount").cast("double").isNotNull(), 
                                col("claim_amount").cast("double"))
                           .otherwise(0.0)) \
                .filter(col("claim_amount") > 0)
            
            # Data quality checks
            null_check = check_nulls(df_cleaned, columns=["claim_id", "policy_id", "claim_amount"], threshold=0.01)
            if not null_check["passed"]:
                logger.warning(f"Null check violations: {null_check['violations']}")
            
            dup_check = detect_duplicates(df_cleaned, key_columns=["claim_id"])
            if not dup_check["passed"]:
                logger.error(f"Found {dup_check['duplicate_count']} duplicates!")
            
            # Apply SCD2 for tracking claim status changes
            df_scd2 = create_scd2_features(
                df_cleaned,
                entity_keys=["claim_id"],
                timestamp_column="ingestion_timestamp",
                hash_columns=["claim_status", "claim_amount"]
            )
            
            logger.info(f"Writing {df_scd2.count()} records to Silver")
            metadata = PurviewMetadata.get_silver_metadata("silver_claims", has_scd2=True, pii=False)
            write_delta(
                df=df_scd2,
                path=SILVER_PATH,
                mode="overwrite",
                description=metadata["description"],
                tags=metadata["tags"]
            )
            logger.info("Silver claims cleaning completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to clean claims: {str(e)}")
            raise

# COMMAND ----------

if __name__ == "__main__":
    main()
