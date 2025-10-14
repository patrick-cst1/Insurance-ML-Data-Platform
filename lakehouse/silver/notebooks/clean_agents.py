# Databricks notebook source
"""
Silver Layer: Clean and standardize agents with SCD Type 2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper
import sys

sys.path.append("/Workspace/framework/libs")
from delta_ops import read_delta, write_delta
from purview_integration import PurviewMetadata
from data_quality import check_nulls, detect_duplicates
from feature_utils import create_scd2_features
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

BRONZE_PATH = "Tables/bronze_agents"
SILVER_PATH = "Tables/silver_agents"

# COMMAND ----------

def main():
    logger = get_logger("silver_clean_agents")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "clean_agents"):
        
        try:
            # Read from Bronze
            df_bronze = read_delta(spark, BRONZE_PATH)
            logger.info(f"Read {df_bronze.count()} records from Bronze")
            
            # Data cleaning
            df_cleaned = df_bronze \
                .dropDuplicates(["agent_id"]) \
                .filter(col("agent_id").isNotNull()) \
                .withColumn("agent_type", upper(trim(col("agent_type")))) \
                .withColumn("status", upper(trim(col("status")))) \
                .withColumn("region", trim(col("region")))
            
            # Data quality checks
            null_check = check_nulls(df_cleaned, columns=["agent_id", "name"], threshold=0.01)
            if not null_check["passed"]:
                logger.warning(f"Null check violations: {null_check['violations']}")
            
            dup_check = detect_duplicates(df_cleaned, key_columns=["agent_id"])
            if not dup_check["passed"]:
                logger.error(f"Found {dup_check['duplicate_count']} duplicates!")
                raise ValueError(f"Duplicate agents detected: {dup_check['duplicate_count']}")
            
            # Apply SCD2 for dimension tracking
            df_scd2 = create_scd2_features(
                df_cleaned,
                entity_keys=["agent_id"],
                timestamp_column="ingestion_timestamp"
            )
            
            logger.info(f"Writing {df_scd2.count()} records to Silver")
            
            # Write to Silver with Purview metadata
            metadata = PurviewMetadata.get_silver_metadata("silver_agents", has_scd2=True, pii=False)
            write_delta(
                df=df_scd2,
                path=SILVER_PATH,
                mode="overwrite",
                description=metadata["description"],
                tags=metadata["tags"]
            )
            
            logger.info("Silver agents cleaning completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to clean agents: {str(e)}")
            raise

# COMMAND ----------

if __name__ == "__main__":
    main()
