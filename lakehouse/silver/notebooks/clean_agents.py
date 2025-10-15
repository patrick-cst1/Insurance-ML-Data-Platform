# Azure Fabric notebook source
"""
Silver Layer: Clean and standardize agents
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, current_timestamp, lit
import logging

# COMMAND ----------

BRONZE_PATH = "Tables/bronze_agents"
SILVER_PATH = "Tables/silver_agents"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read from Bronze
        logger.info(f"Reading from {BRONZE_PATH}")
        df_bronze = spark.read.format("delta").load(BRONZE_PATH)
        
        record_count = df_bronze.count()
        logger.info(f"Read {record_count} records from Bronze")
        
        # Data cleaning
        df_cleaned = df_bronze \
            .dropDuplicates(["agent_id"]) \
            .filter(col("agent_id").isNotNull()) \
            .withColumn("agent_type", upper(trim(col("agent_type")))) \
            .withColumn("status", upper(trim(col("status")))) \
            .withColumn("region", trim(col("region"))) \
            .withColumn("processed_timestamp", current_timestamp())
        
        # Add SCD Type 2 columns
        df_cleaned = df_cleaned \
            .withColumn("effective_from", col("ingestion_timestamp")) \
            .withColumn("effective_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))
        
        cleaned_count = df_cleaned.count()
        logger.info(f"Cleaned records: {cleaned_count} (removed {record_count - cleaned_count} invalid records)")
        
        # Write to Silver with Purview metadata
        logger.info(f"Writing to {SILVER_PATH}")
        df_cleaned.write \
            .format("delta") \
            .mode("overwrite") \
            .option("description", "Silver layer: Cleaned agents with SCD Type 2 tracking") \
            .save(SILVER_PATH)
        
        logger.info("✓ Silver agents cleaning completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Failed to clean agents: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
