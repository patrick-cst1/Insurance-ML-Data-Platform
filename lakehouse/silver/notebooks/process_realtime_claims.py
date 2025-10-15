# Azure Fabric notebook source
"""
Silver Layer: Process real-time claims from Bronze Lakehouse
Simplified version - sync from Eventstream to Silver with SCD2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, current_timestamp, lit
import logging

# COMMAND ----------

BRONZE_PATH = "Tables/bronze_claims_events"
SILVER_PATH = "Tables/silver_claims_realtime"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read from Bronze (Eventstream sink)
        logger.info(f"Reading real-time claims from {BRONZE_PATH}")
        df_bronze = spark.read.format("delta").load(BRONZE_PATH)
        
        record_count = df_bronze.count()
        logger.info(f"Read {record_count} real-time claims from Bronze")
        
        # Data cleaning
        df_cleaned = df_bronze \
            .dropDuplicates(["claim_id"]) \
            .filter(col("claim_id").isNotNull()) \
            .filter(col("policy_id").isNotNull()) \
            .withColumn("claim_status", upper(trim(col("claim_status")))) \
            .withColumn("claim_amount", col("claim_amount").cast("double")) \
            .filter(col("claim_amount") > 0) \
            .withColumn("processed_timestamp", current_timestamp())
        
        # Add SCD Type 2 columns
        df_cleaned = df_cleaned \
            .withColumn("effective_from", col("event_timestamp")) \
            .withColumn("effective_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))
        
        cleaned_count = df_cleaned.count()
        logger.info(f"Cleaned records: {cleaned_count}")
        
        # Write to Silver with Purview metadata
        logger.info(f"Writing to {SILVER_PATH}")
        df_cleaned.write \
            .format("delta") \
            .mode("append") \
            .option("description", "Silver layer: Real-time claims from Eventstream with SCD Type 2") \
            .save(SILVER_PATH)
        
        logger.info("✓ Real-time claims processing completed")
        
    except Exception as e:
        logger.error(f"✗ Failed to process real-time claims: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
