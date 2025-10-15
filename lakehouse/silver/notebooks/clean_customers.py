# Azure Fabric notebook source
"""
Silver Layer: Clean and standardize customers
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, current_timestamp, lit
import logging

# COMMAND ----------

BRONZE_PATH = "Tables/bronze_customers"
SILVER_PATH = "Tables/silver_customers"

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
            .dropDuplicates(["customer_id"]) \
            .filter(col("customer_id").isNotNull()) \
            .withColumn("gender", upper(trim(col("gender")))) \
            .withColumn("location", trim(col("location"))) \
            .withColumn("join_date", to_date(col("join_date"))) \
            .filter(col("age") > 0) \
            .filter(col("age") < 120) \
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
            .option("description", "Silver layer: Cleaned customers with SCD Type 2 tracking (contains PII)") \
            .save(SILVER_PATH)
        
        logger.info("✓ Silver customers cleaning completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Failed to clean customers: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
