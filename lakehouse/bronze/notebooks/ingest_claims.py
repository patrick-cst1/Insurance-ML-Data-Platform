# Azure Fabric notebook source
"""
Bronze Layer: Ingest Claims CSV to Delta
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col
import logging

# COMMAND ----------

SOURCE_PATH = "Files/samples/batch/claims.csv"
TARGET_PATH = "Tables/bronze_claims"
PARTITION_COLUMN = "ingestion_date"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    """Ingest claims from CSV to Bronze Delta table."""
    
    spark = SparkSession.builder.getOrCreate()
    
    try:
        logger.info(f"Reading claims from {SOURCE_PATH}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(SOURCE_PATH)
        
        df_enriched = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        record_count = df_enriched.count()
        logger.info(f"Read {record_count} claims")
        
        # Basic validation
        required_columns = ["claim_id", "policy_id"]
        for col_name in required_columns:
            if col_name not in df_enriched.columns:
                raise ValueError(f"Missing required column: {col_name}")
            
            null_count = df_enriched.filter(col(col_name).isNull()).count()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in {col_name}")
        
        logger.info(f"Writing to {TARGET_PATH}")
        df_enriched.write \
            .format("delta") \
            .mode("append") \
            .partitionBy(PARTITION_COLUMN) \
            .option("mergeSchema", "true") \
            .save(TARGET_PATH)
        
        logger.info("✓ Bronze claims ingestion completed")
        
    except Exception as e:
        logger.error(f"✗ Claims ingestion failed: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
