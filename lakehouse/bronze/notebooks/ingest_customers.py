# Azure Fabric notebook source
"""
Bronze Layer: Ingest Customers CSV to Delta
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col
import logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        logger.info("Reading customers from CSV")
        df = spark.read.csv("Files/samples/batch/customers.csv", header=True, inferSchema=True)
        
        df_enriched = df.withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        record_count = df_enriched.count()
        logger.info(f"Read {record_count} customers")
        
        # Basic validation
        if "customer_id" not in df_enriched.columns:
            raise ValueError("Missing required column: customer_id")
        
        null_count = df_enriched.filter(col("customer_id").isNull()).count()
        if null_count > 0:
            logger.warning(f"Found {null_count} null customer_id values")
        
        logger.info("Writing to Tables/bronze_customers")
        df_enriched.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .option("mergeSchema", "true") \
            .save("Tables/bronze_customers")
        
        logger.info("✓ Bronze customers ingestion completed")
        
    except Exception as e:
        logger.error(f"✗ Customers ingestion failed: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
