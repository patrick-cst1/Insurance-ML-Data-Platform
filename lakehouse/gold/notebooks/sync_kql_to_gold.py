# Azure Fabric notebook source
"""
Gold Layer: Sync KQL materialized view to Gold Lakehouse
Simplified version - hourly sync for ML features
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging

# COMMAND ----------

KQL_CLUSTER = "https://insurance.kusto.fabric.microsoft.com"  # Replace with your KQL endpoint
KQL_DATABASE = "insurance_realtime"
KQL_QUERY = "claims_hourly | where event_timestamp > ago(7d)"  # Last 7 days
GOLD_PATH = "Tables/gold_realtime_claims_features"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read from KQL Database
        logger.info(f"Reading from KQL: {KQL_DATABASE}.claims_hourly")
        
        df_kql = spark.read \
            .format("com.microsoft.kusto.spark.synapse.datasource") \
            .option("kustoCluster", KQL_CLUSTER) \
            .option("kustoDatabase", KQL_DATABASE) \
            .option("kustoQuery", KQL_QUERY) \
            .load()
        
        record_count = df_kql.count()
        logger.info(f"Read {record_count} aggregated records from KQL")
        
        # Add sync timestamp
        df_kql = df_kql.withColumn("sync_timestamp", current_timestamp())
        
        # Write to Gold Lakehouse
        logger.info(f"Writing to {GOLD_PATH}")
        df_kql.write \
            .format("delta") \
            .mode("overwrite") \
            .option("description", "Gold layer: Real-time claims features from KQL aggregations") \
            .save(GOLD_PATH)
        
        logger.info("✓ KQL to Gold sync completed")
        
    except Exception as e:
        logger.error(f"✗ Failed to sync KQL to Gold: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
