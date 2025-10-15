# Azure Fabric notebook source
"""
Gold Layer: Create ML-ready claims features
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, current_timestamp
import logging

# COMMAND ----------

SILVER_CLAIMS_PATH = "Tables/silver_claims"
GOLD_FEATURES_PATH = "Tables/gold_claims_features"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read Silver claims
        logger.info(f"Reading from {SILVER_CLAIMS_PATH}")
        df_claims = spark.read.format("delta").load(SILVER_CLAIMS_PATH)
        
        record_count = df_claims.count()
        logger.info(f"Read {record_count} claims from Silver")
        
        # Simple aggregation features by customer
        claims_features = df_claims.groupBy("customer_id").agg(
            count("claim_id").alias("total_claims"),
            spark_sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            spark_max("claim_amount").alias("max_claim_amount")
        )
        
        # Add feature timestamp
        claims_features = claims_features.withColumn("feature_timestamp", current_timestamp())
        
        feature_count = claims_features.count()
        logger.info(f"Created features for {feature_count} customers")
        
        # Write to Gold with Purview metadata
        logger.info(f"Writing to {GOLD_FEATURES_PATH}")
        claims_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("description", "Gold layer: Aggregated claims features for ML") \
            .save(GOLD_FEATURES_PATH)
        
        logger.info("✓ Claims features creation completed")
        
    except Exception as e:
        logger.error(f"✗ Failed to create claims features: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
