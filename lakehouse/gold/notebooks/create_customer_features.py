# Azure Fabric notebook source
"""
Gold Layer: Create customer dimension features
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, when, current_timestamp
import logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read Silver tables
        logger.info("Reading customers and policies from Silver")
        df_customers = spark.read.format("delta").load("Tables/silver_customers")
        df_policies = spark.read.format("delta").load("Tables/silver_policies")
        
        logger.info(f"Read {df_customers.count()} customers and {df_policies.count()} policies")
        
        # Aggregate policy features per customer
        policy_agg = df_policies.groupBy("customer_id").agg(
            count("policy_id").alias("total_policies"),
            spark_sum(when(col("status") == "ACTIVE", 1).otherwise(0)).alias("active_policies"),
            avg("premium").alias("avg_premium"),
            spark_sum("premium").alias("total_premium")
        )
        
        # Join with customer base
        customer_features = df_customers.join(policy_agg, on="customer_id", how="left") \
            .fillna(0, subset=["total_policies", "active_policies", "avg_premium", "total_premium"]) \
            .withColumn("feature_timestamp", current_timestamp())
        
        feature_count = customer_features.count()
        logger.info(f"Created features for {feature_count} customers")
        
        # Write to Gold with Purview metadata
        logger.info("Writing to Tables/gold_customer_features")
        customer_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("description", "Gold layer: Customer dimension features for ML") \
            .save("Tables/gold_customer_features")
        
        logger.info("✓ Customer features creation completed")
        
    except Exception as e:
        logger.error(f"✗ Failed to create customer features: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
