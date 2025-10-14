# Databricks notebook source
"""
Gold Layer: Create customer dimension features
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, datediff, current_date, when
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import read_delta, write_delta
from purview_integration import PurviewMetadata
from feature_utils import add_feature_metadata
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

def main():
    logger = get_logger("gold_customer_features")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "create_customer_features"):
        
        # Read Silver tables
        df_customers = read_delta(spark, "Tables/silver_customers").filter(col("is_current") == True)
        df_policies = read_delta(spark, "Tables/silver_policies").filter(col("is_current") == True)
        
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
            .fillna(0, subset=["total_policies", "active_policies", "avg_premium", "total_premium"])
        
        # Calculate customer lifetime metrics
        customer_features = customer_features \
            .withColumn("customer_tenure_days", datediff(current_date(), col("join_date"))) \
            .withColumn("customer_lifetime_value", col("total_premium") * col("customer_tenure_days") / 365)
        
        # Add metadata
        customer_features = add_feature_metadata(customer_features)
        
        logger.info(f"Created features for {customer_features.count()} customers")
        
        # Write to Gold
        metadata = PurviewMetadata.get_gold_metadata("gold_customer_features", feature_type="customer")
        write_delta(
            df=customer_features,
            path="Tables/gold_customer_features",
            mode="overwrite",
            description=metadata["description"],
            tags=metadata["tags"]
        )
        logger.info("Customer features creation completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
