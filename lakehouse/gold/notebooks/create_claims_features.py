# Databricks notebook source
"""
Gold Layer: Create ML-ready claims features with point-in-time correctness
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date
import sys

sys.path.append("/Workspace/framework/libs")
from delta_ops import read_delta, write_delta
from purview_integration import PurviewMetadata
from feature_utils import build_aggregation_features, add_feature_metadata
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

SILVER_CLAIMS_PATH = "Tables/silver_claims"
GOLD_FEATURES_PATH = "Tables/gold_claims_features"

# COMMAND ----------

def main():
    logger = get_logger("gold_claims_features")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "create_claims_features"):
        
        # Read Silver claims
        df_claims = read_delta(spark, SILVER_CLAIMS_PATH)
        logger.info(f"Read {df_claims.count()} claims from Silver")
        
        # Build time-window aggregation features (30/90/365 days)
        claims_features = build_aggregation_features(
            df=df_claims,
            entity_keys=["customer_id"],
            timestamp_column="claim_date",
            aggregation_windows=[30, 90, 365],
            agg_columns={
                "claim_amount": ["sum", "avg", "max", "count"],
                "claim_id": ["count"]
            }
        )
        
        # Add derived features
        claims_features = claims_features \
            .withColumnRenamed("claim_id_count_30d", "claims_count_30d") \
            .withColumnRenamed("claim_id_count_90d", "claims_count_90d") \
            .withColumnRenamed("claim_id_count_365d", "claims_count_365d")
        
        # Calculate days since last claim
        latest_claim = df_claims.groupBy("customer_id") \
            .agg({"claim_date": "max"}) \
            .withColumnRenamed("max(claim_date)", "last_claim_date")
        
        claims_features = claims_features.join(latest_claim, on="customer_id", how="left") \
            .withColumn("days_since_last_claim", datediff(current_date(), col("last_claim_date"))) \
            .drop("last_claim_date")
        
        # Add feature metadata (event_time for point-in-time join)
        claims_features = add_feature_metadata(claims_features)
        
        logger.info(f"Created features for {claims_features.count()} customers")
        
        # Write to Gold
        metadata = PurviewMetadata.get_gold_metadata("gold_claims_features", feature_type="claims")
        write_delta(
            df=claims_features,
            path=GOLD_FEATURES_PATH,
            mode="overwrite",
            partition_by=["feature_timestamp"],
            description=metadata["description"],
            tags=metadata["tags"]
        )
        
        logger.info("Claims features creation completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
