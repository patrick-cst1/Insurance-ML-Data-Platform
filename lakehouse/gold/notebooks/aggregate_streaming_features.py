# Databricks notebook source
"""
Gold Layer: Aggregate streaming features for real-time ML inference
Creates time-window aggregations from streaming events
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, window, expr
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
    logger = get_logger("gold_aggregate_streaming")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "aggregate_streaming_features"):
        
        # Read Silver streaming claims
        df_stream = read_delta(spark, "Tables/silver_realtime_claims")
        logger.info(f"Read {df_stream.count()} streaming events from Silver")
        
        # Aggregate by customer (1-hour and 24-hour windows)
        # Note: For true streaming, this would use structured streaming
        # This batch version processes accumulated events
        
        # 1-hour aggregations
        agg_1hr = df_stream \
            .filter(col("event_time") >= expr("current_timestamp() - interval 1 hour")) \
            .groupBy("policy_id") \
            .agg(
                count("claim_id").alias("claims_submitted_last_1hr"),
                spark_sum("amount").alias("amount_sum_1hr"),
                avg("amount").alias("avg_claim_amount_1hr")
            )
        
        # 24-hour aggregations
        agg_24hr = df_stream \
            .filter(col("event_time") >= expr("current_timestamp() - interval 24 hours")) \
            .groupBy("policy_id") \
            .agg(
                count("claim_id").alias("claims_submitted_last_24hr"),
                spark_sum("amount").alias("amount_sum_24hr"),
                avg("amount").alias("avg_claim_amount_24hr")
            )
        
        # Combine aggregations
        streaming_features = agg_1hr.join(agg_24hr, on="policy_id", how="outer")
        
        # Add metadata
        streaming_features = add_feature_metadata(streaming_features)
        
        logger.info(f"Created streaming features for {streaming_features.count()} policies")
        
        # Write to Gold
        metadata = PurviewMetadata.get_gold_metadata("gold_streaming_features", feature_type="streaming")
        write_delta(
            df=streaming_features,
            path="Tables/gold_streaming_features",
            mode="overwrite",
            description=metadata["description"],
            tags=metadata["tags"]
        )
        
        logger.info("Streaming feature aggregation completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
