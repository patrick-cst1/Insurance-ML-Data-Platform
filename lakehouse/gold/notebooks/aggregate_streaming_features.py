# Azure Fabric notebook source
"""
Gold Layer: Aggregate streaming policy events (Event Medallion Pattern)
Processes real-time policy status changes for operational dashboards

Real-time data: Policy status changes (ACTIVE → SUSPENDED → CANCELLED, etc.)
Batch data: Historical claims, policies, customers (CSV files)

Architecture:
- Real-time queries → KQL Database (materialized views) 
- Batch ML training → Lakehouse Gold tables (aggregated policy events)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import write_delta
from purview_integration import PurviewMetadata
from feature_utils import add_feature_metadata
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

# KQL Database connection (configured in Fabric workspace)
KQL_DATABASE = "kql_insurance_realtime"
KQL_CLUSTER_URI = ""  # Auto-detected in Fabric workspace

# COMMAND ----------

def main():
    logger = get_logger("gold_sync_kql_features")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "sync_kql_streaming_features"):
        
        # Read from KQL materialized views using Kusto Spark connector
        # This syncs pre-computed real-time aggregations to Lakehouse
        try:
            # Option 1: Read from KQL using Spark connector (if available)
            # df_1hr = spark.read.format("kusto") \
            #     .option("kustoCluster", KQL_CLUSTER_URI) \
            #     .option("kustoDatabase", KQL_DATABASE) \
            #     .option("kustoQuery", "claims_1hr_agg") \
            #     .load()
            
            # Option 2: Read from Silver (for now - KQL connector setup required)
            logger.info("Reading streaming policy events from Silver layer")
            df_stream = spark.read.table("silver_realtime_policy_events")
            
            if df_stream.count() == 0:
                logger.info("No streaming events available, skipping sync")
                return
            
            # Compute aggregations (in production, these come from KQL materialized views)
            from pyspark.sql.functions import count, countDistinct, max as spark_max, expr, collect_list
            
            # 24-hour policy event aggregations
            policy_activity = df_stream \
                .filter(col("event_time") >= expr("current_timestamp() - interval 24 hours")) \
                .groupBy("policy_id") \
                .agg(
                    count("event_id").alias("status_changes_24hr"),
                    countDistinct("new_status").alias("unique_statuses_24hr"),
                    spark_max("event_time").alias("last_event_time"),
                    collect_list("new_status").alias("status_history_24hr")
                )
            
            # 7-day policy event aggregations
            policy_activity_7d = df_stream \
                .filter(col("event_time") >= expr("current_timestamp() - interval 7 days")) \
                .groupBy("policy_id") \
                .agg(
                    count("event_id").alias("status_changes_7d"),
                    countDistinct("new_status").alias("unique_statuses_7d")
                )
            
            # Join and create final feature set
            streaming_features = policy_activity.join(policy_activity_7d, on="policy_id", how="outer")
            streaming_features = add_feature_metadata(streaming_features)
            
            logger.info(f"Aggregated streaming policy events for {streaming_features.count()} policies")
            
            # Persist to Gold Lakehouse for operational dashboards and ML
            metadata = PurviewMetadata.get_gold_metadata("gold_realtime_policy_activity", feature_type="streaming")
            write_delta(
                df=streaming_features,
                path="Tables/gold_realtime_policy_activity",
                mode="overwrite",
                description=metadata["description"],
                tags=metadata["tags"]
            )
            
            logger.info("✓ Real-time policy activity aggregation completed (Event Medallion pattern)")
            
        except Exception as e:
            logger.error(f"Failed to sync KQL features: {e}")
            logger.info("Ensure KQL Database and materialized views are configured")
            raise

# COMMAND ----------

if __name__ == "__main__":
    main()
