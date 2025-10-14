"""
Delta Lake Maintenance Script
Performs OPTIMIZE, ZORDER, and VACUUM operations on Delta tables
"""

from pyspark.sql import SparkSession
import sys
import os
from typing import List, Dict

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import optimize_delta
from logging_utils import get_logger, PipelineTimer


# Maintenance configuration for each table
MAINTENANCE_CONFIG: List[Dict] = [
    {
        "table_path": "Tables/bronze_policies",
        "zorder_columns": ["customer_id", "ingestion_date"],
        "vacuum_hours": 168  # 7 days
    },
    {
        "table_path": "Tables/bronze_claims",
        "zorder_columns": ["policy_id", "ingestion_date"],
        "vacuum_hours": 168
    },
    {
        "table_path": "Tables/bronze_customers",
        "zorder_columns": ["customer_id", "ingestion_date"],
        "vacuum_hours": 168
    },
    {
        "table_path": "Tables/bronze_agents",
        "zorder_columns": ["agent_id", "ingestion_date"],
        "vacuum_hours": 168
    },
    {
        "table_path": "Tables/bronze_realtime_events",
        "zorder_columns": ["ingestion_date"],
        "vacuum_hours": 168
    },
    {
        "table_path": "Tables/silver_policies",
        "zorder_columns": ["customer_id", "is_current"],
        "vacuum_hours": 336  # 14 days (keep longer for SCD2)
    },
    {
        "table_path": "Tables/silver_claims",
        "zorder_columns": ["claim_id", "policy_id"],
        "vacuum_hours": 336
    },
    {
        "table_path": "Tables/silver_customers",
        "zorder_columns": ["customer_id", "is_current"],
        "vacuum_hours": 336
    },
    {
        "table_path": "Tables/silver_agents",
        "zorder_columns": ["agent_id", "is_current"],
        "vacuum_hours": 336
    },
    {
        "table_path": "Tables/gold_claims_features",
        "zorder_columns": ["customer_id", "feature_timestamp"],
        "vacuum_hours": 720  # 30 days (keep features for longer)
    },
    {
        "table_path": "Tables/gold_customer_features",
        "zorder_columns": ["customer_id"],
        "vacuum_hours": 720
    },
    {
        "table_path": "Tables/gold_risk_features",
        "zorder_columns": ["customer_id"],
        "vacuum_hours": 720
    }
]


def run_maintenance(spark: SparkSession, table_config: Dict, logger):
    """
    Run maintenance operations on a single table.
    
    Args:
        spark: SparkSession instance
        table_config: Table maintenance configuration
        logger: Logger instance
    """
    table_path = table_config["table_path"]
    zorder_columns = table_config.get("zorder_columns", [])
    vacuum_hours = table_config.get("vacuum_hours")
    
    logger.info(f"Starting maintenance for {table_path}")
    
    try:
        with PipelineTimer(logger, f"optimize_{table_path}"):
            optimize_delta(
                spark=spark,
                path=table_path,
                zorder_by=zorder_columns if zorder_columns else None,
                vacuum_retention_hours=vacuum_hours
            )
        
        logger.info(f"✓ Maintenance completed for {table_path}")
        return {"table": table_path, "status": "SUCCESS"}
        
    except Exception as e:
        logger.error(f"✗ Maintenance failed for {table_path}: {str(e)}")
        return {"table": table_path, "status": "FAILED", "error": str(e)}


def main():
    """Run maintenance on all configured tables."""
    logger = get_logger("delta_maintenance")
    spark = SparkSession.builder.appName("delta_maintenance").getOrCreate()
    
    logger.info("=" * 60)
    logger.info("Starting Delta Lake Maintenance")
    logger.info(f"Tables to maintain: {len(MAINTENANCE_CONFIG)}")
    logger.info("=" * 60)
    
    results = []
    
    for table_config in MAINTENANCE_CONFIG:
        result = run_maintenance(spark, table_config, logger)
        results.append(result)
    
    # Summary
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = len(results) - success_count
    
    logger.info("=" * 60)
    logger.info("Maintenance Summary")
    logger.info(f"Total tables: {len(results)}")
    logger.info(f"Success: {success_count}")
    logger.info(f"Failed: {failed_count}")
    logger.info("=" * 60)
    
    if failed_count > 0:
        failed_tables = [r["table"] for r in results if r["status"] == "FAILED"]
        logger.error(f"Failed tables: {failed_tables}")
        raise Exception(f"Maintenance failed for {failed_count} tables")
    else:
        logger.info("✓ All maintenance operations completed successfully")


if __name__ == "__main__":
    main()
