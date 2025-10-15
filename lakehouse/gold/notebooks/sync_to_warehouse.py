# Azure Fabric notebook source
"""
Gold Layer: Sync Gold tables to Fabric Warehouse (optional)
Enables SQL endpoint access for BI tools and external consumers

Use Case: When downstream consumers need T-SQL access instead of Spark SQL
"""

from pyspark.sql import SparkSession
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import read_delta
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

# Warehouse connection (configured in Fabric workspace)
WAREHOUSE_NAME = "wh_insurance_analytics"
WAREHOUSE_SCHEMA = "gold"

# Tables to sync
GOLD_TABLES = [
    "gold_claims_features",
    "gold_customer_features",
    "gold_risk_features",
    "gold_streaming_features"
]

# COMMAND ----------

def sync_table_to_warehouse(spark, table_name: str, warehouse_name: str, schema: str):
    """
    Sync Delta table from Lakehouse to Warehouse.
    
    In Fabric, this can be done via:
    1. Direct table reference (Lakehouse SQL endpoint automatically creates views)
    2. Warehouse shortcuts to Lakehouse tables
    3. Manual CTAS (Create Table As Select)
    
    Method 1 (Recommended): Use Lakehouse SQL endpoint - automatic, no sync needed
    Method 2 (This notebook): Explicit sync for full control
    """
    logger = get_logger("sync_to_warehouse")
    
    try:
        # Read from Lakehouse Gold
        df = read_delta(spark, f"Tables/{table_name}")
        count = df.count()
        logger.info(f"Read {count} rows from Lakehouse table: {table_name}")
        
        # Write to Warehouse using JDBC or direct SQL
        # Option 1: Use Fabric Warehouse connector (if available)
        # df.write.format("fabric.warehouse") \
        #     .option("warehouse", warehouse_name) \
        #     .option("schema", schema) \
        #     .option("table", table_name) \
        #     .mode("overwrite") \
        #     .save()
        
        # Option 2: Create external table in Warehouse pointing to Lakehouse
        # This is the recommended Fabric-native approach
        spark.sql(f"""
            CREATE OR REPLACE TABLE {warehouse_name}.{schema}.{table_name}
            USING DELTA
            LOCATION 'Tables/{table_name}'
        """)
        
        logger.info(f"✓ Synced {table_name} to Warehouse: {warehouse_name}.{schema}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to sync {table_name}: {e}")
        return False

# COMMAND ----------

def main():
    logger = get_logger("gold_warehouse_sync")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "sync_gold_to_warehouse"):
        
        logger.info(f"Starting Warehouse sync: {len(GOLD_TABLES)} tables")
        logger.info(f"Target: {WAREHOUSE_NAME}.{WAREHOUSE_SCHEMA}")
        
        results = {}
        for table_name in GOLD_TABLES:
            success = sync_table_to_warehouse(spark, table_name, WAREHOUSE_NAME, WAREHOUSE_SCHEMA)
            results[table_name] = "SUCCESS" if success else "FAILED"
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Warehouse Sync Summary")
        logger.info("=" * 60)
        for table_name, status in results.items():
            logger.info(f"{table_name}: {status}")
        
        failed_count = sum(1 for s in results.values() if s == "FAILED")
        if failed_count > 0:
            logger.warning(f"{failed_count} tables failed to sync")
        else:
            logger.info("✓ All Gold tables synced to Warehouse successfully")
        
        logger.info("\nNote: Fabric Lakehouse SQL endpoint provides automatic SQL access")
        logger.info("Explicit Warehouse sync is optional for advanced SQL features")

# COMMAND ----------

if __name__ == "__main__":
    main()
