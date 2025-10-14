"""
Initialize Watermark Control Table
Creates the Delta table for tracking incremental processing watermarks
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import write_delta
from logging_utils import get_logger


def initialize_watermark_table(spark: SparkSession, table_path: str):
    """
    Initialize watermark control table with schema.
    
    Args:
        spark: SparkSession instance
        table_path: Path to watermark control table
    """
    logger = get_logger("initialize_watermark")
    
    logger.info(f"Initializing watermark control table at {table_path}")
    
    # Define schema
    schema = StructType([
        StructField("source_name", StringType(), nullable=False),
        StructField("watermark_value", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False)
    ])
    
    # Create empty DataFrame with schema
    df_watermark = spark.createDataFrame([], schema)
    
    # Write initial table
    write_delta(
        df=df_watermark,
        path=table_path,
        mode="overwrite"
    )
    
    logger.info("Watermark control table initialized successfully")
    
    # Add initial watermark entries for known sources
    initial_watermarks = [
        ("bronze_policies", "1900-01-01T00:00:00"),
        ("bronze_claims", "1900-01-01T00:00:00"),
        ("bronze_customers", "1900-01-01T00:00:00"),
        ("bronze_agents", "1900-01-01T00:00:00"),
        ("bronze_realtime_events", "1900-01-01T00:00:00")
    ]
    
    df_init = spark.createDataFrame(initial_watermarks, ["source_name", "watermark_value"]) \
        .withColumn("updated_at", current_timestamp())
    
    write_delta(
        df=df_init,
        path=table_path,
        mode="append"
    )
    
    logger.info(f"Added {len(initial_watermarks)} initial watermark entries")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("initialize_watermark").getOrCreate()
    watermark_path = "Tables/watermark_control"
    
    initialize_watermark_table(spark, watermark_path)
    
    print("✓ Watermark control table initialization completed")
