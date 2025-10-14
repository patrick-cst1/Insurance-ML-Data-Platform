"""
Initialize control tables for data platform
- Watermark control table
- Data quality results table
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType, DoubleType
import sys
import os

sys.path.append("/Workspace/framework/libs")
from delta_ops import write_delta
from logging_utils import get_logger


def init_watermark_table(spark: SparkSession, table_path: str = "Tables/watermark_control"):
    """Initialize watermark control table."""
    logger = get_logger("init_watermark_table")
    
    schema = StructType([
        StructField("source_name", StringType(), False),
        StructField("watermark_value", StringType(), False),
        StructField("updated_at", TimestampType(), False)
    ])
    
    # Create empty table
    df_empty = spark.createDataFrame([], schema)
    
    write_delta(
        df=df_empty,
        path=table_path,
        mode="overwrite"
    )
    
    logger.info(f"Watermark control table initialized at {table_path}")


def init_dq_results_table(spark: SparkSession, table_path: str = "Tables/dq_check_results"):
    """Initialize data quality results table."""
    logger = get_logger("init_dq_results_table")
    
    schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("record_count", IntegerType(), True),
        StructField("overall_passed", BooleanType(), False),
        StructField("duplicate_check", BooleanType(), True),
        StructField("null_check", BooleanType(), True),
        StructField("freshness_check", BooleanType(), True),
        StructField("check_timestamp", TimestampType(), False),
        StructField("details", StringType(), True)
    ])
    
    # Create empty table
    df_empty = spark.createDataFrame([], schema)
    
    write_delta(
        df=df_empty,
        path=table_path,
        mode="overwrite"
    )
    
    logger.info(f"DQ results table initialized at {table_path}")


def main():
    """Main initialization routine."""
    logger = get_logger("init_control_tables")
    spark = SparkSession.builder.appName("init_control_tables").getOrCreate()
    
    logger.info("Starting control tables initialization...")
    
    # Initialize tables
    init_watermark_table(spark)
    init_dq_results_table(spark)
    
    logger.info("All control tables initialized successfully")


if __name__ == "__main__":
    main()
