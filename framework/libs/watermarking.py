from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from typing import Optional
import logging


WATERMARK_TABLE = "watermark_control"


def get_current_watermark(
    spark: SparkSession,
    watermark_table_path: str,
    source_name: str,
    default_value: Optional[str] = None
) -> Optional[str]:
    """
    Get current watermark value for incremental processing.
    
    Args:
        spark: SparkSession instance
        watermark_table_path: Path to watermark control table
        source_name: Data source name
        default_value: Default value when no record exists
    
    Returns:
        Watermark value (timestamp string or other)
    """
    try:
        watermark_df = spark.read.format("delta").load(watermark_table_path)
        
        result = watermark_df.filter(f"source_name = '{source_name}'") \
            .select("watermark_value") \
            .first()
        
        if result:
            return result["watermark_value"]
        else:
            logging.info(f"No watermark found for {source_name}, using default: {default_value}")
            return default_value
    
    except Exception as e:
        logging.warning(f"Failed to read watermark table: {e}. Using default: {default_value}")
        return default_value


def update_watermark(
    spark: SparkSession,
    watermark_table_path: str,
    source_name: str,
    new_watermark_value: str
) -> None:
    """
    Update watermark value using MERGE operation.
    
    Args:
        spark: SparkSession instance
        watermark_table_path: Path to watermark control table
        source_name: Data source name
        new_watermark_value: New watermark value
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    # Prepare update data
    update_df = spark.createDataFrame([
        (source_name, new_watermark_value)
    ], ["source_name", "watermark_value"]) \
        .withColumn("updated_at", current_timestamp())
    
    # Ensure target table exists
    try:
        delta_table = DeltaTable.forPath(spark, watermark_table_path)
        
        # MERGE update
        delta_table.alias("target").merge(
            update_df.alias("source"),
            "target.source_name = source.source_name"
        ).whenMatchedUpdate(set={
            "watermark_value": "source.watermark_value",
            "updated_at": "source.updated_at"
        }).whenNotMatchedInsert(values={
            "source_name": "source.source_name",
            "watermark_value": "source.watermark_value",
            "updated_at": "source.updated_at"
        }).execute()
        
    except Exception as e:
        logging.warning(f"Watermark table not found, creating new: {e}")
        # First time creation
        update_df.write.format("delta").mode("overwrite").save(watermark_table_path)


def reset_watermark(
    spark: SparkSession,
    watermark_table_path: str,
    source_name: str
) -> None:
    """
    Reset watermark for specified source (delete record).
    
    Args:
        spark: SparkSession instance
        watermark_table_path: Path to watermark control table
        source_name: Data source name
    """
    try:
        delta_table = DeltaTable.forPath(spark, watermark_table_path)
        delta_table.delete(f"source_name = '{source_name}'")
        logging.info(f"Watermark reset for {source_name}")
    except Exception as e:
        logging.error(f"Failed to reset watermark: {e}")


def get_incremental_filter(
    watermark_column: str,
    watermark_value: Optional[str]
) -> str:
    """
    Generate incremental read filter condition.
    
    Args:
        watermark_column: Watermark column name
        watermark_value: Current watermark value
    
    Returns:
        SQL filter condition string
    """
    if watermark_value is None:
        return "1=1"  # Full read
    else:
        return f"{watermark_column} > '{watermark_value}'"
