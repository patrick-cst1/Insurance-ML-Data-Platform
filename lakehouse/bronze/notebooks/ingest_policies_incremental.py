# Azure Fabric notebook source
"""
Bronze Layer: Incremental Ingest Policies CSV to Delta using Watermarking
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col
import sys
import os

# Add framework libs to path
sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))

from delta_ops import write_delta
from watermarking import get_current_watermark, update_watermark, get_incremental_filter
from logging_utils import get_logger, PipelineTimer
from schema_contracts import load_contract, validate_contract

# COMMAND ----------

# Configuration
SOURCE_PATH = "Files/samples/batch/policies.csv"
TARGET_PATH = "Tables/bronze_policies"
WATERMARK_TABLE_PATH = "Tables/watermark_control"
SOURCE_NAME = "bronze_policies"
WATERMARK_COLUMN = "last_modified_date"  # Assuming source has this column
PARTITION_COLUMN = "ingestion_date"
SCHEMA_CONTRACT_PATHS = [
    "/Workspace/framework/config/schema_contracts/bronze_policies.yaml",
    "framework/config/schema_contracts/bronze_policies.yaml"
]

# COMMAND ----------

def main():
    """Incrementally ingest policies from CSV to Bronze Delta table."""
    
    logger = get_logger("bronze_ingest_policies_incremental")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "ingest_policies_incremental"):
        
        try:
            # Get current watermark
            current_watermark = get_current_watermark(
                spark=spark,
                watermark_table_path=WATERMARK_TABLE_PATH,
                source_name=SOURCE_NAME,
                default_value="1900-01-01T00:00:00"
            )
            
            logger.info(f"Current watermark for {SOURCE_NAME}: {current_watermark}")
            
            # Read source CSV
            logger.info(f"Reading policies from {SOURCE_PATH}")
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(SOURCE_PATH)
            
            # Apply incremental filter
            # Note: If source CSV doesn't have timestamp, this will do full load
            if WATERMARK_COLUMN in df.columns:
                filter_condition = get_incremental_filter(WATERMARK_COLUMN, current_watermark)
                df_incremental = df.filter(filter_condition)
                logger.info(f"Applying incremental filter: {filter_condition}")
            else:
                logger.warning(f"Column {WATERMARK_COLUMN} not found, performing full load")
                df_incremental = df
            
            # Add metadata columns
            df_enriched = df_incremental \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("ingestion_date", to_date(current_timestamp())) \
                .withColumn("source_system", lit("legacy_csv"))
            
            record_count = df_enriched.count()
            logger.info(f"Read {record_count} policies (incremental)")
            
            if record_count > 0:
                # Validate schema against contract
                contract = None
                for contract_path in SCHEMA_CONTRACT_PATHS:
                    try:
                        contract = load_contract(contract_path)
                        logger.info(f"Loaded schema contract from {contract_path}")
                        break
                    except Exception:
                        continue
                
                if contract:
                    validation_result = validate_contract(df_enriched, contract, raise_on_failure=False)
                    if validation_result["passed"]:
                        logger.info("✓ Schema validation PASSED")
                    else:
                        logger.warning(f"✗ Schema validation FAILED: {validation_result}")
                else:
                    logger.warning("Schema contract not found, skipping validation")
                
                # Write to Bronze Delta (append mode)
                logger.info(f"Writing to {TARGET_PATH}")
                write_delta(
                    df=df_enriched,
                    path=TARGET_PATH,
                    mode="append",
                    partition_by=[PARTITION_COLUMN],
                    merge_schema=True
                )
                
                # Update watermark to current timestamp
                new_watermark = df_enriched.agg({"ingestion_timestamp": "max"}).first()[0]
                update_watermark(
                    spark=spark,
                    watermark_table_path=WATERMARK_TABLE_PATH,
                    source_name=SOURCE_NAME,
                    new_watermark_value=str(new_watermark)
                )
                
                logger.info(f"Updated watermark to: {new_watermark}")
                logger.info("Bronze incremental ingestion completed successfully")
            else:
                logger.info("No new records to ingest")
                
        except Exception as e:
            logger.error(f"Failed to ingest policies: {str(e)}")
            raise

# COMMAND ----------

if __name__ == "__main__":
    main()
