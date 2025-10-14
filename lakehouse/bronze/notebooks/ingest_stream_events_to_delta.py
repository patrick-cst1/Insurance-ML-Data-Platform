# Azure Fabric notebook source
"""
Bronze Layer: Ingest streaming events from Eventstream to Delta (dual-sink pattern)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import write_delta
from logging_utils import get_logger
from schema_contracts import load_contract, validate_contract

# COMMAND ----------

# Configuration
EVENTSTREAM_PATH = "Tables/eventstream_raw"  # Eventstream output location
BRONZE_DELTA_PATH = "Tables/bronze_realtime_events"
SCHEMA_CONTRACT_PATHS = [
    "/Workspace/framework/config/schema_contracts/bronze_realtime_events.yaml",
    "framework/config/schema_contracts/bronze_realtime_events.yaml"
]

# COMMAND ----------

def main():
    """
    Read from Eventstream landing zone and append to Bronze Delta for replay capability.
    This enables dual-sink: KQL for low-latency queries + Delta for batch/ML workflows.
    """
    logger = get_logger("bronze_stream_to_delta")
    spark = SparkSession.builder.getOrCreate()
    
    logger.info("Reading streaming events from Eventstream landing zone")
    
    # Read raw events (batch over micro-batches or streaming readStream)
    df_events = spark.read.format("delta").load(EVENTSTREAM_PATH)
    
    # Add metadata
    df_enriched = df_events \
        .withColumn("delta_ingestion_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", to_date(current_timestamp()))
    
    logger.info(f"Processing {df_enriched.count()} streaming events")
    
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
    
    # Append to Bronze Delta (immutable, append-only)
    write_delta(
        df=df_enriched,
        path=BRONZE_DELTA_PATH,
        mode="append",
        partition_by=["ingestion_date"]
    )
    
    logger.info("Streaming events written to Bronze Delta for replay capability")

# COMMAND ----------

if __name__ == "__main__":
    main()
