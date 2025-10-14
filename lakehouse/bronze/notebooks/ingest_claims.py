# Databricks notebook source
"""
Bronze Layer: Ingest Claims CSV to Delta
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import write_delta
from logging_utils import get_logger, PipelineTimer
from schema_contracts import load_contract, validate_contract

# COMMAND ----------

SOURCE_PATH = "Files/samples/batch/claims.csv"
TARGET_PATH = "Tables/bronze_claims"
PARTITION_COLUMN = "ingestion_date"
SCHEMA_CONTRACT_PATHS = [
    "/Workspace/framework/config/schema_contracts/bronze_claims.yaml",
    "framework/config/schema_contracts/bronze_claims.yaml"
]

# COMMAND ----------

def main():
    """Ingest claims from CSV to Bronze Delta table."""
    
    logger = get_logger("bronze_ingest_claims")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "ingest_claims"):
        
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(SOURCE_PATH)
        
        df_enriched = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        logger.info(f"Read {df_enriched.count()} claims")
        
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
        
        write_delta(
            df=df_enriched,
            path=TARGET_PATH,
            mode="append",
            partition_by=[PARTITION_COLUMN],
            merge_schema=True
        )
        
        logger.info("Bronze claims ingestion completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
