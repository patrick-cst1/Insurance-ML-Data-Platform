# Databricks notebook source
"""
Bronze Layer: Ingest Customers CSV to Delta
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

SCHEMA_CONTRACT_PATHS = [
    "/Workspace/framework/config/schema_contracts/bronze_customers.yaml",
    "framework/config/schema_contracts/bronze_customers.yaml"
]

# COMMAND ----------

def main():
    logger = get_logger("bronze_ingest_customers")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "ingest_customers"):
        df = spark.read.csv("Files/samples/batch/customers.csv", header=True, inferSchema=True)
        df_enriched = df.withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        logger.info(f"Read {df_enriched.count()} customers")
        
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
        
        write_delta(df_enriched, "Tables/bronze_customers", mode="append", partition_by=["ingestion_date"], merge_schema=True)
        logger.info("Bronze customers ingestion completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
