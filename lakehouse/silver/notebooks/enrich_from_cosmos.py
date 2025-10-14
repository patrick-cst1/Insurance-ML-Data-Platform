# Databricks notebook source
"""
Silver Layer: Enrich policies with Cosmos DB data
"""

from pyspark.sql import SparkSession
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import read_delta, write_delta
from purview_integration import PurviewMetadata
from cosmos_io import enrich_dataframe
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

# Parameters (read from environment variables or Fabric parameters)
COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT", "")
COSMOS_KEY = os.environ.get("COSMOS_KEY", "")
COSMOS_DATABASE = os.environ.get("COSMOS_DATABASE", "insurance")
COSMOS_CONTAINER = os.environ.get("COSMOS_CONTAINER", "policy-enrichment")

SILVER_POLICIES_PATH = "Tables/silver_policies"
ENRICHED_OUTPUT_PATH = "Tables/silver_policies_enriched"

# COMMAND ----------

def main():
    logger = get_logger("silver_enrich_cosmos")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "enrich_from_cosmos"):
        
        # Read Silver policies
        df_silver = read_delta(spark, SILVER_POLICIES_PATH)
        logger.info(f"Read {df_silver.count()} Silver policies")
        
        # Enrich with Cosmos DB
        if COSMOS_ENDPOINT and COSMOS_KEY:
            df_enriched = enrich_dataframe(
                spark=spark,
                df=df_silver,
                cosmos_endpoint=COSMOS_ENDPOINT,
                cosmos_key=COSMOS_KEY,
                cosmos_database=COSMOS_DATABASE,
                cosmos_container=COSMOS_CONTAINER,
                join_keys=["policy_id"],
                select_columns=["risk_score", "underwriting_flags", "external_rating"],
                broadcast_cosmos=True
            )
            
            logger.info(f"Enriched {df_enriched.count()} policies with Cosmos data")
        else:
            logger.warning("Cosmos credentials not provided, skipping enrichment")
            df_enriched = df_silver
        
        # Write enriched data
        metadata = PurviewMetadata.get_silver_metadata("silver_policies_enriched", has_scd2=True, pii=False)
        write_delta(
            df=df_enriched,
            path=ENRICHED_OUTPUT_PATH,
            mode="overwrite",
            description=metadata["description"],
            tags=metadata["tags"]
        )
        
        logger.info("Cosmos enrichment completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
