# Azure Fabric notebook source
"""
Bronze Layer: Ingest Agents CSV to Delta
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col
import logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        logger.info("Reading agents from CSV")
        df = spark.read.csv("Files/samples/batch/agents.csv", header=True, inferSchema=True)
        
        df_enriched = df.withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv"))
        
        record_count = df_enriched.count()
        logger.info(f"Read {record_count} agents")
        
        # Basic validation
        if "agent_id" not in df_enriched.columns:
            raise ValueError("Missing required column: agent_id")
        
        null_count = df_enriched.filter(col("agent_id").isNull()).count()
        if null_count > 0:
            logger.warning(f"Found {null_count} null agent_id values")
        
        logger.info("Writing to Tables/bronze_agents")
        df_enriched.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .option("mergeSchema", "true") \
            .save("Tables/bronze_agents")
        
        logger.info("✓ Bronze agents ingestion completed")
        
    except Exception as e:
        logger.error(f"✗ Agents ingestion failed: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
