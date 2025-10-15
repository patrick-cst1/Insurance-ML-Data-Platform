# Azure Fabric notebook source
"""
Bronze Layer: Ingest Agents CSV to Delta
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col, input_file_name
import logging
import yaml
import uuid

# COMMAND ----------

SCHEMA_PATH = "/lakehouse/default/Files/config/schemas/bronze/bronze_agents.yaml"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def validate_schema(df, schema_path):
    """Simplified inline schema validation."""
    try:
        with open(schema_path, 'r') as f:
            schema = yaml.safe_load(f)
        
        for col_def in schema['required_columns']:
            col_name = col_def['name']
            nullable = col_def['nullable']
            
            if col_name not in df.columns:
                raise ValueError(f"Missing required column: {col_name}")
            
            if not nullable:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in non-nullable column: {col_name}")
        
        logger.info("✓ Schema validation passed")
        return True
    except FileNotFoundError:
        logger.warning(f"Schema file not found: {schema_path}, skipping validation")
        return True
    except Exception as e:
        logger.error(f"Schema validation failed: {str(e)}")
        raise

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        logger.info("Reading agents from CSV")
        # Read all columns as strings to preserve raw data (Medallion best practice)
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load("Files/samples/batch/agents.csv")
        
        # Generate unique process ID for this pipeline run
        process_id = str(uuid.uuid4())
        
        df_enriched = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv")) \
            .withColumn("process_id", lit(process_id)) \
            .withColumn("source_file_name", input_file_name())
        
        record_count = df_enriched.count()
        logger.info(f"Read {record_count} agents")
        
        # Schema validation
        validate_schema(df_enriched, SCHEMA_PATH)
        
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
