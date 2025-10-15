# Azure Fabric notebook source
"""
Bronze Layer: Ingest Policies CSV to Delta
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col, input_file_name
import logging
import yaml
import uuid

# COMMAND ----------

# Configuration
SOURCE_PATH = "Files/samples/batch/policies.csv"
TARGET_PATH = "Tables/bronze_policies"
PARTITION_COLUMN = "ingestion_date"
SCHEMA_PATH = "/lakehouse/default/Files/config/schemas/bronze/bronze_policies.yaml"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def validate_schema(df, schema_path):
    """Simplified inline schema validation."""
    try:
        with open(schema_path, 'r') as f:
            schema = yaml.safe_load(f)
        
        # Check required columns
        for col_def in schema['required_columns']:
            col_name = col_def['name']
            nullable = col_def['nullable']
            
            # Check column exists
            if col_name not in df.columns:
                raise ValueError(f"Missing required column: {col_name}")
            
            # Check nulls if not nullable
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
    """Ingest policies from CSV to Bronze Delta table."""
    
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read source CSV (all columns as strings - Medallion best practice)
        logger.info(f"Reading policies from {SOURCE_PATH}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load(SOURCE_PATH)
        
        # Generate unique process ID for this pipeline run
        process_id = str(uuid.uuid4())
        
        # Add metadata columns
        df_enriched = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", to_date(current_timestamp())) \
            .withColumn("source_system", lit("legacy_csv")) \
            .withColumn("process_id", lit(process_id)) \
            .withColumn("source_file_name", input_file_name())
        
        record_count = df_enriched.count()
        logger.info(f"Read {record_count} policies")
        
        # Schema validation
        validate_schema(df_enriched, SCHEMA_PATH)
        
        # Write to Bronze Delta (append mode)
        logger.info(f"Writing to {TARGET_PATH}")
        df_enriched.write \
            .format("delta") \
            .mode("append") \
            .partitionBy(PARTITION_COLUMN) \
            .option("mergeSchema", "true") \
            .save(TARGET_PATH)
        
        logger.info("✓ Bronze ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Bronze ingestion failed: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
