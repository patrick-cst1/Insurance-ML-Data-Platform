# Azure Fabric notebook source
"""
Silver Layer: Clean and standardize customers
Schema-driven type casting from silver schema YAML
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, current_timestamp, lit
import logging
import yaml

# COMMAND ----------

BRONZE_PATH = "Tables/bronze_customers"
SILVER_PATH = "Tables/silver_customers"
SCHEMA_PATH = "/lakehouse/default/Files/config/schemas/silver/silver_customers.yaml"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def apply_schema_transformations(df, schema_path):
    """Apply type casting and transformations based on silver schema."""
    with open(schema_path, 'r') as f:
        schema = yaml.safe_load(f)
    
    for col_def in schema['business_columns']:
        col_name = col_def['name']
        col_type = col_def['type']
        transformation = col_def.get('transformation', None)
        
        # Apply transformation first
        if transformation == 'upper_trim':
            df = df.withColumn(col_name, upper(trim(col(col_name))))
        elif transformation == 'trim':
            df = df.withColumn(col_name, trim(col(col_name)))
        
        # Apply type casting
        if col_type == 'double':
            df = df.withColumn(col_name, col(col_name).cast("double"))
        elif col_type == 'integer':
            df = df.withColumn(col_name, col(col_name).cast("int"))
        elif col_type == 'date':
            df = df.withColumn(col_name, to_date(col(col_name)))
        
        # Apply nullable filter
        if not col_def['nullable']:
            df = df.filter(col(col_name).isNotNull())
        
        # Apply validation rules
        if 'validation' in col_def:
            for rule in col_def['validation']:
                if rule['rule'] == 'greater_than':
                    df = df.filter(col(col_name) > rule['value'])
                elif rule['rule'] == 'less_than':
                    df = df.filter(col(col_name) < rule['value'])
    
    return df

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        logger.info(f"Reading from {BRONZE_PATH}")
        df_bronze = spark.read.format("delta").load(BRONZE_PATH)
        
        record_count = df_bronze.count()
        logger.info(f"Read {record_count} records from Bronze")
        
        # Deduplication
        df_cleaned = df_bronze.dropDuplicates(["customer_id"])
        
        # Apply schema-driven type casting and validations
        logger.info(f"Applying silver schema transformations from {SCHEMA_PATH}")
        df_cleaned = apply_schema_transformations(df_cleaned, SCHEMA_PATH)
        
        # Add processing timestamp
        df_cleaned = df_cleaned.withColumn("processed_timestamp", current_timestamp())
        
        # Add SCD Type 2 columns
        df_cleaned = df_cleaned \
            .withColumn("effective_from", col("ingestion_timestamp")) \
            .withColumn("effective_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))
        
        cleaned_count = df_cleaned.count()
        dropped_count = record_count - cleaned_count
        pass_rate = (cleaned_count / record_count * 100) if record_count > 0 else 0
        
        logger.info(f"Data Quality Metrics:")
        logger.info(f"  - Total records: {record_count}")
        logger.info(f"  - Cleaned records: {cleaned_count}")
        logger.info(f"  - Dropped records: {dropped_count}")
        logger.info(f"  - Pass rate: {pass_rate:.2f}%")
        
        # Write to Silver with Purview metadata
        logger.info(f"Writing to {SILVER_PATH}")
        df_cleaned.write \
            .format("delta") \
            .mode("overwrite") \
            .option("description", "Silver layer: Cleaned customers with SCD Type 2 tracking (contains PII)") \
            .save(SILVER_PATH)
        
        logger.info("✓ Silver customers cleaning completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Failed to clean customers: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
