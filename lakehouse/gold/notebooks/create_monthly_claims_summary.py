# Azure Fabric notebook source
"""
Gold Layer: Create monthly claims summary for time-series analysis
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    year, month, date_trunc, current_timestamp
)
import logging

# COMMAND ----------

SILVER_CLAIMS_PATH = "Tables/silver_claims"
GOLD_MONTHLY_PATH = "Tables/gold_monthly_claims_summary"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read Silver claims
        logger.info(f"Reading from {SILVER_CLAIMS_PATH}")
        df_claims = spark.read.format("delta").load(SILVER_CLAIMS_PATH)
        
        record_count = df_claims.count()
        logger.info(f"Read {record_count} claims from Silver")
        
        # Filter only current records (SCD Type 2)
        df_current = df_claims.filter(col("is_current") == True)
        
        # Add year-month columns for aggregation
        df_with_period = df_current \
            .withColumn("claim_year", year(col("claim_date"))) \
            .withColumn("claim_month", month(col("claim_date"))) \
            .withColumn("claim_year_month", date_trunc("month", col("claim_date")))
        
        # Monthly aggregations
        monthly_summary = df_with_period.groupBy("claim_year_month", "claim_year", "claim_month").agg(
            count("claim_id").alias("total_claims"),
            spark_sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            spark_max("claim_amount").alias("max_claim_amount"),
            spark_min("claim_amount").alias("min_claim_amount"),
            count(col("claim_id")).alias("claim_count")
        )
        
        # Add additional business metrics
        monthly_summary = monthly_summary \
            .withColumn("feature_timestamp", current_timestamp()) \
            .orderBy("claim_year_month")
        
        summary_count = monthly_summary.count()
        logger.info(f"Created monthly summary for {summary_count} time periods")
        
        # Show sample results
        logger.info("Sample monthly summary:")
        monthly_summary.show(5, truncate=False)
        
        # Write to Gold with Purview metadata
        logger.info(f"Writing to {GOLD_MONTHLY_PATH}")
        monthly_summary.write \
            .format("delta") \
            .mode("overwrite") \
            .option("description", "Gold layer: Monthly claims summary for time-series analysis") \
            .save(GOLD_MONTHLY_PATH)
        
        logger.info("✓ Monthly claims summary creation completed")
        
    except Exception as e:
        logger.error(f"✗ Failed to create monthly claims summary: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
