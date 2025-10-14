#Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when
import sys

sys.path.append("/Workspace/framework/libs")
from delta_ops import read_delta, write_delta
from data_quality import check_nulls, detect_duplicates
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

def main():
    logger = get_logger("silver_clean_claims")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "clean_claims"):
        df_bronze = read_delta(spark, "Tables/bronze_claims")
        logger.info(f"Read {df_bronze.count()} records from Bronze")
        
        # Data cleaning
        df_cleaned = df_bronze \
            .dropDuplicates(["claim_id"]) \
            .filter(col("claim_id").isNotNull()) \
            .withColumn("claim_status", upper(trim(col("claim_status")))) \
            .withColumn("claim_type", upper(trim(col("claim_type")))) \
            .withColumn("claim_amount", col("claim_amount").cast("double")) \
            .filter(col("claim_amount") > 0)
        
        # Data quality checks
        null_check = check_nulls(df_cleaned, columns=["claim_id", "policy_id", "claim_amount"], threshold=0.01)
        if not null_check["passed"]:
            logger.warning(f"Null check violations: {null_check['violations']}")
        
        logger.info(f"Writing {df_cleaned.count()} records to Silver")
        df_cleaned.write.format("delta").mode("overwrite").save("Tables/silver_claims")
        logger.info("Silver claims cleaning completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
