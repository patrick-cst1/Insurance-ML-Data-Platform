# Databricks notebook source
"""Gold Layer: Create risk assessment features"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce
import sys

sys.path.append("/Workspace/framework/libs")
from delta_ops import read_delta, write_delta
from feature_utils import add_feature_metadata
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

def main():
    logger = get_logger("gold_risk_features")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "create_risk_features"):
        
        # Read enriched policies (with Cosmos risk scores) and claims features
        df_policies = read_delta(spark, "Tables/silver_policies_enriched").filter(col("is_current") == True)
        df_claims_features = read_delta(spark, "Tables/gold_claims_features")
        
        # Join claims features with policies
        risk_features = df_policies.join(df_claims_features, on="customer_id", how="left")
        
        # Calculate overall risk score (combining Cosmos score + claims history)
        # Normalized to 0-100 scale
        risk_features = risk_features \
            .withColumn("claims_risk_component", 
                       (coalesce(col("claims_count_365d"), 0) * 2).cast("double")) \
            .withColumn("amount_risk_component",
                       when(col("claim_amount_sum_365d") > 50000, 30)
                       .when(col("claim_amount_sum_365d") > 10000, 15)
                       .otherwise(0).cast("double")) \
            .withColumn("overall_risk_score", 
                       when((coalesce(col("risk_score"), 50) + 
                            col("claims_risk_component") + 
                            col("amount_risk_component")) > 100, 100)
                       .otherwise(coalesce(col("risk_score"), 50) + 
                                 col("claims_risk_component") + 
                                 col("amount_risk_component"))
                       .cast("double")) \
            .withColumn("high_value_claim_ratio", 
                       when(col("claims_count_365d") > 0, 
                            col("claim_amount_max_365d") / col("claim_amount_sum_365d")).otherwise(0.0)) \
            .drop("claims_risk_component", "amount_risk_component")
        
        risk_features = add_feature_metadata(risk_features)
        
        logger.info(f"Created risk features for {risk_features.count()} policies")
        
        write_delta(risk_features, "Tables/gold_risk_features", mode="overwrite")
        logger.info("Risk features creation completed")

# COMMAND ----------

if __name__ == "__main__":
    main()
