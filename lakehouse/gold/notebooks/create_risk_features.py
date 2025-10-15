# Azure Fabric notebook source
"""Gold Layer: Create risk assessment features
Simplified version - no framework dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit, current_timestamp
import logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def main():
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Read Silver policies and claims features
        logger.info("Reading policies and claims features")
        df_policies = spark.read.format("delta").load("Tables/silver_policies")
        df_claims_features = spark.read.format("delta").load("Tables/gold_claims_features")
        
        logger.info(f"Read {df_policies.count()} policies from Silver layer")
        
        # Join claims features with policies
        risk_features = df_policies.join(df_claims_features, on="customer_id", how="left")
        
        # Calculate simple risk score based on claims history (0-100 scale)
        risk_features = risk_features \
            .withColumn("claims_risk_component", 
                       (coalesce(col("total_claims"), lit(0)) * 10).cast("double")) \
            .withColumn("amount_risk_component",
                       when(col("total_claim_amount") > 50000, 40)
                       .when(col("total_claim_amount") > 10000, 20)
                       .otherwise(0).cast("double")) \
            .withColumn("overall_risk_score", 
                       when((lit(30) + 
                            col("claims_risk_component") + 
                            col("amount_risk_component")) > 100, 100)
                       .otherwise(lit(30) + 
                                 col("claims_risk_component") + 
                                 col("amount_risk_component"))
                       .cast("double")) \
            .withColumn("feature_timestamp", current_timestamp()) \
            .drop("claims_risk_component", "amount_risk_component")
        
        feature_count = risk_features.count()
        logger.info(f"Created risk features for {feature_count} policies")
        
        # Write to Gold with Purview metadata
        logger.info("Writing to Tables/gold_risk_features")
        risk_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("description", "Gold layer: Risk assessment features for ML") \
            .save("Tables/gold_risk_features")
        
        logger.info("✓ Risk features creation completed")
        
    except Exception as e:
        logger.error(f"✗ Failed to create risk features: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
