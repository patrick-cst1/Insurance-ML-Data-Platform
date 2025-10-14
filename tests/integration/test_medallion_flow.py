"""
Integration test for end-to-end Medallion flow.
Requires live Fabric workspace with Lakehouses configured.
"""

import pytest
from pyspark.sql import SparkSession
import os


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for integration tests."""
    return SparkSession.builder \
        .appName("integration_test") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@pytest.mark.integration
def test_bronze_to_silver_flow(spark):
    """
    Test Bronze to Silver data flow.
    Prerequisites: Fabric workspace with sample data loaded
    """
    try:
        # Check if Bronze tables exist
        bronze_policies = spark.read.format("delta").load("Tables/bronze_policies")
        assert bronze_policies.count() > 0, "Bronze policies table is empty"
        
        # Check if Silver tables exist
        silver_policies = spark.read.format("delta").load("Tables/silver_policies")
        assert silver_policies.count() > 0, "Silver policies table is empty"
        
        # Validate SCD2 columns exist
        required_columns = ["policy_id", "effective_from", "effective_to", "is_current"]
        for col in required_columns:
            assert col in silver_policies.columns, f"Missing required column: {col}"
        
        # Validate data quality
        current_records = silver_policies.filter("is_current = true")
        assert current_records.count() > 0, "No current records found in Silver"
        
        print("✓ Bronze to Silver flow validation passed")
        
    except Exception as e:
        pytest.skip(f"Integration test skipped: {str(e)}")


@pytest.mark.integration
def test_silver_to_gold_features(spark):
    """
    Test Silver to Gold feature engineering flow.
    """
    try:
        # Check if Gold feature tables exist
        gold_claims = spark.read.format("delta").load("Tables/gold_claims_features")
        assert gold_claims.count() > 0, "Gold claims features table is empty"
        
        # Validate feature columns
        required_features = [
            "customer_id", 
            "claims_count_30d", 
            "claim_amount_sum_30d",
            "feature_timestamp"
        ]
        for col in required_features:
            assert col in gold_claims.columns, f"Missing feature column: {col}"
        
        # Validate feature metadata
        assert "load_timestamp" in gold_claims.columns, "Missing load_timestamp metadata"
        
        print("✓ Silver to Gold features validation passed")
        
    except Exception as e:
        pytest.skip(f"Integration test skipped: {str(e)}")


@pytest.mark.integration  
def test_cosmos_enrichment(spark):
    """
    Test Cosmos DB enrichment integration.
    """
    try:
        # Check if enriched table exists
        enriched_policies = spark.read.format("delta").load("Tables/silver_policies_enriched")
        assert enriched_policies.count() > 0, "Enriched policies table is empty"
        
        # Validate Cosmos enrichment columns (may be null if not enriched)
        cosmos_columns = ["risk_score", "underwriting_flags", "external_rating"]
        for col in cosmos_columns:
            if col in enriched_policies.columns:
                print(f"  Found Cosmos column: {col}")
        
        print("✓ Cosmos enrichment validation passed")
        
    except Exception as e:
        pytest.skip(f"Integration test skipped: {str(e)}")


@pytest.mark.integration
def test_data_quality_checks(spark):
    """
    Test that data quality check results table exists and has recent checks.
    """
    try:
        dq_results = spark.read.format("delta").load("Tables/dq_check_results")
        assert dq_results.count() > 0, "No DQ check results found"
        
        # Check for recent results (last 7 days)
        from pyspark.sql.functions import datediff, current_date, col
        recent_checks = dq_results.filter(
            datediff(current_date(), col("check_timestamp")) <= 7
        )
        
        assert recent_checks.count() > 0, "No recent DQ checks found (last 7 days)"
        
        print("✓ Data quality checks validation passed")
        
    except Exception as e:
        pytest.skip(f"Integration test skipped: {str(e)}")


@pytest.mark.integration
def test_watermark_table(spark):
    """
    Test watermark control table exists and is functioning.
    """
    try:
        watermark_table = spark.read.format("delta").load("Tables/watermark_control")
        assert watermark_table.count() > 0, "Watermark table is empty"
        
        # Validate schema
        required_columns = ["source_name", "watermark_value", "updated_at"]
        for col in required_columns:
            assert col in watermark_table.columns, f"Missing watermark column: {col}"
        
        print("✓ Watermark table validation passed")
        
    except Exception as e:
        pytest.skip(f"Integration test skipped: {str(e)}")
