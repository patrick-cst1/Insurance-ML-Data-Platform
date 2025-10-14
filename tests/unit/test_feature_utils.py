import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from framework.libs.feature_utils import (
    build_aggregation_features,
    add_feature_metadata,
    create_scd2_features
)
from datetime import datetime, timedelta


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    return SparkSession.builder \
        .appName("test_feature_utils") \
        .master("local[*]") \
        .getOrCreate()


def test_add_feature_metadata(spark):
    """Test adding feature metadata columns."""
    df = spark.createDataFrame([
        (1, "feature1"),
        (2, "feature2")
    ], ["id", "value"])
    
    df_with_metadata = add_feature_metadata(df)
    
    assert "feature_timestamp" in df_with_metadata.columns
    assert "load_timestamp" in df_with_metadata.columns
    assert df_with_metadata.count() == 2


def test_create_scd2_features(spark):
    """Test creating SCD Type 2 columns."""
    # Create sample dimension data
    df = spark.createDataFrame([
        (1, "Product A", 100.0, "2025-01-01"),
        (1, "Product A", 120.0, "2025-01-15"),  # Price change
        (2, "Product B", 200.0, "2025-01-01")
    ], ["product_id", "name", "price", "timestamp"])
    
    df_scd2 = create_scd2_features(
        df,
        entity_keys=["product_id"],
        timestamp_column="timestamp"
    )
    
    # Check SCD2 columns exist
    assert "effective_from" in df_scd2.columns
    assert "effective_to" in df_scd2.columns
    assert "is_current" in df_scd2.columns
    
    # Check current records
    current_records = df_scd2.filter(col("is_current") == True).count()
    assert current_records == 2  # One current record per product


def test_build_aggregation_features(spark):
    """Test building time-window aggregation features."""
    # Create sample event data
    current_date = datetime.now()
    df = spark.createDataFrame([
        (1, 100.0, (current_date - timedelta(days=5)).strftime("%Y-%m-%d")),
        (1, 150.0, (current_date - timedelta(days=10)).strftime("%Y-%m-%d")),
        (1, 200.0, (current_date - timedelta(days=60)).strftime("%Y-%m-%d")),
        (2, 300.0, (current_date - timedelta(days=3)).strftime("%Y-%m-%d"))
    ], ["customer_id", "amount", "event_date"])
    
    features = build_aggregation_features(
        df=df,
        entity_keys=["customer_id"],
        timestamp_column="event_date",
        aggregation_windows=[30, 90],
        agg_columns={"amount": ["sum", "count"]}
    )
    
    # Check feature columns exist
    assert "amount_sum_30d" in features.columns
    assert "amount_count_30d" in features.columns
    assert "amount_sum_90d" in features.columns
    assert "amount_count_90d" in features.columns
    
    # Verify customer count
    assert features.count() == 2
