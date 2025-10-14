import pytest
from pyspark.sql import SparkSession
from framework.libs.watermarking import (
    get_current_watermark,
    update_watermark,
    reset_watermark,
    get_incremental_filter
)


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    return SparkSession.builder \
        .appName("test_watermarking") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@pytest.fixture
def watermark_table_path(tmp_path):
    """Temporary watermark table path."""
    return str(tmp_path / "watermark_control")


def test_get_incremental_filter():
    """Test incremental filter generation."""
    # With watermark
    filter_str = get_incremental_filter("timestamp_col", "2025-01-01T00:00:00")
    assert filter_str == "timestamp_col > '2025-01-01T00:00:00'"
    
    # Without watermark (full load)
    filter_str = get_incremental_filter("timestamp_col", None)
    assert filter_str == "1=1"


def test_update_and_get_watermark(spark, watermark_table_path):
    """Test watermark update and retrieval."""
    source_name = "test_source"
    watermark_value = "2025-01-01T00:00:00"
    
    # Update watermark
    update_watermark(spark, watermark_table_path, source_name, watermark_value)
    
    # Retrieve watermark
    retrieved = get_current_watermark(spark, watermark_table_path, source_name)
    assert retrieved == watermark_value


def test_get_watermark_default(spark, watermark_table_path):
    """Test watermark retrieval with default value."""
    default_value = "1900-01-01T00:00:00"
    retrieved = get_current_watermark(
        spark, 
        watermark_table_path, 
        "non_existent_source", 
        default_value
    )
    assert retrieved == default_value


def test_reset_watermark(spark, watermark_table_path):
    """Test watermark reset."""
    source_name = "test_source"
    
    # Create watermark
    update_watermark(spark, watermark_table_path, source_name, "2025-01-01T00:00:00")
    
    # Reset watermark
    reset_watermark(spark, watermark_table_path, source_name)
    
    # Verify deleted
    retrieved = get_current_watermark(spark, watermark_table_path, source_name, "default")
    assert retrieved == "default"
