import pytest
from pyspark.sql import SparkSession
from framework.libs.delta_ops import read_delta, write_delta, merge_delta, optimize_delta


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    return SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def test_read_delta(spark, tmp_path):
    """Test reading Delta table."""
    # Create sample Delta table
    test_path = str(tmp_path / "test_delta")
    df = spark.createDataFrame([(1, "test")], ["id", "value"])
    write_delta(df, test_path, mode="overwrite")
    
    # Read and verify
    result = read_delta(spark, test_path)
    assert result.count() == 1
    assert result.first()["value"] == "test"


def test_write_delta_with_partition(spark, tmp_path):
    """Test writing Delta table with partitioning."""
    test_path = str(tmp_path / "test_partitioned")
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "category"])
    
    write_delta(df, test_path, mode="overwrite", partition_by=["category"])
    
    result = read_delta(spark, test_path)
    assert result.count() == 2


def test_merge_delta(spark, tmp_path):
    """Test Delta MERGE operation."""
    test_path = str(tmp_path / "test_merge")
    
    # Initial data
    initial_df = spark.createDataFrame([(1, "old"), (2, "keep")], ["id", "value"])
    write_delta(initial_df, test_path, mode="overwrite")
    
    # Update data
    update_df = spark.createDataFrame([(1, "new"), (3, "insert")], ["id", "value"])
    
    merge_delta(
        spark=spark,
        target_path=test_path,
        source_df=update_df,
        merge_condition="target.id = source.id",
        when_matched_update={"value": "source.value"},
        when_not_matched_insert={"id": "source.id", "value": "source.value"}
    )
    
    result = read_delta(spark, test_path)
    assert result.count() == 3
    assert result.filter("id = 1").first()["value"] == "new"
