import pytest
from pyspark.sql import SparkSession
from framework.libs.data_quality import validate_schema, check_nulls, detect_duplicates


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    return SparkSession.builder \
        .appName("test_dq") \
        .master("local[*]") \
        .getOrCreate()


def test_validate_schema(spark):
    """Test schema validation."""
    df = spark.createDataFrame([(1, "test"), (2, "test2")], ["id", "value"])
    
    expected_schema = {"id": "int", "value": "string"}
    result = validate_schema(df, expected_schema, strict=True)
    
    assert result["passed"] == True
    assert len(result["missing_columns"]) == 0


def test_check_nulls(spark):
    """Test null checking."""
    df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])
    
    result = check_nulls(df, columns=["value"], threshold=0.5)
    
    assert result["passed"] == True
    assert result["null_ratios"]["value"] < 0.5


def test_detect_duplicates(spark):
    """Test duplicate detection."""
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["id", "value"])
    
    result = detect_duplicates(df, key_columns=["id"], threshold=0)
    
    assert result["passed"] == False
    assert result["duplicate_count"] == 1
