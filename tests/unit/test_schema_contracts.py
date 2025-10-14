import pytest
from pyspark.sql import SparkSession
from framework.libs.schema_contracts import (
    load_contract,
    validate_contract,
    build_spark_schema,
    enforce_contract
)
import yaml
import tempfile
import os


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    return SparkSession.builder \
        .appName("test_schema_contracts") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_contract():
    """Sample schema contract for testing."""
    return {
        "name": "test_table",
        "schema": {
            "id": "int",
            "name": "string",
            "amount": "double"
        },
        "required_columns": ["id", "name"]
    }


@pytest.fixture
def contract_file(sample_contract):
    """Create temporary contract file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(sample_contract, f)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)


def test_load_contract(contract_file, sample_contract):
    """Test loading contract from YAML file."""
    loaded = load_contract(contract_file)
    assert loaded["name"] == sample_contract["name"]
    assert loaded["schema"] == sample_contract["schema"]


def test_validate_contract_success(spark, sample_contract):
    """Test successful contract validation."""
    df = spark.createDataFrame([
        (1, "Alice", 100.0),
        (2, "Bob", 200.0)
    ], ["id", "name", "amount"])
    
    result = validate_contract(df, sample_contract)
    assert result["passed"] == True
    assert len(result["missing_required_columns"]) == 0


def test_validate_contract_missing_column(spark, sample_contract):
    """Test contract validation with missing required column."""
    df = spark.createDataFrame([
        (1, 100.0),
        (2, 200.0)
    ], ["id", "amount"])
    
    result = validate_contract(df, sample_contract)
    assert result["passed"] == False
    assert "name" in result["missing_required_columns"]


def test_build_spark_schema(sample_contract):
    """Test building Spark schema from contract."""
    schema = build_spark_schema(sample_contract)
    
    assert len(schema.fields) == 3
    field_names = [f.name for f in schema.fields]
    assert "id" in field_names
    assert "name" in field_names
    assert "amount" in field_names


def test_enforce_contract(spark, sample_contract):
    """Test enforcing contract on DataFrame."""
    df = spark.createDataFrame([
        (1, "Alice", 100.0, "extra_data"),
        (2, "Bob", 200.0, "extra_data2")
    ], ["id", "name", "amount", "extra_column"])
    
    enforced_df = enforce_contract(df, sample_contract, drop_extra_columns=True)
    
    assert "id" in enforced_df.columns
    assert "name" in enforced_df.columns
    assert "amount" in enforced_df.columns
    assert "extra_column" not in enforced_df.columns
