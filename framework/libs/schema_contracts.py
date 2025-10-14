import yaml
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from typing import Dict, List, Optional, Any
import logging


TYPE_MAPPING = {
    "string": StringType(),
    "int": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "timestamp": TimestampType(),
    "boolean": BooleanType()
}


def load_contract(contract_path: str) -> Dict:
    """
    Load schema contract from YAML file.
    
    Args:
        contract_path: Path to contract YAML file
    
    Returns:
        Contract dictionary
    """
    with open(contract_path, 'r', encoding='utf-8') as f:
        contract = yaml.safe_load(f)
    
    return contract


def validate_contract(
    df: DataFrame,
    contract: Dict,
    raise_on_failure: bool = False
) -> Dict[str, Any]:
    """
    Validate DataFrame against contract.
    
    Args:
        df: DataFrame to validate
        contract: Schema contract dictionary
        raise_on_failure: Whether to raise exception on validation failure
    
    Returns:
        Validation result dictionary
    """
    expected_schema = contract.get("schema", {})
    required_columns = contract.get("required_columns", [])
    
    actual_fields = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    actual_column_names = set(actual_fields.keys())
    
    # Check required columns
    missing_required = [col for col in required_columns if col not in actual_column_names]
    
    # Check type matching
    type_mismatches = {}
    for col_name, expected_type in expected_schema.items():
        if col_name in actual_fields:
            actual_type = actual_fields[col_name]
            if actual_type != expected_type:
                type_mismatches[col_name] = {
                    "expected": expected_type,
                    "actual": actual_type
                }
    
    passed = len(missing_required) == 0 and len(type_mismatches) == 0
    
    result = {
        "passed": passed,
        "missing_required_columns": missing_required,
        "type_mismatches": type_mismatches
    }
    
    if not passed and raise_on_failure:
        raise ValueError(f"Contract validation failed: {result}")
    
    return result


def build_spark_schema(contract: Dict) -> StructType:
    """
    Build Spark StructType schema from contract.
    
    Args:
        contract: Schema contract dictionary
    
    Returns:
        StructType schema
    """
    schema_dict = contract.get("schema", {})
    required_columns = set(contract.get("required_columns", []))
    
    fields = []
    for col_name, col_type in schema_dict.items():
        spark_type = TYPE_MAPPING.get(col_type.lower(), StringType())
        nullable = col_name not in required_columns
        fields.append(StructField(col_name, spark_type, nullable))
    
    return StructType(fields)


def enforce_contract(
    df: DataFrame,
    contract: Dict,
    drop_extra_columns: bool = False
) -> DataFrame:
    """
    Enforce DataFrame to conform to contract (select columns, convert types).
    
    Args:
        df: Original DataFrame
        contract: Schema contract
        drop_extra_columns: Whether to drop extra columns
    
    Returns:
        DataFrame conforming to contract
    """
    schema_dict = contract.get("schema", {})
    
    # Select and convert columns
    select_cols = []
    for col_name, col_type in schema_dict.items():
        if col_name in df.columns:
            # Type conversion logic can be added here
            select_cols.append(col_name)
        else:
            logging.warning(f"Column {col_name} not found in DataFrame")
    
    result_df = df.select(*select_cols) if drop_extra_columns else df
    
    return result_df
