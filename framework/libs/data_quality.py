from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when, max as spark_max, current_timestamp, datediff
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import logging


def validate_schema(
    df: DataFrame,
    expected_schema: Dict[str, str],
    strict: bool = True
) -> Dict[str, Any]:
    """
    Validate DataFrame schema against expected schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected schema (column name -> data type)
        strict: Strict mode (requires exact match when True)
    
    Returns:
        Validation result dict (passed, missing_columns, extra_columns, type_mismatches)
    """
    actual_fields = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    expected_fields = set(expected_schema.keys())
    actual_field_names = set(actual_fields.keys())
    
    missing_columns = expected_fields - actual_field_names
    extra_columns = actual_field_names - expected_fields if strict else set()
    
    type_mismatches = {}
    for col_name in expected_fields.intersection(actual_field_names):
        if actual_fields[col_name] != expected_schema[col_name]:
            type_mismatches[col_name] = {
                "expected": expected_schema[col_name],
                "actual": actual_fields[col_name]
            }
    
    passed = len(missing_columns) == 0 and len(extra_columns) == 0 and len(type_mismatches) == 0
    
    return {
        "passed": passed,
        "missing_columns": list(missing_columns),
        "extra_columns": list(extra_columns),
        "type_mismatches": type_mismatches
    }


def check_nulls(
    df: DataFrame,
    columns: Optional[List[str]] = None,
    threshold: float = 0.0
) -> Dict[str, Any]:
    """
    Check null value ratios.
    
    Args:
        df: DataFrame to check
        columns: Columns to check (None = all columns)
        threshold: Acceptable null ratio threshold (0-1)
    
    Returns:
        Check result dict (passed, null_counts, null_ratios)
    """
    check_cols = columns if columns else df.columns
    total_count = df.count()
    
    null_counts = {}
    null_ratios = {}
    
    for col_name in check_cols:
        # Check data type to avoid isnan() on non-numeric columns
        col_type = dict(df.dtypes)[col_name] if col_name in dict(df.dtypes) else None
        
        if col_type in ['double', 'float', 'decimal']:
            null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        else:
            null_count = df.filter(col(col_name).isNull()).count()
        
        null_ratio = null_count / total_count if total_count > 0 else 0.0
        
        null_counts[col_name] = null_count
        null_ratios[col_name] = null_ratio
    
    violations = {k: v for k, v in null_ratios.items() if v > threshold}
    passed = len(violations) == 0
    
    return {
        "passed": passed,
        "null_counts": null_counts,
        "null_ratios": null_ratios,
        "violations": violations
    }


def detect_duplicates(
    df: DataFrame,
    key_columns: List[str],
    threshold: int = 0
) -> Dict[str, Any]:
    """
    Detect duplicate records.
    
    Args:
        df: DataFrame to check
        key_columns: Columns used for uniqueness check
        threshold: Acceptable duplicate count threshold
    
    Returns:
        Check result dict (passed, duplicate_count, duplicate_keys)
    """
    duplicate_df = df.groupBy(*key_columns).count().filter(col("count") > 1)
    duplicate_count = duplicate_df.count()
    
    duplicate_keys = []
    if duplicate_count > 0 and duplicate_count <= 100:  # Limit returned rows
        duplicate_keys = [row.asDict() for row in duplicate_df.limit(100).collect()]
    
    passed = duplicate_count <= threshold
    
    return {
        "passed": passed,
        "duplicate_count": duplicate_count,
        "duplicate_keys": duplicate_keys
    }


def check_freshness(
    df: DataFrame,
    timestamp_column: str,
    max_age_hours: int = 24
) -> Dict[str, Any]:
    """
    Check data freshness (time since latest record).
    
    Args:
        df: DataFrame to check
        timestamp_column: Timestamp column name
        max_age_hours: Maximum acceptable age in hours
    
    Returns:
        Check result dict (passed, latest_timestamp, age_hours)
    """
    latest_ts = df.select(spark_max(col(timestamp_column)).alias("max_ts")).first()["max_ts"]

    if latest_ts is None:
        return {
            "passed": False,
            "latest_timestamp": None,
            "age_hours": None,
            "error": "No data or null timestamp"
        }

    # Calculate age based on latest timestamp
    try:
        now_utc = datetime.now(timezone.utc)
        # latest_ts is a Python datetime (timezone-aware in most Spark configs)
        age_seconds = (now_utc - latest_ts.replace(tzinfo=timezone.utc)).total_seconds()
        age_hours = age_seconds / 3600.0
    except Exception:
        # Fallback using Spark SQL if timezone handling differs
        age_hours = df.selectExpr(
            f"(unix_timestamp(current_timestamp()) - unix_timestamp(max({timestamp_column}))) / 3600.0 as age_hours"
        ).first()["age_hours"]

    passed = age_hours <= max_age_hours

    return {
        "passed": passed,
        "latest_timestamp": str(latest_ts),
        "age_hours": age_hours
    }


def check_value_range(
    df: DataFrame,
    checks: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Check if column values are within specified ranges.
    
    Args:
        df: DataFrame to check
        checks: List of range checks, each with keys: column, min, max
                Example: [{"column": "amount", "min": 0, "max": 1000000}]
    
    Returns:
        Check result dict (passed, violations)
    """
    violations = []
    total_count = df.count()
    
    for check in checks:
        column = check["column"]
        min_val = check.get("min")
        max_val = check.get("max")
        
        if column not in df.columns:
            violations.append({
                "column": column,
                "error": "Column not found in DataFrame"
            })
            continue
        
        # Build range condition
        conditions = []
        if min_val is not None:
            conditions.append(col(column) < min_val)
        if max_val is not None:
            conditions.append(col(column) > max_val)
        
        if conditions:
            # Count violations (values outside range or NULL)
            violation_condition = conditions[0]
            for cond in conditions[1:]:
                violation_condition = violation_condition | cond
            
            violation_count = df.filter(violation_condition).count()
            violation_ratio = violation_count / total_count if total_count > 0 else 0.0
            
            if violation_count > 0:
                violations.append({
                    "column": column,
                    "min": min_val,
                    "max": max_val,
                    "violation_count": violation_count,
                    "violation_ratio": violation_ratio
                })
    
    passed = len(violations) == 0
    
    return {
        "passed": passed,
        "violations": violations,
        "total_checks": len(checks)
    }


def check_completeness(
    df: DataFrame,
    required_columns: List[str],
    min_completeness_ratio: float = 0.95
) -> Dict[str, Any]:
    """
    Check feature completeness (non-null ratio for required columns).
    
    Args:
        df: DataFrame to check
        required_columns: List of required column names
        min_completeness_ratio: Minimum acceptable completeness ratio (0-1)
    
    Returns:
        Check result dict (passed, completeness_ratios, incomplete_columns)
    """
    total_count = df.count()
    completeness_ratios = {}
    incomplete_columns = []
    
    for col_name in required_columns:
        if col_name not in df.columns:
            incomplete_columns.append({
                "column": col_name,
                "error": "Column not found",
                "completeness_ratio": 0.0
            })
            continue
        
        # Count non-null values
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        completeness_ratio = non_null_count / total_count if total_count > 0 else 0.0
        
        completeness_ratios[col_name] = completeness_ratio
        
        if completeness_ratio < min_completeness_ratio:
            incomplete_columns.append({
                "column": col_name,
                "completeness_ratio": completeness_ratio,
                "required_ratio": min_completeness_ratio
            })
    
    passed = len(incomplete_columns) == 0
    
    return {
        "passed": passed,
        "completeness_ratios": completeness_ratios,
        "incomplete_columns": incomplete_columns,
        "min_required_ratio": min_completeness_ratio
    }
