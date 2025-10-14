from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lit, expr, datediff, count, sum as spark_sum, avg, max as spark_max, min as spark_min, when, row_number
from typing import List, Dict, Optional
import logging


def build_aggregation_features(
    df: DataFrame,
    entity_keys: List[str],
    timestamp_column: str,
    aggregation_windows: List[int],
    agg_columns: Dict[str, List[str]]
) -> DataFrame:
    """
    Build time-window aggregation features (e.g., 30/90/365 days).
    
    Args:
        df: Source event DataFrame
        entity_keys: Entity key list (e.g., customer_id)
        timestamp_column: Timestamp column name
        aggregation_windows: Window list (in days)
        agg_columns: Aggregation column and function mapping (e.g., {"claim_amount": ["sum", "avg", "count"]})
    
    Returns:
        Feature DataFrame
    """
    from pyspark.sql.functions import current_date
    
    feature_dfs = []
    
    for window_days in aggregation_windows:
        # Filter data within time window
        windowed_df = df.filter(
            expr(f"datediff(current_date(), {timestamp_column}) <= {window_days}")
        )
        
        # Build aggregation expressions
        agg_exprs = []
        for col_name, funcs in agg_columns.items():
            for func in funcs:
                if func == "count":
                    agg_exprs.append(count(col_name).alias(f"{col_name}_{func}_{window_days}d"))
                elif func == "sum":
                    agg_exprs.append(spark_sum(col_name).alias(f"{col_name}_{func}_{window_days}d"))
                elif func == "avg":
                    agg_exprs.append(avg(col_name).alias(f"{col_name}_{func}_{window_days}d"))
                elif func == "max":
                    agg_exprs.append(spark_max(col_name).alias(f"{col_name}_{func}_{window_days}d"))
                elif func == "min":
                    agg_exprs.append(spark_min(col_name).alias(f"{col_name}_{func}_{window_days}d"))
        
        agg_df = windowed_df.groupBy(*entity_keys).agg(*agg_exprs)
        feature_dfs.append(agg_df)
    
    # Join all window features
    result_df = feature_dfs[0]
    for i in range(1, len(feature_dfs)):
        result_df = result_df.join(feature_dfs[i], on=entity_keys, how="outer")
    
    return result_df


def generate_point_in_time_view(
    feature_df: DataFrame,
    entity_keys: List[str],
    event_time_column: str,
    valid_from_column: str = "valid_from",
    valid_to_column: str = "valid_to",
    as_of_timestamp: Optional[str] = None
) -> DataFrame:
    """
    Generate point-in-time correct feature view (supports SCD Type 2).
    
    Args:
        feature_df: Feature table DataFrame (with valid_from/valid_to columns)
        entity_keys: Entity keys
        event_time_column: Event time column name
        valid_from_column: Valid from timestamp column
        valid_to_column: Valid to timestamp column (NULL indicates currently valid)
        as_of_timestamp: Specific point in time (None for current)
    
    Returns:
        Point-in-time feature DataFrame
    """
    if as_of_timestamp:
        condition = f"({event_time_column} >= {valid_from_column}) AND " \
                   f"(({valid_to_column} IS NULL) OR ({event_time_column} < {valid_to_column})) AND " \
                   f"({event_time_column} <= '{as_of_timestamp}')"
    else:
        condition = f"({event_time_column} >= {valid_from_column}) AND " \
                   f"(({valid_to_column} IS NULL) OR ({event_time_column} < {valid_to_column}))"
    
    # Select valid record for each entity at specified point in time
    window_spec = Window.partitionBy(*entity_keys).orderBy(col(valid_from_column).desc())
    
    pit_df = feature_df.filter(condition) \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter("rn = 1") \
        .drop("rn")
    
    return pit_df


def create_scd2_features(
    df: DataFrame,
    entity_keys: List[str],
    timestamp_column: str,
    hash_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Create SCD Type 2 columns for dimension tables (is_current, effective_from/to).
    
    Args:
        df: Source dimension DataFrame
        entity_keys: Entity keys
        timestamp_column: Change timestamp column
        hash_columns: Columns used for change detection (None for all columns)
    
    Returns:
        DataFrame with SCD2 columns
    """
    from pyspark.sql.functions import lead, hash as spark_hash
    
    # Calculate change detection hash
    if hash_columns:
        hash_expr = spark_hash(*[col(c) for c in hash_columns])
    else:
        hash_expr = spark_hash(*[col(c) for c in df.columns if c not in entity_keys])
    
    df = df.withColumn("row_hash", hash_expr)
    
    # Order by entity and time to detect changes
    window_spec = Window.partitionBy(*entity_keys).orderBy(col(timestamp_column))
    
    scd2_df = df.withColumn("next_hash", lead("row_hash").over(window_spec)) \
        .withColumn("effective_from", col(timestamp_column)) \
        .withColumn("effective_to", lead(col(timestamp_column)).over(window_spec)) \
        .withColumn("is_current", when(col("effective_to").isNull(), True).otherwise(False)) \
        .drop("row_hash", "next_hash")
    
    return scd2_df


def add_feature_metadata(
    df: DataFrame,
    feature_timestamp: Optional[str] = None
) -> DataFrame:
    """
    Add metadata columns to feature table (feature_timestamp, load_timestamp).
    
    Args:
        df: Feature DataFrame
        feature_timestamp: Feature computation time (None for current timestamp)
    
    Returns:
        DataFrame with metadata columns
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    result_df = df.withColumn("load_timestamp", current_timestamp())
    
    if feature_timestamp:
        result_df = result_df.withColumn("feature_timestamp", lit(feature_timestamp))
    else:
        result_df = result_df.withColumn("feature_timestamp", current_timestamp())
    
    return result_df
