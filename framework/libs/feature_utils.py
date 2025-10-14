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
    構建時間窗口聚合特徵（例如 30/90/365 天）。
    
    Args:
        df: 原始事件 DataFrame
        entity_keys: 實體鍵列表（例如 customer_id）
        timestamp_column: 時間戳欄位
        aggregation_windows: 窗口列表（天數）
        agg_columns: 聚合欄位與函數映射（例如 {"claim_amount": ["sum", "avg", "count"]}）
    
    Returns:
        特徵 DataFrame
    """
    from pyspark.sql.functions import current_date
    
    feature_dfs = []
    
    for window_days in aggregation_windows:
        # 過濾窗口內數據
        windowed_df = df.filter(
            expr(f"datediff(current_date(), {timestamp_column}) <= {window_days}")
        )
        
        # 構建聚合
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
    
    # Join 所有窗口特徵
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
    生成 point-in-time 正確嘅特徵視圖（支援 SCD2）。
    
    Args:
        feature_df: 特徵表 DataFrame（含 valid_from/valid_to）
        entity_keys: 實體鍵
        event_time_column: 事件時間欄位
        valid_from_column: 有效起始時間欄位
        valid_to_column: 有效結束時間欄位（NULL 表示當前有效）
        as_of_timestamp: 指定時間點（None 表示當前）
    
    Returns:
        Point-in-time 特徵 DataFrame
    """
    if as_of_timestamp:
        condition = f"({event_time_column} >= {valid_from_column}) AND " \
                   f"(({valid_to_column} IS NULL) OR ({event_time_column} < {valid_to_column})) AND " \
                   f"({event_time_column} <= '{as_of_timestamp}')"
    else:
        condition = f"({event_time_column} >= {valid_from_column}) AND " \
                   f"(({valid_to_column} IS NULL) OR ({event_time_column} < {valid_to_column}))"
    
    # 選擇每個實體在指定時間點有效嘅記錄
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
    為維度表創建 SCD Type 2 欄位（is_current、effective_from/to）。
    
    Args:
        df: 原始維度 DataFrame
        entity_keys: 實體鍵
        timestamp_column: 變更時間戳欄位
        hash_columns: 用於檢測變更嘅欄位（None 表示全部）
    
    Returns:
        含 SCD2 欄位嘅 DataFrame
    """
    from pyspark.sql.functions import lead, hash as spark_hash
    
    # 計算變更 hash
    if hash_columns:
        hash_expr = spark_hash(*[col(c) for c in hash_columns])
    else:
        hash_expr = spark_hash(*[col(c) for c in df.columns if c not in entity_keys])
    
    df = df.withColumn("row_hash", hash_expr)
    
    # 按實體與時間排序，檢測變更
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
    為特徵表添加元數據欄位（feature_timestamp、load_timestamp）。
    
    Args:
        df: 特徵 DataFrame
        feature_timestamp: 特徵計算時間（None 表示當前）
    
    Returns:
        含元數據欄位嘅 DataFrame
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    result_df = df.withColumn("load_timestamp", current_timestamp())
    
    if feature_timestamp:
        result_df = result_df.withColumn("feature_timestamp", lit(feature_timestamp))
    else:
        result_df = result_df.withColumn("feature_timestamp", current_timestamp())
    
    return result_df
