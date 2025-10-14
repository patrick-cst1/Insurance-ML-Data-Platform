from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from typing import Optional, Dict, List


def read_delta(
    spark: SparkSession,
    path: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None
) -> DataFrame:
    """
    Read Delta table with time travel support.
    
    Args:
        spark: SparkSession instance
        path: Delta table path
        version: Optional version number for time travel
        timestamp: Optional timestamp for time travel
    
    Returns:
        DataFrame
    """
    reader = spark.read.format("delta")
    
    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    
    return reader.load(path)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = False,
    overwrite_schema: bool = False,
    description: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None
) -> None:
    """
    Write DataFrame to Delta table with Purview lineage metadata.
    
    Args:
        df: DataFrame to write
        path: Delta table path
        mode: Write mode (append/overwrite)
        partition_by: List of partition columns
        merge_schema: Whether to merge schema
        overwrite_schema: Whether to overwrite schema
        description: Table description for Purview metadata
        tags: Custom tags for Purview classification (e.g., {"layer": "silver", "pii": "true"})
    """
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    
    # Add Purview metadata as Delta table properties
    # Fabric auto-syncs these to Purview Hub
    if description:
        writer = writer.option("delta.description", description)
    
    if tags:
        for key, value in tags.items():
            writer = writer.option(f"delta.{key}", value)
    
    writer.save(path)


def merge_delta(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    merge_condition: str,
    when_matched_update: Optional[Dict[str, str]] = None,
    when_not_matched_insert: Optional[Dict[str, str]] = None,
    when_matched_delete: Optional[str] = None
) -> None:
    """
    Execute Delta MERGE operation, supporting upsert and SCD2 scenarios.
    
    Args:
        spark: SparkSession instance
        target_path: Target Delta table path
        source_df: Source DataFrame
        merge_condition: MERGE condition (e.g., "target.id = source.id")
        when_matched_update: Update mapping when matched
        when_not_matched_insert: Insert mapping when not matched
        when_matched_delete: Delete condition when matched
    """
    delta_table = DeltaTable.forPath(spark, target_path)
    
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    )
    
    if when_matched_update:
        merge_builder = merge_builder.whenMatchedUpdate(set=when_matched_update)
    
    if when_matched_delete:
        merge_builder = merge_builder.whenMatchedDelete(condition=when_matched_delete)
    
    if when_not_matched_insert:
        merge_builder = merge_builder.whenNotMatchedInsert(values=when_not_matched_insert)
    
    merge_builder.execute()


def optimize_delta(
    spark: SparkSession,
    path: str,
    zorder_by: Optional[List[str]] = None,
    vacuum_retention_hours: Optional[int] = None
) -> None:
    """
    Optimize Delta table (OPTIMIZE + optional ZORDER + VACUUM).
    
    Args:
        spark: SparkSession instance
        path: Delta table path
        zorder_by: List of columns for ZORDER
        vacuum_retention_hours: VACUUM retention hours (default: no vacuum)
    """
    delta_table = DeltaTable.forPath(spark, path)
    
    # Run OPTIMIZE with fallback for OSS Delta
    try:
        optimize_builder = delta_table.optimize()  # Fabric API
        if zorder_by:
            optimize_builder = optimize_builder.executeZOrderBy(zorder_by)
        else:
            optimize_builder.executeCompaction()
    except Exception:
        # Fallback using Spark SQL (Delta Lake OSS / Fabric runtime)
        try:
            if zorder_by and len(zorder_by) > 0:
                cols = ", ".join(zorder_by)
                spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
            else:
                spark.sql(f"OPTIMIZE delta.`{path}`")
        except Exception:
            # Some Fabric runtimes may not support OPTIMIZE; proceed without compaction
            pass
    
    # Run VACUUM (optional) with fallback
    if vacuum_retention_hours is not None:
        try:
            delta_table.vacuum(vacuum_retention_hours)
        except Exception:
            try:
                spark.sql(f"VACUUM delta.`{path}` RETAIN {vacuum_retention_hours} HOURS")
            except Exception:
                # If VACUUM is not supported, skip silently
                pass
