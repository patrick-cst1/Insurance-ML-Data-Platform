"""
Data Quality Report Generator
Generates comprehensive DQ report from check results
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, max as spark_max, min as spark_min, avg, sum as spark_sum
import sys
import os
from datetime import datetime

sys.path.append("/Workspace/framework/libs")
from delta_ops import read_delta
from logging_utils import get_logger


def generate_dq_summary(spark: SparkSession) -> dict:
    """
    Generate DQ summary statistics.
    
    Returns:
        dict: Summary statistics
    """
    logger = get_logger("dq_report")
    
    logger.info("Generating Data Quality Summary Report")
    logger.info("=" * 60)
    
    # Read DQ results
    df_dq = read_delta(spark, "Tables/dq_check_results")
    
    # Overall statistics
    total_checks = df_dq.count()
    passed_checks = df_dq.filter("overall_passed = true").count()
    failed_checks = total_checks - passed_checks
    success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
    
    # Get latest check time
    latest_check = df_dq.select(spark_max("check_timestamp")).first()[0]
    
    # Per-table statistics
    table_stats = df_dq.groupBy("table_name").agg(
        count("*").alias("total_checks"),
        spark_sum(when(col("overall_passed") == True, 1).otherwise(0)).alias("passed"),
        spark_sum(when(col("overall_passed") == False, 1).otherwise(0)).alias("failed"),
        spark_max("check_timestamp").alias("last_check")
    ).collect()
    
    # Failed checks breakdown
    failed_details = df_dq.filter("overall_passed = false") \
        .select("table_name", "check_timestamp", "duplicate_check", "null_check", "freshness_check") \
        .collect()
    
    # Print report
    print("\n" + "=" * 80)
    print("DATA QUALITY SUMMARY REPORT")
    print(f"Generated: {datetime.now()}")
    print("=" * 80)
    
    print(f"\n📊 Overall Statistics:")
    print(f"  Total Checks: {total_checks}")
    print(f"  Passed: {passed_checks} ({success_rate:.2f}%)")
    print(f"  Failed: {failed_checks}")
    print(f"  Latest Check: {latest_check}")
    
    print(f"\n📋 Per-Table Statistics:")
    print("-" * 80)
    print(f"{'Table Name':<30} {'Total':<10} {'Passed':<10} {'Failed':<10} {'Last Check':<20}")
    print("-" * 80)
    
    for row in table_stats:
        print(f"{row['table_name']:<30} {row['total_checks']:<10} {row['passed']:<10} {row['failed']:<10} {str(row['last_check']):<20}")
    
    if failed_checks > 0:
        print(f"\n⚠️  Failed Checks Details:")
        print("-" * 80)
        
        for row in failed_details:
            print(f"\nTable: {row['table_name']}")
            print(f"  Timestamp: {row['check_timestamp']}")
            print(f"  Duplicate Check: {'PASSED' if row['duplicate_check'] else 'FAILED'}")
            print(f"  Null Check: {'PASSED' if row['null_check'] else 'FAILED'}")
            print(f"  Freshness Check: {'PASSED' if row['freshness_check'] else 'FAILED'}")
    
    print("\n" + "=" * 80)
    
    # Return summary dict
    return {
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "success_rate": success_rate,
        "latest_check": str(latest_check) if latest_check else None
    }


def generate_table_profile(spark: SparkSession, table_name: str):
    """Generate detailed profile for a specific table."""
    logger = get_logger("table_profile")
    
    logger.info(f"\nGenerating Profile for: {table_name}")
    logger.info("=" * 60)
    
    try:
        df = read_delta(spark, f"Tables/{table_name}")
        
        # Basic statistics
        row_count = df.count()
        col_count = len(df.columns)
        
        print(f"\n📊 Table: {table_name}")
        print(f"  Total Rows: {row_count:,}")
        print(f"  Total Columns: {col_count}")
        print(f"  Schema: {df.schema.simpleString()}")
        
        # Null count per column
        print(f"\n  Null Counts:")
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / row_count * 100) if row_count > 0 else 0
            if null_count > 0:
                print(f"    {col_name}: {null_count} ({null_pct:.2f}%)")
        
    except Exception as e:
        logger.error(f"Failed to profile table {table_name}: {str(e)}")


def main():
    """Main report generation."""
    spark = SparkSession.builder.appName("dq_report").getOrCreate()
    
    # Generate overall summary
    summary = generate_dq_summary(spark)
    
    # Generate profiles for key tables
    key_tables = ["silver_policies", "silver_claims", "silver_customers"]
    
    for table in key_tables:
        generate_table_profile(spark, table)
    
    print("\n✓ Data Quality Report Generation Completed\n")


if __name__ == "__main__":
    main()
