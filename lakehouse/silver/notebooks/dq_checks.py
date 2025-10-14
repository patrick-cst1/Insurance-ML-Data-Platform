# Databricks notebook source
"""
Silver Layer: Comprehensive Data Quality Checks
Runs validation suite across all Silver tables and logs results
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import sys
import os
import json

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import read_delta, write_delta
from data_quality import validate_schema, check_nulls, detect_duplicates, check_freshness
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

# Data Quality Rules Configuration
DQ_RULES = {
    "silver_policies": {
        "key_columns": ["policy_id"],
        "required_columns": ["policy_id", "customer_id", "premium"],
        "null_threshold": 0.01,
        "freshness_hours": 24,
        "timestamp_column": "ingestion_timestamp"
    },
    "silver_claims": {
        "key_columns": ["claim_id"],
        "required_columns": ["claim_id", "policy_id", "claim_amount"],
        "null_threshold": 0.01,
        "freshness_hours": 24,
        "timestamp_column": "ingestion_timestamp"
    },
    "silver_customers": {
        "key_columns": ["customer_id"],
        "required_columns": ["customer_id", "name"],
        "null_threshold": 0.01,
        "freshness_hours": 24,
        "timestamp_column": "ingestion_timestamp"
    },
    "silver_agents": {
        "key_columns": ["agent_id"],
        "required_columns": ["agent_id", "name"],
        "null_threshold": 0.01,
        "freshness_hours": 24,
        "timestamp_column": "ingestion_timestamp"
    }
}

# COMMAND ----------

def run_dq_checks_for_table(spark, table_name, rules, logger):
    """
    Run comprehensive DQ checks for a single table.
    
    Returns:
        dict: DQ check results
    """
    logger.info(f"Running DQ checks for {table_name}")
    
    try:
        # Read table
        df = read_delta(spark, f"Tables/{table_name}")
        record_count = df.count()
        logger.info(f"{table_name}: {record_count} records")
        
        results = {
            "table_name": table_name,
            "record_count": record_count,
            "check_timestamp": str(current_timestamp()),
            "checks": {}
        }
        
        # 1. Duplicate Check
        dup_result = detect_duplicates(
            df, 
            key_columns=rules["key_columns"], 
            threshold=0
        )
        results["checks"]["duplicates"] = {
            "passed": dup_result["passed"],
            "duplicate_count": dup_result["duplicate_count"]
        }
        logger.info(f"  Duplicate check: {'PASSED' if dup_result['passed'] else 'FAILED'}")
        
        # 2. Null Check
        null_result = check_nulls(
            df, 
            columns=rules["required_columns"], 
            threshold=rules["null_threshold"]
        )
        results["checks"]["nulls"] = {
            "passed": null_result["passed"],
            "violations": null_result.get("violations", {})
        }
        logger.info(f"  Null check: {'PASSED' if null_result['passed'] else 'FAILED'}")
        
        # 3. Freshness Check
        freshness_result = check_freshness(
            df,
            timestamp_column=rules["timestamp_column"],
            max_age_hours=rules["freshness_hours"]
        )
        results["checks"]["freshness"] = {
            "passed": freshness_result["passed"],
            "age_hours": freshness_result.get("age_hours")
        }
        logger.info(f"  Freshness check: {'PASSED' if freshness_result['passed'] else 'FAILED'}")
        
        # Overall result
        results["overall_passed"] = all([
            dup_result["passed"],
            null_result["passed"],
            freshness_result["passed"]
        ])
        
        return results
        
    except Exception as e:
        logger.error(f"DQ checks failed for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "overall_passed": False,
            "error": str(e)
        }

# COMMAND ----------

def main():
    logger = get_logger("silver_dq_checks")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "dq_checks"):
        
        all_results = []
        
        # Run DQ checks for all tables
        for table_name, rules in DQ_RULES.items():
            result = run_dq_checks_for_table(spark, table_name, rules, logger)
            all_results.append(result)
        
        # Create results DataFrame
        results_rows = [
            {
                "table_name": r["table_name"],
                "record_count": r.get("record_count", 0),
                "overall_passed": r["overall_passed"],
                "duplicate_check": r.get("checks", {}).get("duplicates", {}).get("passed", False),
                "null_check": r.get("checks", {}).get("nulls", {}).get("passed", False),
                "freshness_check": r.get("checks", {}).get("freshness", {}).get("passed", False),
                "details": json.dumps(r)
            }
            for r in all_results
        ]
        results_df = spark.createDataFrame(results_rows)
        results_df = results_df.withColumn("check_timestamp", current_timestamp())
        
        # Write DQ results to Delta table
        write_delta(
            df=results_df,
            path="Tables/dq_check_results",
            mode="append",
            partition_by=["check_timestamp"]
        )
        
        # Summary
        total_checks = len(all_results)
        passed_checks = sum(1 for r in all_results if r["overall_passed"])
        
        logger.info(f"DQ Summary: {passed_checks}/{total_checks} tables passed all checks")
        
        # Fail pipeline if any critical table failed
        if passed_checks < total_checks:
            failed_tables = [r["table_name"] for r in all_results if not r["overall_passed"]]
            logger.error(f"DQ checks failed for tables: {failed_tables}")
            # Uncomment to fail pipeline on DQ issues:
            # raise ValueError(f"Data quality checks failed for: {failed_tables}")
        else:
            logger.info("All DQ checks PASSED ✓")

# COMMAND ----------

if __name__ == "__main__":
    main()
