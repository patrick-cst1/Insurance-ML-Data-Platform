# Azure Fabric notebook source
"""
Silver Layer: Data Quality Checks with Great Expectations
Enhanced validation using Great Expectations framework
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import sys
import os
import yaml
import json

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from delta_ops import read_delta, write_delta
from great_expectations_validator import validate_with_great_expectations
from logging_utils import get_logger, PipelineTimer

# COMMAND ----------

# Configuration
GE_RULES_CANDIDATES = [
    "/Workspace/framework/config/great_expectations_rules.yaml",
    "framework/config/great_expectations_rules.yaml"
]
DQ_RESULTS_PATH = "Tables/dq_check_results_ge"

# COMMAND ----------

def load_ge_rules(rules_path: str) -> dict:
    """Load Great Expectations rules from YAML."""
    with open(rules_path, 'r') as f:
        return yaml.safe_load(f)

def load_ge_rules_with_fallback(paths: list) -> dict:
    for p in paths:
        try:
            return load_ge_rules(p)
        except Exception:
            continue
    raise FileNotFoundError("great_expectations_rules.yaml not found in candidates: " + ",".join(paths))

# COMMAND ----------

def run_ge_validation_for_table(spark, table_name: str, expectations: list, logger) -> dict:
    """
    Run Great Expectations validation for a single table.
    
    Returns:
        dict: Validation results
    """
    logger.info(f"Running Great Expectations validation for {table_name}")
    
    try:
        # Read table
        df = read_delta(spark, f"Tables/{table_name}")
        record_count = df.count()
        logger.info(f"{table_name}: {record_count} records")
        
        # Run Great Expectations validation
        validation_results = validate_with_great_expectations(
            df=df,
            table_name=table_name,
            expectations_config=expectations
        )
        
        # Log results
        success = validation_results["success"]
        stats = validation_results["statistics"]
        
        logger.info(
            f"  GE Validation: {'PASSED' if success else 'FAILED'} "
            f"({stats['successful_expectations']}/{stats['evaluated_expectations']} checks passed)"
        )
        
        # Extract failed expectations
        failed_expectations = [
            r for r in validation_results["results"] if not r["success"]
        ]
        
        return {
            "table_name": table_name,
            "record_count": record_count,
            "overall_passed": success,
            "successful_expectations": stats["successful_expectations"],
            "evaluated_expectations": stats["evaluated_expectations"],
            "failed_expectations": failed_expectations,
            "check_timestamp": str(current_timestamp()),
            "validation_engine": "great_expectations"
        }
        
    except Exception as e:
        logger.error(f"GE validation failed for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "overall_passed": False,
            "error": str(e),
            "validation_engine": "great_expectations"
        }

# COMMAND ----------

def main():
    logger = get_logger("silver_dq_ge_checks")
    spark = SparkSession.builder.getOrCreate()
    
    with PipelineTimer(logger, "dq_checks_great_expectations"):
        
        # Load GE rules
        ge_rules = load_ge_rules_with_fallback(GE_RULES_CANDIDATES)
        
        all_results = []
        
        # Run GE validation for each configured table
        for table_name, config in ge_rules.items():
            expectations = config.get("expectations", [])
            
            if expectations:
                result = run_ge_validation_for_table(spark, table_name, expectations, logger)
                all_results.append(result)
        
        # Create results DataFrame
        results_rows = [
            {
                "table_name": r["table_name"],
                "record_count": r.get("record_count", 0),
                "overall_passed": r["overall_passed"],
                "successful_expectations": r.get("successful_expectations", 0),
                "evaluated_expectations": r.get("evaluated_expectations", 0),
                "validation_engine": "great_expectations",
                "details": json.dumps(r)
            }
            for r in all_results
        ]
        results_df = spark.createDataFrame(results_rows)
        results_df = results_df.withColumn("check_timestamp", current_timestamp())
        
        # Write GE results to Delta table
        write_delta(
            df=results_df,
            path=DQ_RESULTS_PATH,
            mode="append",
            partition_by=["check_timestamp"]
        )
        
        # Summary
        total_checks = len(all_results)
        passed_checks = sum(1 for r in all_results if r["overall_passed"])
        
        logger.info(f"Great Expectations Summary: {passed_checks}/{total_checks} tables passed all checks")
        
        # Log failed expectations
        for result in all_results:
            if not result["overall_passed"] and "failed_expectations" in result:
                logger.warning(f"Failed expectations for {result['table_name']}:")
                for exp in result["failed_expectations"]:
                    logger.warning(f"  - {exp['expectation_type']}")
        
        if passed_checks < total_checks:
            failed_tables = [r["table_name"] for r in all_results if not r["overall_passed"]]
            logger.error(f"Great Expectations checks failed for tables: {failed_tables}")
            # Uncomment to fail pipeline on DQ issues:
            # raise ValueError(f"Data quality checks failed for: {failed_tables}")
        else:
            logger.info("All Great Expectations checks PASSED ✓")

# COMMAND ----------

if __name__ == "__main__":
    main()
