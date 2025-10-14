"""
Comprehensive Deployment Validation Script
Validates that all components are properly deployed and configured
"""

from pyspark.sql import SparkSession
import sys
import os
from typing import List, Dict

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))
from logging_utils import get_logger


class DeploymentValidator:
    """Validates deployment of Insurance ML Data Platform."""
    
    def __init__(self, spark: SparkSession, logger):
        self.spark = spark
        self.logger = logger
        self.validation_results = []
    
    def validate_table_exists(self, table_path: str, table_name: str) -> Dict:
        """Validate that a Delta table exists and is readable."""
        try:
            df = self.spark.read.format("delta").load(table_path)
            row_count = df.count()
            col_count = len(df.columns)
            
            self.logger.info(f"✓ {table_name}: {row_count} rows, {col_count} columns")
            return {
                "table": table_name,
                "status": "SUCCESS",
                "row_count": row_count,
                "column_count": col_count
            }
        except Exception as e:
            self.logger.error(f"✗ {table_name}: {str(e)}")
            return {
                "table": table_name,
                "status": "FAILED",
                "error": str(e)
            }
    
    def validate_bronze_layer(self) -> List[Dict]:
        """Validate Bronze layer tables."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Validating Bronze Layer")
        self.logger.info("=" * 60)
        
        bronze_tables = [
            ("Tables/bronze_policies", "bronze_policies"),
            ("Tables/bronze_claims", "bronze_claims"),
            ("Tables/bronze_customers", "bronze_customers"),
            ("Tables/bronze_agents", "bronze_agents"),
            ("Tables/bronze_realtime_events", "bronze_realtime_events")
        ]
        
        results = []
        for table_path, table_name in bronze_tables:
            result = self.validate_table_exists(table_path, table_name)
            results.append(result)
        
        return results
    
    def validate_silver_layer(self) -> List[Dict]:
        """Validate Silver layer tables."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Validating Silver Layer")
        self.logger.info("=" * 60)
        
        silver_tables = [
            ("Tables/silver_policies", "silver_policies"),
            ("Tables/silver_claims", "silver_claims"),
            ("Tables/silver_customers", "silver_customers"),
            ("Tables/silver_agents", "silver_agents"),
            ("Tables/silver_policies_enriched", "silver_policies_enriched"),
            ("Tables/silver_realtime_claims", "silver_realtime_claims")
        ]
        
        results = []
        for table_path, table_name in silver_tables:
            result = self.validate_table_exists(table_path, table_name)
            
            # Additional validation for SCD2 tables
            if result["status"] == "SUCCESS" and "silver_policies" in table_name:
                try:
                    df = self.spark.read.format("delta").load(table_path)
                    if "is_current" in df.columns:
                        current_count = df.filter("is_current = true").count()
                        self.logger.info(f"  → Current records: {current_count}")
                except:
                    pass
            
            results.append(result)
        
        return results
    
    def validate_gold_layer(self) -> List[Dict]:
        """Validate Gold layer tables."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Validating Gold Layer")
        self.logger.info("=" * 60)
        
        gold_tables = [
            ("Tables/gold_claims_features", "gold_claims_features"),
            ("Tables/gold_customer_features", "gold_customer_features"),
            ("Tables/gold_risk_features", "gold_risk_features"),
            ("Tables/gold_streaming_features", "gold_streaming_features")
        ]
        
        results = []
        for table_path, table_name in gold_tables:
            result = self.validate_table_exists(table_path, table_name)
            results.append(result)
        
        return results
    
    def validate_control_tables(self) -> List[Dict]:
        """Validate control and metadata tables."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Validating Control Tables")
        self.logger.info("=" * 60)
        
        control_tables = [
            ("Tables/watermark_control", "watermark_control"),
            ("Tables/dq_check_results", "dq_check_results")
        ]
        
        results = []
        for table_path, table_name in control_tables:
            result = self.validate_table_exists(table_path, table_name)
            results.append(result)
        
        return results
    
    def validate_framework_libraries(self) -> Dict:
        """Validate that framework libraries can be imported."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Validating Framework Libraries")
        self.logger.info("=" * 60)
        
        libraries = [
            "delta_ops",
            "data_quality",
            "cosmos_io",
            "schema_contracts",
            "watermarking",
            "feature_utils",
            "logging_utils"
        ]
        
        failed_imports = []
        for lib in libraries:
            try:
                __import__(lib)
                self.logger.info(f"✓ {lib}")
            except Exception as e:
                self.logger.error(f"✗ {lib}: {str(e)}")
                failed_imports.append(lib)
        
        return {
            "status": "SUCCESS" if len(failed_imports) == 0 else "FAILED",
            "failed_imports": failed_imports
        }
    
    def generate_summary(self, all_results: List[Dict]) -> Dict:
        """Generate validation summary."""
        total = len(all_results)
        successful = sum(1 for r in all_results if r["status"] == "SUCCESS")
        failed = total - successful
        
        return {
            "total_validations": total,
            "successful": successful,
            "failed": failed,
            "success_rate": (successful / total * 100) if total > 0 else 0
        }
    
    def run_full_validation(self):
        """Run complete deployment validation."""
        self.logger.info("=" * 60)
        self.logger.info("DEPLOYMENT VALIDATION STARTED")
        self.logger.info("=" * 60)
        
        # Validate all layers
        bronze_results = self.validate_bronze_layer()
        silver_results = self.validate_silver_layer()
        gold_results = self.validate_gold_layer()
        control_results = self.validate_control_tables()
        framework_result = self.validate_framework_libraries()
        
        # Combine all results
        all_results = bronze_results + silver_results + gold_results + control_results
        
        # Generate summary
        summary = self.generate_summary(all_results)
        
        # Print summary
        self.logger.info("\n" + "=" * 60)
        self.logger.info("VALIDATION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total Validations: {summary['total_validations']}")
        self.logger.info(f"Successful: {summary['successful']}")
        self.logger.info(f"Failed: {summary['failed']}")
        self.logger.info(f"Success Rate: {summary['success_rate']:.2f}%")
        self.logger.info("=" * 60)
        
        if summary['failed'] > 0:
            self.logger.error("\n⚠️  DEPLOYMENT VALIDATION FAILED")
            failed_items = [r['table'] for r in all_results if r['status'] == 'FAILED']
            self.logger.error(f"Failed items: {failed_items}")
            return False
        else:
            self.logger.info("\n✓ DEPLOYMENT VALIDATION PASSED")
            return True


def main():
    """Main validation entry point."""
    logger = get_logger("deployment_validation", level="INFO")
    spark = SparkSession.builder.appName("deployment_validation").getOrCreate()
    
    validator = DeploymentValidator(spark, logger)
    success = validator.run_full_validation()
    
    if not success:
        raise Exception("Deployment validation failed")
    
    print("\n✓ All deployment validations completed successfully")


if __name__ == "__main__":
    main()
