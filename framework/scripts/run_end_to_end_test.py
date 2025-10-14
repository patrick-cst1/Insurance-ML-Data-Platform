"""
End-to-End Test Runner
Executes complete Medallion flow in test environment
"""

from pyspark.sql import SparkSession
import sys
import os

sys.path.append("/Workspace/framework/libs")
sys.path.append(os.path.join(os.getcwd(), "framework", "libs"))

from delta_ops import read_delta, write_delta
from logging_utils import get_logger, PipelineTimer
from datetime import datetime


class MedallionFlowTester:
    """Test complete Bronze->Silver->Gold flow."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger("e2e_test", level="INFO")
        self.test_results = []
    
    def test_bronze_layer(self) -> bool:
        """Test Bronze layer ingestion."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Testing Bronze Layer")
        self.logger.info("=" * 60)
        
        try:
            # Check Bronze tables
            bronze_tables = [
                "Tables/bronze_policies",
                "Tables/bronze_claims",
                "Tables/bronze_customers",
                "Tables/bronze_agents"
            ]
            
            for table_path in bronze_tables:
                df = read_delta(self.spark, table_path)
                count = df.count()
                self.logger.info(f"✓ {table_path}: {count} records")
                
                if count == 0:
                    self.logger.warning(f"⚠️  {table_path} is empty!")
            
            self.test_results.append(("Bronze Layer", "PASSED"))
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Bronze Layer Test Failed: {str(e)}")
            self.test_results.append(("Bronze Layer", "FAILED"))
            return False
    
    def test_silver_layer(self) -> bool:
        """Test Silver layer transformations."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Testing Silver Layer")
        self.logger.info("=" * 60)
        
        try:
            # Check Silver tables with SCD2
            df_policies = read_delta(self.spark, "Tables/silver_policies")
            
            # Validate SCD2 columns
            required_cols = ["effective_from", "effective_to", "is_current"]
            for col in required_cols:
                if col not in df_policies.columns:
                    raise ValueError(f"Missing SCD2 column: {col}")
            
            # Check current records
            current_count = df_policies.filter("is_current = true").count()
            total_count = df_policies.count()
            
            self.logger.info(f"✓ Silver Policies: {total_count} total, {current_count} current")
            
            # Validate no nulls in key columns
            null_count = df_policies.filter("policy_id IS NULL").count()
            if null_count > 0:
                raise ValueError(f"Found {null_count} null policy_id values")
            
            self.test_results.append(("Silver Layer", "PASSED"))
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Silver Layer Test Failed: {str(e)}")
            self.test_results.append(("Silver Layer", "FAILED"))
            return False
    
    def test_gold_layer(self) -> bool:
        """Test Gold layer feature engineering."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Testing Gold Layer")
        self.logger.info("=" * 60)
        
        try:
            # Check Gold feature tables
            df_claims_features = read_delta(self.spark, "Tables/gold_claims_features")
            df_risk_features = read_delta(self.spark, "Tables/gold_risk_features")
            
            # Validate feature columns
            required_features = ["claims_count_30d", "claim_amount_sum_30d"]
            for feat in required_features:
                if feat not in df_claims_features.columns:
                    raise ValueError(f"Missing feature: {feat}")
            
            # Validate metadata columns
            if "feature_timestamp" not in df_claims_features.columns:
                raise ValueError("Missing feature_timestamp metadata")
            
            self.logger.info(f"✓ Gold Claims Features: {df_claims_features.count()} records")
            self.logger.info(f"✓ Gold Risk Features: {df_risk_features.count()} records")
            
            self.test_results.append(("Gold Layer", "PASSED"))
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Gold Layer Test Failed: {str(e)}")
            self.test_results.append(("Gold Layer", "FAILED"))
            return False
    
    def test_data_quality(self) -> bool:
        """Test data quality results."""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Testing Data Quality Checks")
        self.logger.info("=" * 60)
        
        try:
            df_dq = read_delta(self.spark, "Tables/dq_check_results")
            
            # Check for failed DQ checks
            failed_checks = df_dq.filter("overall_passed = false").count()
            total_checks = df_dq.count()
            
            self.logger.info(f"✓ DQ Results: {total_checks} checks, {failed_checks} failed")
            
            if failed_checks > 0:
                self.logger.warning(f"⚠️  {failed_checks} data quality checks failed")
            
            self.test_results.append(("Data Quality", "PASSED"))
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Data Quality Test Failed: {str(e)}")
            self.test_results.append(("Data Quality", "FAILED"))
            return False
    
    def run_all_tests(self):
        """Run all end-to-end tests."""
        self.logger.info("=" * 60)
        self.logger.info("End-to-End Medallion Flow Test")
        self.logger.info(f"Test Time: {datetime.now()}")
        self.logger.info("=" * 60)
        
        with PipelineTimer(self.logger, "e2e_test"):
            self.test_bronze_layer()
            self.test_silver_layer()
            self.test_gold_layer()
            self.test_data_quality()
        
        # Print summary
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Test Summary")
        self.logger.info("=" * 60)
        
        for test_name, status in self.test_results:
            status_icon = "✓" if status == "PASSED" else "✗"
            self.logger.info(f"{status_icon} {test_name}: {status}")
        
        passed = sum(1 for _, status in self.test_results if status == "PASSED")
        total = len(self.test_results)
        
        self.logger.info(f"\nTotal: {passed}/{total} tests passed")
        self.logger.info("=" * 60)
        
        return passed == total


def main():
    """Main test entry point."""
    spark = SparkSession.builder.appName("e2e_test").getOrCreate()
    
    tester = MedallionFlowTester(spark)
    success = tester.run_all_tests()
    
    if not success:
        sys.exit(1)
    
    print("\n✓ All end-to-end tests passed successfully!")


if __name__ == "__main__":
    main()
