"""
Great Expectations Integration for Data Quality Validation
Provides advanced validation capabilities using Great Expectations framework
Compatible with Great Expectations 0.18.x
"""

from pyspark.sql import DataFrame
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset
from typing import Dict, List, Optional, Any
import logging


class GreatExpectationsValidator:
    """
    Wrapper for Great Expectations validation in Fabric environment.
    Uses SparkDFDataset for compatibility with Azure Fabric.
    """
    
    def __init__(self, context_root_dir: Optional[str] = None):
        """
        Initialize Great Expectations validator.
        
        Args:
            context_root_dir: Root directory for GE context (None for in-memory)
        """
        self.logger = logging.getLogger(__name__)
        # Store expectations for validation
        self.expectations = {}
    
    def _create_in_memory_context(self):
        """Create in-memory Great Expectations context - not used in SparkDFDataset mode."""
        pass
    
    def create_expectation_suite(self, suite_name: str) -> None:
        """
        Create expectation suite.
        
        Args:
            suite_name: Name of the expectation suite
        """
        if suite_name not in self.expectations:
            self.expectations[suite_name] = []
            self.logger.info(f"Created expectation suite '{suite_name}'")
        else:
            self.logger.info(f"Expectation suite '{suite_name}' already exists")
    
    def add_expectations_from_config(
        self,
        suite_name: str,
        expectations_config: List[Dict]
    ) -> None:
        """
        Add expectations to suite from configuration.
        
        Args:
            suite_name: Name of the expectation suite
            expectations_config: List of expectation configurations
        """
        if suite_name not in self.expectations:
            self.expectations[suite_name] = []
        
        self.expectations[suite_name].extend(expectations_config)
        self.logger.info(f"Added {len(expectations_config)} expectations to '{suite_name}'")
    
    def validate_dataframe(
        self,
        df: DataFrame,
        suite_name: str,
        batch_identifier: str = "default_batch"
    ) -> Dict:
        """
        Validate DataFrame against expectation suite using SparkDFDataset.
        
        Args:
            df: PySpark DataFrame to validate
            suite_name: Name of the expectation suite
            batch_identifier: Identifier for this batch
        
        Returns:
            Validation results dictionary
        """
        # Get expectations for this suite
        expectations_config = self.expectations.get(suite_name, [])
        
        if not expectations_config:
            self.logger.warning(f"No expectations found for suite '{suite_name}'")
            return {
                "success": True,
                "statistics": {
                    "evaluated_expectations": 0,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0
                },
                "results": []
            }
        
        # Create SparkDFDataset for validation
        ge_df = SparkDFDataset(df)
        
        # Run each expectation and collect results
        results = []
        successful = 0
        evaluated = 0
        
        for exp_config in expectations_config:
            expectation_type = exp_config.get("expectation_type")
            kwargs = {k: v for k, v in exp_config.items() if k != "expectation_type"}
            
            try:
                # Get the expectation method
                expectation_method = getattr(ge_df, expectation_type, None)
                
                if expectation_method:
                    result = expectation_method(**kwargs)
                    evaluated += 1
                    
                    if result.get("success", False):
                        successful += 1
                    
                    results.append({
                        "expectation_type": expectation_type,
                        "success": result.get("success", False),
                        "result": result
                    })
                else:
                    self.logger.warning(f"Unknown expectation type: {expectation_type}")
                    results.append({
                        "expectation_type": expectation_type,
                        "success": False,
                        "result": {"error": f"Unknown expectation type: {expectation_type}"}
                    })
                    evaluated += 1
                    
            except Exception as e:
                self.logger.error(f"Error executing {expectation_type}: {str(e)}")
                results.append({
                    "expectation_type": expectation_type,
                    "success": False,
                    "result": {"error": str(e)}
                })
                evaluated += 1
        
        # Calculate statistics
        success_rate = (successful / evaluated * 100.0) if evaluated > 0 else 0.0
        all_passed = (successful == evaluated)
        
        results_dict = {
            "success": all_passed,
            "statistics": {
                "evaluated_expectations": evaluated,
                "successful_expectations": successful,
                "unsuccessful_expectations": evaluated - successful,
                "success_percent": success_rate
            },
            "results": results
        }
        
        self.logger.info(
            f"Validation {'PASSED' if all_passed else 'FAILED'}: "
            f"{successful}/{evaluated} checks passed ({success_rate:.1f}%)"
        )
        
        return results_dict


def validate_with_great_expectations(
    df: DataFrame,
    table_name: str,
    expectations_config: List[Dict],
    suite_name: Optional[str] = None
) -> Dict:
    """
    Convenience function to validate DataFrame with Great Expectations.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table being validated
        expectations_config: List of expectation configurations
        suite_name: Name of expectation suite (defaults to table_name)
    
    Returns:
        Validation results dictionary
    
    Example:
        expectations = [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": "customer_id"
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "column": "premium",
                "min_value": 0,
                "max_value": 1000000
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "column": "status",
                "value_set": ["ACTIVE", "INACTIVE", "PENDING"]
            }
        ]
        
        results = validate_with_great_expectations(
            df=df_policies,
            table_name="silver_policies",
            expectations_config=expectations
        )
    """
    suite_name = suite_name or f"{table_name}_suite"
    
    validator = GreatExpectationsValidator()
    validator.create_expectation_suite(suite_name)
    validator.add_expectations_from_config(suite_name, expectations_config)
    
    return validator.validate_dataframe(df, suite_name, table_name)


def create_standard_expectations_for_table(
    table_type: str,
    required_columns: List[str],
    numeric_columns: Optional[List[str]] = None,
    categorical_columns: Optional[Dict[str, List[str]]] = None
) -> List[Dict]:
    """
    Generate standard expectations based on table type.
    
    Args:
        table_type: Type of table (bronze/silver/gold)
        required_columns: Columns that should not be null
        numeric_columns: Numeric columns to validate ranges
        categorical_columns: Dict of column -> allowed values
    
    Returns:
        List of expectation configurations
    """
    expectations = []
    
    # Required columns should not be null
    for col in required_columns:
        expectations.append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": col
        })
    
    # Numeric columns should be >= 0 (for most insurance data)
    if numeric_columns:
        for col in numeric_columns:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_between",
                "column": col,
                "min_value": 0,
                "mostly": 0.95  # Allow 5% outliers
            })
    
    # Categorical columns should be in allowed set
    if categorical_columns:
        for col, allowed_values in categorical_columns.items():
            expectations.append({
                "expectation_type": "expect_column_values_to_be_in_set",
                "column": col,
                "value_set": allowed_values
            })
    
    # Table-level expectations
    expectations.append({
        "expectation_type": "expect_table_row_count_to_be_between",
        "min_value": 1  # Table should not be empty
    })
    
    return expectations
