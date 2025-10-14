"""
Great Expectations Integration for Data Quality Validation
Provides advanced validation capabilities using Great Expectations framework
"""

from pyspark.sql import DataFrame
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults
from typing import Dict, List, Optional
import logging


class GreatExpectationsValidator:
    """
    Wrapper for Great Expectations validation in Fabric environment.
    """
    
    def __init__(self, context_root_dir: Optional[str] = None):
        """
        Initialize Great Expectations context.
        
        Args:
            context_root_dir: Root directory for GE context (None for in-memory)
        """
        self.logger = logging.getLogger(__name__)
        
        if context_root_dir:
            self.context = gx.get_context(context_root_dir=context_root_dir)
        else:
            # Use in-memory context for Fabric environment
            self.context = self._create_in_memory_context()
    
    def _create_in_memory_context(self) -> BaseDataContext:
        """Create in-memory Great Expectations context."""
        data_context_config = DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults()
        )
        return BaseDataContext(project_config=data_context_config)
    
    def create_expectation_suite(self, suite_name: str) -> None:
        """
        Create or retrieve expectation suite.
        
        Args:
            suite_name: Name of the expectation suite
        """
        try:
            self.context.get_expectation_suite(expectation_suite_name=suite_name)
            self.logger.info(f"Expectation suite '{suite_name}' already exists")
        except:
            self.context.add_expectation_suite(expectation_suite_name=suite_name)
            self.logger.info(f"Created expectation suite '{suite_name}'")
    
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
        suite = self.context.get_expectation_suite(expectation_suite_name=suite_name)
        
        for exp_config in expectations_config:
            expectation_type = exp_config.pop("expectation_type")
            suite.add_expectation(
                expectation_configuration={
                    "expectation_type": expectation_type,
                    "kwargs": exp_config
                }
            )
        
        self.context.save_expectation_suite(expectation_suite=suite)
        self.logger.info(f"Added {len(expectations_config)} expectations to '{suite_name}'")
    
    def validate_dataframe(
        self,
        df: DataFrame,
        suite_name: str,
        batch_identifier: str = "default_batch"
    ) -> Dict:
        """
        Validate DataFrame against expectation suite.
        
        Args:
            df: PySpark DataFrame to validate
            suite_name: Name of the expectation suite
            batch_identifier: Identifier for this batch
        
        Returns:
            Validation results dictionary
        """
        # Create datasource for PySpark DataFrame
        datasource_config = {
            "name": "spark_datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine"
            },
            "data_connectors": {
                "runtime_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"]
                }
            }
        }
        
        try:
            self.context.add_datasource(**datasource_config)
        except:
            pass  # Datasource already exists
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_connector",
            data_asset_name=batch_identifier,
            batch_identifiers={"batch_id": batch_identifier},
            runtime_parameters={"batch_data": df}
        )
        
        # Create validator
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Run validation
        validation_results = validator.validate()
        
        # Extract results
        results_dict = {
            "success": validation_results.success,
            "statistics": validation_results.statistics,
            "results": []
        }
        
        for result in validation_results.results:
            results_dict["results"].append({
                "expectation_type": result.expectation_config.expectation_type,
                "success": result.success,
                "result": result.result
            })
        
        self.logger.info(
            f"Validation {'PASSED' if validation_results.success else 'FAILED'}: "
            f"{validation_results.statistics['successful_expectations']}/{validation_results.statistics['evaluated_expectations']} checks passed"
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
