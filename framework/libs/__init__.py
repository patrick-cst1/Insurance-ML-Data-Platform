"""
Framework Libraries for Insurance ML Data Platform
Provides reusable components for Medallion architecture
"""

__version__ = "1.0.0"

# Import all modules for easier access
from .delta_ops import read_delta, write_delta, merge_delta, optimize_delta
from .data_quality import (
    validate_schema,
    check_nulls,
    detect_duplicates,
    check_freshness,
    check_value_range,
    check_completeness
)
from .cosmos_io import connect_cosmos, query_cosmos, enrich_dataframe, batch_read_by_keys
from .schema_contracts import load_contract, validate_contract, build_spark_schema, enforce_contract
from .watermarking import (
    get_current_watermark,
    update_watermark,
    reset_watermark,
    get_incremental_filter
)
from .feature_utils import (
    build_aggregation_features,
    generate_point_in_time_view,
    create_scd2_features,
    add_feature_metadata
)
from .logging_utils import get_logger, log_dataframe_stats, log_pipeline_step, PipelineTimer
from .great_expectations_validator import (
    GreatExpectationsValidator,
    validate_with_great_expectations,
    create_standard_expectations_for_table
)

__all__ = [
    # Delta operations
    "read_delta",
    "write_delta",
    "merge_delta",
    "optimize_delta",
    # Data quality
    "validate_schema",
    "check_nulls",
    "detect_duplicates",
    "check_freshness",
    "check_value_range",
    "check_completeness",
    # Cosmos DB
    "connect_cosmos",
    "query_cosmos",
    "enrich_dataframe",
    "batch_read_by_keys",
    # Schema contracts
    "load_contract",
    "validate_contract",
    "build_spark_schema",
    "enforce_contract",
    # Watermarking
    "get_current_watermark",
    "update_watermark",
    "reset_watermark",
    "get_incremental_filter",
    # Feature utilities
    "build_aggregation_features",
    "generate_point_in_time_view",
    "create_scd2_features",
    "add_feature_metadata",
    # Logging
    "get_logger",
    "log_dataframe_stats",
    "log_pipeline_step",
    "PipelineTimer",
    # Great Expectations
    "GreatExpectationsValidator",
    "validate_with_great_expectations",
    "create_standard_expectations_for_table",
]
