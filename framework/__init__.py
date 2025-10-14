"""
Insurance ML Data Platform Framework
Production-grade medallion architecture framework for Microsoft Fabric

Modules:
- libs: Core library functions (delta_ops, data_quality, cosmos_io, etc.)
- scripts: Management scripts (delta_maintenance, deploy_to_fabric, validate_deployment)
- setup: Initialization scripts (init_control_tables)
- config: Configuration files (YAML schemas, Great Expectations rules)
"""

__version__ = "1.0.0"
__author__ = "Patrick Cheung"
__description__ = "Production-Ready Medallion Architecture Framework for Microsoft Fabric"

# Core modules are imported via sys.path in notebooks
# Example: sys.path.append("/Workspace/framework/libs")
