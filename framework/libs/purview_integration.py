"""
Microsoft Purview Hub Integration
Utilities for enriching Delta tables with Purview metadata
"""

from typing import Dict, Optional


class PurviewMetadata:
    """
    Helper class for generating Purview-compatible metadata tags.
    
    Microsoft Fabric automatically syncs Delta table properties to Purview Hub.
    This class standardizes metadata formatting for consistency.
    """
    
    @staticmethod
    def get_layer_tags(layer: str, table_name: str, pii: bool = False) -> Dict[str, str]:
        """
        Generate standard tags for medallion layer tables.
        
        Args:
            layer: Medallion layer (bronze/silver/gold)
            table_name: Table name
            pii: Whether table contains PII data
        
        Returns:
            Dictionary of metadata tags
        """
        tags = {
            "layer": layer,
            "table": table_name,
            "project": "Insurance-ML-Platform",
            "domain": "Insurance"
        }
        
        if pii:
            tags["pii"] = "true"
            tags["sensitivity"] = "high"
        
        return tags
    
    @staticmethod
    def get_bronze_metadata(table_name: str, source_system: str) -> Dict[str, str]:
        """
        Generate metadata for Bronze layer tables.
        
        Args:
            table_name: Table name
            source_system: Source system name (e.g., "CSV", "Eventstream")
        
        Returns:
            Metadata dictionary with description and tags
        """
        return {
            "description": f"Bronze layer: Raw data from {source_system}",
            "tags": {
                "layer": "bronze",
                "table": table_name,
                "source_system": source_system,
                "immutable": "true"
            }
        }
    
    @staticmethod
    def get_silver_metadata(table_name: str, has_scd2: bool = False, pii: bool = False) -> Dict[str, str]:
        """
        Generate metadata for Silver layer tables.
        
        Args:
            table_name: Table name
            has_scd2: Whether table implements SCD Type 2
            pii: Whether table contains PII
        
        Returns:
            Metadata dictionary
        """
        description = f"Silver layer: Cleaned and validated {table_name}"
        if has_scd2:
            description += " (SCD Type 2)"
        
        tags = {
            "layer": "silver",
            "table": table_name,
            "scd_type": "2" if has_scd2 else "0"
        }
        
        if pii:
            tags["pii"] = "true"
            tags["sensitivity"] = "high"
        
        return {
            "description": description,
            "tags": tags
        }
    
    @staticmethod
    def get_gold_metadata(table_name: str, feature_type: str) -> Dict[str, str]:
        """
        Generate metadata for Gold layer feature tables.
        
        Args:
            table_name: Table name
            feature_type: Feature type (e.g., "claims", "customer", "risk")
        
        Returns:
            Metadata dictionary
        """
        return {
            "description": f"Gold layer: ML features for {feature_type}",
            "tags": {
                "layer": "gold",
                "table": table_name,
                "feature_type": feature_type,
                "ml_ready": "true"
            }
        }


# Example usage in notebooks:
# from framework.libs.purview_integration import PurviewMetadata
# from framework.libs.delta_ops import write_delta
#
# metadata = PurviewMetadata.get_silver_metadata("silver_policies", has_scd2=True, pii=True)
# write_delta(
#     df=cleaned_df,
#     path="Tables/silver_policies",
#     description=metadata["description"],
#     tags=metadata["tags"]
# )
