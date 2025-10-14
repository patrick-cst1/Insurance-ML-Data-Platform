# Microsoft Purview Hub Integration Guide

## Overview

Microsoft Purview Hub is **natively integrated** into Microsoft Fabric, providing automatic data governance, catalog, and lineage tracking without external configuration.

## Architecture Decision

### What Purview Provides (Auto-Enabled)

1. **Data Catalog** - Auto-discovers all Fabric assets
   - Lakehouses (lh_bronze, lh_silver, lh_gold)
   - Delta tables (bronze_*, silver_*, gold_*)
   - KQL Database (kql_insurance_realtime)
   - Pipelines (master_batch_pipeline, gold_realtime_aggregation)
   - Notebooks (all framework and lakehouse notebooks)

2. **Lineage Tracking** - Auto-captures data flow
   - Table → Table relationships
   - Notebook → Table transformations
   - Pipeline → Table dependencies

3. **Metadata Management** - Auto-syncs schema information
   - Delta table schemas
   - Column descriptions
   - Custom properties

### What Purview Does NOT Provide

1. **Custom Data Quality Rules** ❌
   - No support for null checks with custom thresholds
   - No duplicate detection logic
   - No freshness SLA validation
   - No Great Expectations integration

2. **Pipeline Performance Monitoring** ❌
   - No success rate tracking
   - No duration metrics
   - No streaming lag monitoring
   - No throughput dashboards

### Decision: Retain Both Systems

This platform uses **Purview Hub for governance** + **Custom dashboards for operations**:

| Use Case | System |
|----------|--------|
| Data discovery & search | Purview Hub |
| Lineage visualization | Purview Hub |
| Custom DQ validation | Custom dashboards (`monitoring/dashboards/data_quality_dashboard.json`) |
| Pipeline performance | Custom dashboards (`monitoring/dashboards/pipeline_performance_dashboard.json`) |

---

## Configuration

### Enable Purview (Already Auto-Enabled in Fabric)

No setup required. Purview Hub is automatically available in all Fabric workspaces.

Configuration in `devops/parameters/fabric.yml`:

```yaml
purview:
  enabled: true  # Auto-enabled in Fabric
  features:
    data_catalog: true        # Auto-register Delta tables
    lineage_tracking: true    # Auto-track transformations
    metadata_sync: true       # Sync schema contracts
  custom_properties:
    project: Insurance-ML-Platform
    data_domain: Insurance
    criticality: High
```

---

## Usage Examples

### Example 1: Write Delta Table with Purview Metadata

```python
from framework.libs.delta_ops import write_delta
from framework.libs.purview_integration import PurviewMetadata

# Generate metadata for Silver layer table
metadata = PurviewMetadata.get_silver_metadata(
    table_name="silver_policies",
    has_scd2=True,
    pii=True
)

# Write table with Purview metadata
write_delta(
    df=cleaned_policies_df,
    path="Tables/silver_policies",
    mode="overwrite",
    partition_by=["policy_year"],
    description=metadata["description"],  # "Silver layer: Cleaned and validated silver_policies (SCD Type 2)"
    tags=metadata["tags"]  # {"layer": "silver", "table": "silver_policies", "scd_type": "2", "pii": "true", "sensitivity": "high"}
)
```

**Result**: Purview Hub automatically:
- Registers `silver_policies` table in Data Catalog
- Adds description and tags to table metadata
- Captures lineage from source notebook

### Example 2: Bronze Layer Ingestion

```python
from framework.libs.delta_ops import write_delta
from framework.libs.purview_integration import PurviewMetadata

# Generate Bronze metadata
metadata = PurviewMetadata.get_bronze_metadata(
    table_name="bronze_claims",
    source_system="CSV"
)

write_delta(
    df=raw_claims_df,
    path="Tables/bronze_claims",
    description=metadata["description"],  # "Bronze layer: Raw data from CSV"
    tags=metadata["tags"]  # {"layer": "bronze", "table": "bronze_claims", "source_system": "CSV", "immutable": "true"}
)
```

### Example 3: Gold Layer Features

```python
from framework.libs.delta_ops import write_delta
from framework.libs.purview_integration import PurviewMetadata

# Generate Gold metadata
metadata = PurviewMetadata.get_gold_metadata(
    table_name="gold_claims_features",
    feature_type="claims"
)

write_delta(
    df=claims_features_df,
    path="Tables/gold_claims_features",
    mode="overwrite",
    partition_by=["feature_date"],
    description=metadata["description"],  # "Gold layer: ML features for claims"
    tags=metadata["tags"]  # {"layer": "gold", "table": "gold_claims_features", "feature_type": "claims", "ml_ready": "true"}
)
```

### Example 4: Custom Tags

```python
from framework.libs.delta_ops import write_delta

# Custom tags for specific business logic
custom_tags = {
    "layer": "silver",
    "table": "silver_policies_enriched",
    "pii": "true",
    "sensitivity": "high",
    "retention_days": "2555",  # 7 years
    "compliance": "GDPR,HIPAA",
    "enriched_from": "Cosmos_DB"
}

write_delta(
    df=enriched_df,
    path="Tables/silver_policies_enriched",
    description="Silver layer: Policies enriched with Cosmos DB risk scores (PII, GDPR/HIPAA compliant)",
    tags=custom_tags
)
```

---

## Accessing Purview Hub

### In Microsoft Fabric Workspace

1. Navigate to workspace: **Insurance-ML-Platform**
2. Click **Purview Hub** in left navigation
3. Explore:
   - **Data Catalog**: Browse all Delta tables
   - **Lineage**: View data flow diagrams
   - **Search**: Find assets by name, tags, or metadata

### Example Lineage Flow

Purview automatically captures:

```
CSV Files → bronze_policies → silver_policies → gold_claims_features
                                     ↓
                              silver_policies_enriched (← Cosmos DB)
```

**Visual Lineage**: Available in Purview Hub → Lineage tab

---

## Migration from Existing Notebooks

### Before (No Purview Metadata)

```python
from framework.libs.delta_ops import write_delta

write_delta(
    df=cleaned_df,
    path="Tables/silver_policies",
    mode="overwrite"
)
```

### After (With Purview Metadata)

```python
from framework.libs.delta_ops import write_delta
from framework.libs.purview_integration import PurviewMetadata

metadata = PurviewMetadata.get_silver_metadata("silver_policies", has_scd2=True, pii=True)

write_delta(
    df=cleaned_df,
    path="Tables/silver_policies",
    mode="overwrite",
    description=metadata["description"],
    tags=metadata["tags"]
)
```

**No breaking changes** - `description` and `tags` are optional parameters.

---

## Best Practices

### 1. Always Add Metadata to Tables

```python
# ❌ Bad: No metadata
write_delta(df, "Tables/silver_policies")

# ✅ Good: Include metadata
metadata = PurviewMetadata.get_silver_metadata("silver_policies", has_scd2=True)
write_delta(df, "Tables/silver_policies", description=metadata["description"], tags=metadata["tags"])
```

### 2. Mark PII Tables

```python
# For tables with PII data (customer_id, email, etc.)
metadata = PurviewMetadata.get_silver_metadata("silver_customers", pii=True)
```

### 3. Document Feature Types

```python
# For Gold layer features, specify feature type
metadata = PurviewMetadata.get_gold_metadata("gold_risk_features", feature_type="risk")
```

### 4. Use Custom Tags for Business Logic

```python
# Add compliance, retention, or business domain tags
tags = {
    "layer": "silver",
    "compliance": "GDPR",
    "retention_days": "2555",
    "business_unit": "Underwriting"
}
```

---

## Monitoring Dashboards (Retained)

Purview **does not replace** custom monitoring dashboards:

### Retained Dashboards

1. **`monitoring/dashboards/data_quality_dashboard.json`**
   - Purpose: Custom DQ monitoring (null checks, duplicates, freshness)
   - Why: Purview has no custom DQ rules

2. **`monitoring/dashboards/pipeline_performance_dashboard.json`**
   - Purpose: Pipeline execution metrics (success rate, duration, lag)
   - Why: Purview has no pipeline performance tracking

### Purview Complementary Features

- **Data Catalog**: Search and browse tables
- **Lineage**: Visualize data flow
- **Metadata**: View schema and tags

---

## Troubleshooting

### Q: Why don't I see lineage in Purview Hub?

**A**: Lineage is auto-captured during notebook execution. Ensure:
1. Notebooks have run at least once
2. Delta tables are written with `write_delta()` or `merge_delta()`
3. Refresh Purview Hub (may take 5-10 minutes to sync)

### Q: Can I add column-level descriptions?

**A**: Yes, but requires Spark table properties:

```python
spark.sql(f"""
    ALTER TABLE delta.`Tables/silver_policies` 
    SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql(f"""
    ALTER TABLE delta.`Tables/silver_policies` 
    CHANGE COLUMN policy_id COMMENT 'Unique policy identifier (PII)'
""")
```

Purview syncs column comments automatically.

### Q: How do I classify sensitive data?

**A**: Use Purview UI:
1. Purview Hub → Data Catalog → Select table
2. Click "Classify"
3. Apply classifications (e.g., "Personal Information", "Financial Data")

---

## Summary

| Feature | Purview Hub | Custom Dashboards |
|---------|-------------|-------------------|
| **Data Catalog** | ✅ Auto-enabled | ❌ N/A |
| **Lineage Tracking** | ✅ Auto-enabled | ❌ N/A |
| **Custom DQ Rules** | ❌ Not supported | ✅ Required |
| **Pipeline Metrics** | ❌ Not supported | ✅ Required |
| **Setup Required** | ❌ None (auto-enabled) | ✅ Already configured |

**Decision**: Use **both systems** for comprehensive governance and operations.
