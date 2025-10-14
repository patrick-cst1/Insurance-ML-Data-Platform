# Purview Integration Deployment Checklist

## Pre-Deployment Verification

### ✅ Configuration Files Updated

- [x] **`devops/parameters/fabric.yml`**
  - Added `purview` section with enabled features
  - Configured custom properties (project, domain, criticality)

- [x] **`framework/libs/delta_ops.py`**
  - Updated `write_delta()` with `description` and `tags` parameters
  - Supports Purview metadata as Delta table properties

- [x] **`framework/libs/__init__.py`**
  - Exported `PurviewMetadata` class
  - Added to `__all__` list

- [x] **`devops/pipelines/azure-pipelines-cd.yml`**
  - Added Purview integration status output
  - Deployment summary includes Purview enablement

- [x] **`README.md`**
  - Added Purview Hub section
  - Documented Purview vs Custom Monitoring
  - Updated Key Features table

### ✅ New Files Created

- [x] **`framework/libs/purview_integration.py`**
  - `PurviewMetadata` helper class
  - Standard metadata generators for all medallion layers

- [x] **`framework/docs/purview_integration_guide.md`**
  - Comprehensive usage guide
  - Migration examples
  - Best practices

- [x] **`framework/docs/purview_deployment_checklist.md`**
  - This file

---

## Deployment Steps

### Step 1: Review Changes

```bash
# Review all modified files
git status

# Expected changes:
# - devops/parameters/fabric.yml
# - framework/libs/delta_ops.py
# - framework/libs/__init__.py
# - framework/libs/purview_integration.py (new)
# - devops/pipelines/azure-pipelines-cd.yml
# - README.md
# - framework/docs/purview_integration_guide.md (new)
# - framework/docs/purview_deployment_checklist.md (new)
```

### Step 2: Validate Configuration

```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('devops/parameters/fabric.yml'))"

# Expected output: No errors
```

### Step 3: Deploy to Fabric

```bash
# Option A: Azure DevOps Pipeline
git add .
git commit -m "feat: Integrate Microsoft Purview Hub for data governance"
git push origin main

# Option B: Manual deployment
# Upload files to Fabric workspace via Fabric UI
```

### Step 4: Verify Purview Hub Access

1. Navigate to Fabric workspace: **Insurance-ML-Platform**
2. Click **Purview Hub** in left navigation
3. Verify access granted (should be auto-enabled)

### Step 5: Test Metadata Sync

Run test notebook in Fabric:

```python
from framework.libs.delta_ops import write_delta
from framework.libs.purview_integration import PurviewMetadata
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Create test DataFrame
test_df = spark.createDataFrame([(1, "test")], ["id", "value"])

# Write with Purview metadata
metadata = PurviewMetadata.get_silver_metadata("test_purview_table", pii=False)
write_delta(
    df=test_df,
    path="Tables/test_purview_table",
    mode="overwrite",
    description=metadata["description"],
    tags=metadata["tags"]
)

print("✅ Test table created with Purview metadata")
```

### Step 6: Verify in Purview Hub

1. Purview Hub → Data Catalog
2. Search: `test_purview_table`
3. Verify:
   - ✅ Description: "Silver layer: Cleaned and validated test_purview_table"
   - ✅ Tags: layer=silver, table=test_purview_table

### Step 7: Verify Lineage Tracking

1. Run any existing notebook (e.g., `lakehouse/silver/notebooks/clean_policies.py`)
2. Purview Hub → Lineage
3. Verify lineage captured:
   - ✅ `bronze_policies` → `clean_policies.py` → `silver_policies`

---

## Post-Deployment Verification

### ✅ Purview Hub Features

- [ ] **Data Catalog**: All Delta tables auto-registered
  - Bronze tables: `bronze_policies`, `bronze_claims`, `bronze_customers`, `bronze_agents`, `bronze_realtime_events`
  - Silver tables: `silver_policies`, `silver_claims`, `silver_customers`, `silver_agents`, `silver_policies_enriched`, `silver_realtime_claims`
  - Gold tables: `gold_claims_features`, `gold_customer_features`, `gold_risk_features`, `gold_streaming_features`

- [ ] **Lineage Tracking**: Data flow visualized
  - Batch flow: CSV → Bronze → Silver → Gold
  - Streaming flow: Eventstream → KQL DB + Bronze → Silver → Gold
  - Enrichment flow: Cosmos DB → Silver (enriched tables)

- [ ] **Metadata Sync**: Schema contracts visible in Purview

### ✅ Custom Monitoring Dashboards (Retained)

- [ ] **Data Quality Dashboard**: `monitoring/dashboards/data_quality_dashboard.json`
  - Functional and accessible in Fabric
  - Custom DQ rules working (null checks, duplicates, freshness)

- [ ] **Pipeline Performance Dashboard**: `monitoring/dashboards/pipeline_performance_dashboard.json`
  - Pipeline metrics tracking (success rate, duration)
  - Streaming lag monitoring

---

## Known Limitations

### Purview Cannot Replace

1. **Custom DQ Rules** ❌
   - Purview only provides basic profiling (row count, column stats)
   - Custom validators required: `framework/libs/data_quality.py`
   - Great Expectations integration required: `framework/libs/great_expectations_validator.py`

2. **Pipeline Performance Metrics** ❌
   - Purview has no pipeline execution tracking
   - Custom dashboard required: `monitoring/dashboards/pipeline_performance_dashboard.json`

3. **Real-time Streaming Metrics** ❌
   - Purview has no streaming lag monitoring
   - KQL queries required for real-time metrics

---

## Rollback Plan

If issues arise:

```bash
# Revert changes
git revert <commit-hash>
git push origin main

# Re-deploy previous version
# Azure DevOps pipeline will auto-deploy
```

**Note**: Purview integration is **additive only**. Rollback removes metadata enrichment but does not break existing functionality.

---

## Support

### Documentation

- **Integration Guide**: `framework/docs/purview_integration_guide.md`
- **README Section**: See "Data Governance with Purview Hub"

### Troubleshooting

1. **Lineage not showing**:
   - Wait 5-10 minutes for Purview sync
   - Ensure notebooks have run at least once
   - Refresh Purview Hub

2. **Metadata not syncing**:
   - Verify `description` and `tags` parameters in `write_delta()` calls
   - Check Delta table properties: `DESCRIBE DETAIL delta.`Tables/silver_policies``

3. **Purview Hub not accessible**:
   - Verify workspace permissions (Contributor or Admin required)
   - Contact Fabric workspace admin

---

## Success Criteria

✅ **Deployment successful if**:

1. Purview Hub accessible in Fabric workspace
2. Test table created with metadata (Step 5)
3. Lineage visible for at least 1 notebook (Step 7)
4. Custom monitoring dashboards still functional
5. No deployment errors in Azure DevOps pipeline

---

## Next Steps

### Optional Enhancements

1. **Update existing notebooks** to use `PurviewMetadata` helpers
   - Recommended: Update all `write_delta()` calls
   - Priority: Silver and Gold layer notebooks

2. **Add column-level descriptions** via Spark SQL
   - Example: `ALTER TABLE ... CHANGE COLUMN ... COMMENT '...'`

3. **Configure Purview classifications** via UI
   - Apply sensitivity labels (PII, Financial, etc.)
   - Set up data access policies

4. **Enable Purview scanning** (if using external Azure Purview)
   - Connect Fabric workspace to Azure Purview account
   - Configure scan schedules

---

**Deployment Date**: _______________  
**Deployed By**: _______________  
**Sign-off**: _______________
