# Insurance ML Data Platform

<div align="center">

![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-164F94?style=for-the-badge&logo=microsoft&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**Simplified Dual Medallion Architecture (Batch + Event) for ML Workflows on Microsoft Fabric**

[Features](#-key-features) •
[Architecture](#%EF%B8%8F-architecture) •
[Quick Start](#-quick-start) •
[Project Structure](#-project-structure)

</div>

---

## 📋 Project Goals

This repository provides a **simplified, demonstration-focused** Medallion architecture for insurance ML workflows on Microsoft Fabric:

- ✅ **Fabric Native** - 100% Fabric services (Lakehouse + KQL + Eventstream)
- ✅ **Dual Medallion Pattern** - Batch (CSV) + Event (real-time) pipelines
- ✅ **SCD Type 2** - Simplified historical tracking in Silver layer
- ✅ **Schema Validation** - YAML-based contracts with inline enforcement
- ✅ **Purview Integration** - Basic metadata tagging for data catalog
- ✅ **No Framework Dependencies** - All code inline in notebooks
- ✅ **Git Integration Ready** - Deploy via Fabric Git Integration

## 🚀 Key Features

| Feature | Description | Technology |
|---------|-------------|------------|
| **Batch Medallion** | CSV → Bronze (strings) → Silver (typed) → Gold (features) | Delta Lake, PySpark |
| **Event Medallion** | Eventstream → KQL + Bronze → Silver → Gold (real-time) | Eventstream, KQL Database, Delta Lake |
| **Bronze Raw Storage** | All columns stored as strings (inferSchema=false) | CSV Reader |
| **Silver Type Casting** | Schema-driven: YAML defines types, transformations, validations | PySpark cast functions |
| **Bronze Schema Validation** | YAML contracts validate column existence only | PyYAML |
| **Silver Schema Validation** | YAML contracts define types, nullable, validation rules | PyYAML |
| **Process Tracking** | Unique process_id & source_file_name per ingestion run | UUID, input_file_name() |
| **Data Quality Metrics** | Pass rate logging (total, cleaned, dropped records) | PySpark inline logging |
| **SCD Type 2** | Simplified historical tracking (effective_from, effective_to, is_current) | Delta Lake |
| **Time-series Features** | Monthly claims aggregation for trend analysis | PySpark date functions |
| **Dual Sink** | Real-time events → KQL (queries) + Lakehouse (ML) | Eventstream destinations |
| **Purview Metadata** | Table descriptions via Delta properties | Delta Lake options |
| **Deployment** | Fabric Git Integration (Azure DevOps or GitHub) | Fabric Git Integration |

## 🏗️ Architecture

### Medallion Design

```mermaid
graph LR
    A[CSV Files] -->|Batch Ingest| B[Bronze Lakehouse]
    E[Real-time Events] -->|Eventstream| F[KQL Database]
    E -->|Dual-Sink| B
    B -->|Clean & Validate| C[Silver Lakehouse]
    C -->|Feature Engineering| D[Gold Lakehouse]
    F -->|KQL Materialized Views| H[Real-time Agg]
    H -->|Hourly Sync| D
    D -->|DirectLake/SQL| I[Power BI]
    D -->|PySpark| J[ML Training]
    D -->|Optional Sync| K[Fabric Warehouse]
    K -->|T-SQL| L[BI Tools]
    
    style B fill:#CD853F
    style C fill:#C0C0C0
    style D fill:#FFD700
    style F fill:#20B2AA
    style K fill:#4169E1
```

### Data Flow Sequence

```mermaid
sequenceDiagram
    participant Source as Data Sources
    participant Bronze as Bronze Lakehouse
    participant KQL as KQL Database
    participant Silver as Silver Lakehouse
    participant Gold as Gold Lakehouse
    participant ML as ML Pipelines
    
    Source->>Bronze: 1. Ingest Raw Data
    Note over Bronze: Immutable, Append-Only
    Bronze->>Silver: 2. Clean & Validate
    Note over Silver: SCD Type 2, DQ Checks
    Silver->>Gold: 3. Feature Engineering
    Note over KQL: Real-time Aggregations<br/>(Materialized Views)
    KQL->>Gold: 4. Hourly Sync
    Note over Gold: Batch + Real-time Features
    Gold->>ML: 5. Serve Features
    Note over ML: Training & Inference
```

## 🔧 Key Components & Technology

| Component | Description | Technology Stack |
|-----------|-------------|------------------|
| **Lakehouse Storage** | Delta Lake tables with ACID transactions | Microsoft Fabric Lakehouse, Delta Lake 3.0 |
| **Data Processing** | Distributed ETL/ELT transformations | PySpark 3.5 (Fabric runtime) |
| **SQL Access** | SQL endpoint (automatic) + Warehouse (optional) | Lakehouse SQL Endpoint, Fabric Warehouse |
| **Streaming** | Real-time event ingestion & processing | Eventstream, KQL Database |
| **Orchestration** | Pipeline scheduling & dependencies | Fabric Data Pipelines (master pipelines) |
| **CI/CD** | Automated deployment automation | Azure DevOps, Fabric Deployment Pipelines |
| **Data Quality** | Dual validation system: standard validators (inline) + Great Expectations (gate) | Custom validators (6 functions), Great Expectations |
| **Secrets Management** | Secure credential storage | Azure Key Vault |
| **Monitoring** | Pipeline metrics & alerting | Fabric Monitoring Hub, KQL Queries |
| **Data Governance** | Data catalog, lineage tracking, metadata management | Microsoft Purview Hub (native Fabric integration) |

## 📂 Project Structure

```
Insurance-ML-Data-Platform/
│
├── lakehouse/                         # Medallion Notebooks (15 notebooks)
│   ├── bronze/notebooks/              # Raw data ingestion (4 batch notebooks)
│   │   ├── ingest_policies.py                 # Policies ingestion + process_id + source_file tracking
│   │   ├── ingest_claims.py                   # Claims ingestion + process_id + source_file tracking
│   │   ├── ingest_customers.py                # Customers ingestion + process_id + source_file tracking
│   │   └── ingest_agents.py                   # Agents ingestion + process_id + source_file tracking
│   │
│   ├── silver/notebooks/              # Data cleansing (5 notebooks)
│   │   ├── clean_policies.py                  # Clean + SCD Type 2 + DQ metrics
│   │   ├── clean_claims.py                    # Clean + SCD Type 2 + DQ metrics
│   │   ├── clean_customers.py                 # Clean + SCD Type 2 + DQ metrics
│   │   ├── clean_agents.py                    # Clean + SCD Type 2 + DQ metrics
│   │   └── process_realtime_claims.py         # Real-time claims processing + DQ metrics
│   │
│   └── gold/notebooks/                # ML features (5 notebooks)
│       ├── create_claims_features.py          # Batch claims aggregations
│       ├── create_customer_features.py        # Customer features
│       ├── create_risk_features.py            # Risk scores
│       ├── create_monthly_claims_summary.py   # Monthly time-series aggregation
│       └── sync_kql_to_gold.py                # KQL → Gold sync (hourly)
│
├── pipelines/                         # Orchestration (2 pipelines)
│   ├── master_batch_pipeline.json     # Batch: Bronze → Silver → Gold (daily)
│   └── master_realtime_pipeline.json  # Real-time: Eventstream → Silver → Gold (hourly)
│
├── streaming/                         # Event Medallion (Fabric Native)
│   ├── eventstream_claims_config.json # Eventstream dual-sink config
│   └── kql/
│       ├── create_realtime_table.kql  # KQL table + mapping
│       └── realtime_aggregations.kql  # Real-time queries
│
├── framework/config/schemas/          # Schema Contracts (YAML - organized by layer)
│   ├── bronze/                        # Bronze layer schemas (column existence only)
│   │   ├── bronze_policies.yaml
│   │   ├── bronze_claims.yaml
│   │   ├── bronze_customers.yaml
│   │   └── bronze_agents.yaml
│   └── silver/                        # Silver layer schemas (type casting & validation)
│       ├── silver_policies.yaml
│       ├── silver_claims.yaml
│       ├── silver_customers.yaml
│       ├── silver_agents.yaml
│       └── silver_claims_realtime.yaml    # For streaming data
│
├── devops/parameters/
│   └── fabric.yml                     # Fabric workspace config
│
├── samples/
│   ├── batch/                         # Batch CSV data (4 files)
│   │   ├── policies.csv
│   │   ├── claims.csv
│   │   ├── customers.csv
│   │   └── agents.csv
│   └── streaming/
│       └── realtime_claims_events.json # Sample event data
│
├── .gitignore
├── LICENSE
├── requirements.txt                   # PySpark + Delta Lake + PyYAML
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Azure subscription with Microsoft Fabric capacity
- Azure DevOps or GitHub repository (for Git Integration)

### Deployment via Fabric Git Integration

**Recommended Method** - Using Fabric Native Git Integration:

```bash
# 1. Create Fabric Workspace
# - Navigate to Fabric Portal
# - Create workspace: Insurance-ML-Platform
# - Create Lakehouses: lh_bronze, lh_silver, lh_gold
# - Create KQL Database: insurance_realtime (for event pipeline)

# 2. Setup Git Integration
# - Workspace Settings → Git Integration
# - Connect to Azure DevOps or GitHub
# - Select repository: Insurance-ML-Data-Platform
# - Branch: main
# - Click "Sync"

# 3. Push changes to Git
git add .
git commit -m "Update notebooks"
git push origin main

# 4. Fabric auto-syncs notebooks and pipelines
# - All .py notebooks in lakehouse/ folder sync automatically
# - Pipelines in pipelines/ folder sync automatically
# - KQL scripts manual run in KQL Database query editor
# - No manual deployment needed!
```

### Setup Sample Data & Files

1. **Upload Sample CSV Files + Schema YAMLs**
   ```bash
   # In Fabric Workspace → lh_bronze → Files
   # Create folders and upload:
   # 
   # Files/samples/batch/
   #   - policies.csv
   #   - claims.csv
   #   - customers.csv
   #   - agents.csv
   #
   # Files/config/schemas/
   #   - bronze_policies.yaml
   #   - bronze_claims.yaml
   #   - bronze_customers.yaml
   #   - bronze_agents.yaml
   ```

2. **Setup Event Pipeline (Optional)**
   ```bash
   # Follow setup steps in README (see "Eventstream Setup" section below)
   # 1. Create KQL Database: insurance_realtime
   # 2. Run KQL script: streaming/kql/create_realtime_table.kql
   # 3. Create Eventstream: claims_eventstream (dual sink)
   # 4. Test with sample: samples/streaming/realtime_claims_events.json
   ```

3. **Run Pipelines**
   ```bash
   # Batch Pipeline (Daily)
   # - Navigate to Pipelines → master_batch_pipeline
   # - Click "Run"
   # - Pipeline executes: Bronze → Silver → Gold
   #
   # Real-time Pipeline (Hourly)
   # - Navigate to Pipelines → master_realtime_pipeline
   # - Click "Run" (after Eventstream setup)
   # - Pipeline syncs: Eventstream → Silver → Gold
   #
   # See detailed trigger instructions below
   ```

4. **Verify Results**
   ```bash
   # Batch tables:
   # - lh_bronze: bronze_policies, bronze_claims, bronze_customers, bronze_agents
   #   (with process_id, source_file_name metadata)
   # - lh_silver: silver_* (with SCD2 columns + DQ metrics logged)
   # - lh_gold: gold_claims_features, gold_customer_features, gold_risk_features,
   #            gold_monthly_claims_summary (NEW)
   #
   # Real-time tables:
   # - lh_bronze: bronze_claims_events (Eventstream sink)
   # - lh_silver: silver_claims_realtime (with SCD2 + DQ metrics)
   # - lh_gold: gold_realtime_claims_features (KQL sync)
   # - KQL DB: claims_events, claims_hourly (materialized view)
   ```

## 📚 Code Examples

### 1. Bronze Layer: Raw String Storage (Best Practice)

All Bronze notebooks read data as strings to preserve raw data integrity:

```python
import uuid
from pyspark.sql.functions import input_file_name, lit

# Read all columns as strings (Medallion best practice)
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load(SOURCE_PATH)

# Generate unique process ID for pipeline run tracking
process_id = str(uuid.uuid4())

df_enriched = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("ingestion_date", to_date(current_timestamp())) \
    .withColumn("source_system", lit("legacy_csv")) \
    .withColumn("process_id", lit(process_id)) \
    .withColumn("source_file_name", input_file_name())
```

**Why String Storage in Bronze?**
- ✅ Prevents data loss from source schema changes
- ✅ Preserves raw data exactly as received
- ✅ Enables reprocessing without re-reading source
- ✅ Follows Medallion architecture best practices

**Metadata Columns Added:**
- `ingestion_timestamp`: Exact ingestion time
- `ingestion_date`: Partition column for date-based queries
- `source_system`: Data source identifier
- `process_id`: Unique UUID tracking pipeline run
- `source_file_name`: Full path of source file

### 2. Schema Validation (Column Existence Only)

Bronze YAML schemas validate column existence only (no type checking):

```yaml
# bronze_claims.yaml
# Validates column existence only (all data stored as strings)
# Type casting happens in Silver layer

table_name: bronze_claims
layer: bronze

required_columns:
  - name: claim_id
    nullable: false
  - name: claim_amount
    nullable: false
  # Note: No 'type' field - all stored as strings

metadata_columns:
  - ingestion_timestamp
  - process_id
  - source_file_name
```

**Validation Logic:**
```python
import yaml

def validate_schema(df, schema_path):
    """Validates column existence only."""
    with open(schema_path, 'r') as f:
        schema = yaml.safe_load(f)
    
    for col_def in schema['required_columns']:
        col_name = col_def['name']
        nullable = col_def['nullable']
        
        if col_name not in df.columns:
            raise ValueError(f"Missing required column: {col_name}")
        
        if not nullable:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                logger.warning(f"Found {null_count} nulls in {col_name}")
    
    logger.info("✓ Schema validation passed")
    return True
```

### 3. Silver Layer: Schema-Driven Type Casting

Silver YAML schemas define types, transformations, and validation rules:

```yaml
# silver_claims.yaml
business_columns:
  - name: claim_amount
    type: double
    nullable: false
    validation:
      - rule: greater_than
        value: 0
  
  - name: claim_status
    type: string
    nullable: true
    transformation: upper_trim  # Apply upper() + trim()
  
  - name: age
    type: integer
    nullable: false
    validation:
      - rule: greater_than
        value: 0
      - rule: less_than
        value: 120
```

**Schema-Driven Processing:**
```python
def apply_schema_transformations(df, schema_path):
    """Apply type casting based on silver schema YAML."""
    with open(schema_path, 'r') as f:
        schema = yaml.safe_load(f)
    
    for col_def in schema['business_columns']:
        # 1. Apply transformations (trim, upper_trim)
        if col_def.get('transformation') == 'upper_trim':
            df = df.withColumn(col_name, upper(trim(col(col_name))))
        
        # 2. Apply type casting
        if col_def['type'] == 'double':
            df = df.withColumn(col_name, col(col_name).cast("double"))
        
        # 3. Apply nullable filters
        if not col_def['nullable']:
            df = df.filter(col(col_name).isNotNull())
        
        # 4. Apply validation rules
        for rule in col_def.get('validation', []):
            if rule['rule'] == 'greater_than':
                df = df.filter(col(col_name) > rule['value'])
    
    return df

# Usage in Silver notebooks
df_cleaned = apply_schema_transformations(df_bronze, SCHEMA_PATH)
```

**Benefits:**
- ✅ Type casting logic defined in YAML, not hardcoded
- ✅ Easy to update validation rules without changing code
- ✅ Consistent transformation logic across all Silver notebooks
- ✅ Schema serves as documentation

### 4. SCD Type 2 Implementation (Simplified)

All Silver notebooks include basic SCD2 tracking:

```python
# Add SCD Type 2 columns
df_cleaned = df_cleaned \
    .withColumn("effective_from", col("ingestion_timestamp")) \
    .withColumn("effective_to", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))
```

### 5. Gold Layer: Monthly Time-series Aggregation

New monthly claims summary for trend analysis:

```python
from pyspark.sql.functions import year, month, date_trunc

# Filter current records only (SCD Type 2)
df_current = df_claims.filter(col("is_current") == True)

# Add year-month columns
df_with_period = df_current \
    .withColumn("claim_year", year(col("claim_date"))) \
    .withColumn("claim_month", month(col("claim_date"))) \
    .withColumn("claim_year_month", date_trunc("month", col("claim_date")))

# Monthly aggregations
monthly_summary = df_with_period.groupBy("claim_year_month", "claim_year", "claim_month").agg(
    count("claim_id").alias("total_claims"),
    sum("claim_amount").alias("total_claim_amount"),
    avg("claim_amount").alias("avg_claim_amount"),
    max("claim_amount").alias("max_claim_amount"),
    min("claim_amount").alias("min_claim_amount")
)
```

### 6. Purview Metadata Integration

Table descriptions are added via Delta table properties:

```python
# Write with Purview metadata
df_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .option("description", "Silver layer: Cleaned policies with SCD Type 2 tracking") \
    .save("Tables/silver_policies")

# Fabric automatically syncs to Purview Hub for data catalog
```

## 📖 Architecture Summary

Simplified dual Medallion architecture for demonstration:

### Core Patterns

1. **Dual Medallion Data Flow**
   
   **Batch Pipeline (Daily):**
   - **Bronze Layer**: CSV ingestion with schema validation + process tracking (4 notebooks)
   - **Silver Layer**: Data cleaning + SCD Type 2 + DQ metrics (4 notebooks)
   - **Gold Layer**: Feature aggregations + time-series analysis (4 notebooks)
   
   **Event Pipeline (Real-time):**
   - **Eventstream**: Dual-sink to KQL Database + Bronze Lakehouse
   - **KQL Database**: Low-latency real-time queries (<1s)
   - **Silver Layer**: Process events with SCD Type 2 + DQ metrics (1 notebook)
   - **Gold Layer**: Sync KQL aggregations hourly (1 notebook)

2. **Bronze Layer: String Storage (Best Practice)**
   - **All columns stored as strings**: `inferSchema=false` prevents data loss
   - **Raw data preservation**: Exactly as received from source systems
   - **Schema change resilience**: Source type changes don't break ingestion
   - **Reprocessing capability**: Can reprocess without re-reading source
   - Follows Medallion architecture best practices for immutable raw data

3. **Process Tracking & Data Lineage**
   - **process_id**: Unique UUID for each pipeline run
   - **source_file_name**: Full path of ingested file via `input_file_name()`
   - **ingestion_timestamp**: Exact ingestion time
   - **ingestion_date**: Partition column for time-based queries
   - Enables complete audit trail and reprocessing capabilities

4. **Schema Validation (Two-Layer Approach)**
   
   **Bronze Schemas** (`bronze/*.yaml`):
   - Validates column existence and nullable constraints only
   - **No type validation** (all data stored as strings)
   - Ensures source data has expected columns
   
   **Silver Schemas** (`silver/*.yaml`):
   - Defines target types (`string`, `int`, `double`, `date`)
   - Specifies transformations (`trim`, `upper_trim`)
   - Declares validation rules (`greater_than`, `less_than`)
   - Documents expected business logic
   
5. **Silver Layer: Schema-Driven Type Casting**
   - **YAML-driven transformations**: Type casting logic defined in silver schemas
   - **Reusable function**: `apply_schema_transformations(df, schema_path)`
   - **Safe type conversion**: Automatic casting based on schema definitions
   - **Invalid value filtering**: Schema-defined validation rules
   - **Consistent logic**: Same transformation function for batch & streaming
   - All type enforcement happens in Silver, not Bronze

6. **Streaming Schema Validation (NEW)**
   - **Real-time schema enforcement**: Streaming data follows same pattern as batch
   - **silver_claims_realtime.yaml**: Dedicated schema for Eventstream data
   - **Consistent DQ**: Same validation rules for real-time & batch pipelines
   - **Industry best practice**: Schema validation for streaming prevents bad data

7. **Data Quality Metrics**
   - **Pass rate logging**: Calculate % of records passing validation
   - **Dropped records tracking**: Monitor filtered records count
   - **Type casting failures**: Tracked in dropped count
   - **Real-time DQ metrics**: Applied to both batch and streaming pipelines
   - Format: "Total: X | Cleaned: Y | Dropped: Z | Pass Rate: W%"

8. **SCD Type 2 Tracking**
   - Simplified historical tracking with `effective_from`, `effective_to`, `is_current` columns
   - Applied to all Silver tables (batch + real-time)
   - Enables point-in-time queries and historical analysis

9. **Time-series Features (NEW)**
   - **Monthly claims aggregation**: Trend analysis by year-month
   - **Metrics**: total_claims, total_amount, avg_amount, max_amount, min_amount
   - **Gold table**: `gold_monthly_claims_summary`
   - Filters only current records (SCD Type 2 aware)

10. **Dual Sink Architecture**
   - **KQL Database**: Real-time monitoring dashboards
   - **Lakehouse**: Historical ML training data
   - Single Eventstream → both destinations

11. **Purview Integration**
   - Table descriptions via Delta `.option("description", "...")`
   - Auto-sync to Purview Hub for data catalog
   - Enhanced metadata for all layers

12. **Deployment**
   - Fabric Git Integration (recommended)
   - Auto-sync on git push
   - No manual deployment scripts needed

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📖 Pipeline Execution Guide

### Batch Pipeline Triggers

#### Required Files Location

```
Fabric Workspace: lh_bronze
├── Files/samples/batch/          ← Upload here
│   ├── policies.csv
│   ├── claims.csv
│   ├── customers.csv
│   └── agents.csv
└── Files/config/schemas/         ← Upload here
    ├── bronze_policies.yaml
    ├── bronze_claims.yaml
    ├── bronze_customers.yaml
    └── bronze_agents.yaml
```

#### Manual Execution

1. Navigate to Fabric Portal → Workspace → Pipelines
2. Click `master_batch_pipeline`
3. Click **Run** button
4. Monitor execution status in the Monitoring tab

#### Scheduled Execution (Daily)

1. Open `master_batch_pipeline`
2. Click **Triggers** tab
3. Toggle `DailySchedule` to **ON**
4. Click **Save**

**Result:** Pipeline runs automatically daily at 2:00 AM UTC

---

### Real-time Pipeline Triggers

#### Prerequisites

1. Create KQL Database: `insurance_realtime`
2. Run KQL script: `streaming/kql/create_realtime_table.kql`
3. Create Eventstream: `claims_eventstream` (dual sink to KQL + Lakehouse)
4. Test with sample: `samples/streaming/realtime_claims_events.json`

#### Manual Execution

1. Navigate to Fabric Portal → Pipelines
2. Click `master_realtime_pipeline`
3. Click **Run** button

#### Scheduled Execution (Hourly)

1. Open `master_realtime_pipeline`
2. Click **Triggers** tab
3. Toggle `HourlySchedule` to **ON**
4. Click **Save**

**Result:** Pipeline runs automatically every hour

---

## 🚀 Eventstream Setup

### Architecture

```
Real-time Claims Events
    ↓ Eventstream (Dual Sink)
    ├→ KQL Database (insurance_realtime) - Low-latency queries
    └→ Bronze Lakehouse (bronze_claims_events) - Historical data
         ↓
    Silver (process_realtime_claims.py - SCD Type 2)
         ↓
    Gold (sync_kql_to_gold.py - Hourly sync)
```

### Setup Steps

#### Step 1: Create KQL Database

1. In Fabric Workspace, click **New** → **KQL Database**
2. Name: `insurance_realtime`
3. Click **Create**
4. Open KQL Query Editor
5. Copy and paste: `streaming/kql/create_realtime_table.kql`
6. Run script → creates `claims_events` table + mapping

#### Step 2: Create Eventstream

1. Click **New** → **Eventstream**
2. Name: `claims_eventstream`
3. Click **Create**
4. Add Source: **Custom App** → name: `claims_events_source`
5. Copy connection string (save for testing)
6. Add Destination 1: **KQL Database**
   - Database: `insurance_realtime`
   - Table: `claims_events`
   - Data format: JSON
   - Mapping: `claims_mapping`
7. Add Destination 2: **Lakehouse**
   - Lakehouse: `lh_bronze`
   - Table: `bronze_claims_events`
   - Mode: Append
   - Data format: JSON
8. Click **Publish**

#### Step 3: Test Eventstream (Optional)

1. In Eventstream, go to **Test** tab
2. Upload sample: `samples/streaming/realtime_claims_events.json`
3. Click **Send events**
4. Verify data in:
   - KQL: `SELECT * FROM claims_events`
   - Lakehouse: `SELECT * FROM bronze_claims_events`

---

## 📋 Schema Contracts

All schema contracts are in `framework/config/schemas/*.yaml` with inline validation in Bronze notebooks

---

## 🚀 GitHub to Fabric Deployment (Step-by-Step)

### Prerequisites

- GitHub repository with this code
- Microsoft Fabric workspace
- Fabric capacity (F2 or higher)

### Deployment Steps

#### 1. Push Code to GitHub

```bash
# In your local repository
git add .
git commit -m "Initial commit: Insurance ML Data Platform"
git push origin main
```

#### 2. Create Fabric Workspace

1. Navigate to [Microsoft Fabric Portal](https://app.fabric.microsoft.com)
2. Click **Workspaces** → **New Workspace**
3. Name: `Insurance-ML-Platform`
4. Select Fabric capacity
5. Click **Apply**

#### 3. Create Fabric Resources

**Create Lakehouses:**
1. In workspace, click **New** → **Lakehouse**
2. Name: `lh_bronze` → Create
3. Repeat for: `lh_silver`, `lh_gold`

**Create KQL Database (for real-time pipeline):**
1. Click **New** → **KQL Database**
2. Name: `insurance_realtime` → Create

#### 4. Connect Fabric to GitHub

1. In workspace, click **Workspace settings** (gear icon)
2. Navigate to **Git integration**
3. Click **Connect**
4. Select **GitHub**
5. Authorize Fabric to access GitHub
6. Select:
   - Repository: `Insurance-ML-Data-Platform`
   - Branch: `main`
   - Git folder: `/` (root)
7. Click **Connect**

#### 5. Sync from GitHub

1. After connection, click **Source control** button (top bar)
2. Click **Sync** → **Update all**
3. Fabric will import:
   - All notebooks from `lakehouse/*/notebooks/*.py`
   - All pipelines from `pipelines/*.json`
   - KQL scripts (manual run required)

**Important:** Notebooks and pipelines auto-sync. KQL scripts must be run manually in KQL query editor.

#### 6. Upload Required Files

**Upload to lh_bronze Lakehouse:**

1. Open `lh_bronze` lakehouse
2. Click **Files** → **Upload** → **Upload files**
3. Create and upload:

```
Files/
├── samples/batch/
│   ├── policies.csv
│   ├── claims.csv
│   ├── customers.csv
│   └── agents.csv
└── config/schemas/
    ├── bronze_policies.yaml
    ├── bronze_claims.yaml
    ├── bronze_customers.yaml
    └── bronze_agents.yaml
```

**Note:** Upload files from your local `samples/` and `framework/config/schemas/` directories.

#### 7. Run KQL Scripts (for real-time pipeline)

1. Open `insurance_realtime` KQL database
2. Click **Query** tab
3. Copy content from `streaming/kql/create_realtime_table.kql`
4. Paste into query editor
5. Click **Run**
6. Verify table created: `claims_events`

#### 8. Test Batch Pipeline

1. Navigate to **Pipelines** → `master_batch_pipeline`
2. Click **Run**
3. Monitor execution (should complete in ~5-10 minutes)
4. Verify tables created:
   - `lh_bronze`: bronze_policies, bronze_claims, bronze_customers, bronze_agents
   - `lh_silver`: silver_* (with SCD2 columns)
   - `lh_gold`: gold_* features

#### 9. Setup Eventstream (Optional - for real-time pipeline)

Follow steps in **Eventstream Setup** section above.

#### 10. Enable Scheduled Triggers

**Batch Pipeline (Daily):**
1. Open `master_batch_pipeline`
2. **Triggers** tab → Toggle `DailySchedule` **ON**
3. Save

**Real-time Pipeline (Hourly):**
1. Open `master_realtime_pipeline`
2. **Triggers** tab → Toggle `HourlySchedule` **ON**
3. Save

---

## ⚠️ Troubleshooting

### Common Issues

#### Issue 1: Pipeline Fails at Bronze Ingestion

**Error:** `FileNotFoundError: Files/samples/batch/policies.csv`

**Solution:**
- Verify files uploaded to `lh_bronze/Files/samples/batch/`
- Check file paths match exactly (case-sensitive)
- Ensure files are in CSV format with headers

#### Issue 2: Schema Validation Fails

**Error:** `FileNotFoundError: /lakehouse/default/Files/config/schemas/bronze_policies.yaml`

**Solution:**
- Upload YAML schema files to `lh_bronze/Files/config/schemas/`
- Verify schema file names match exactly
- Check YAML syntax is valid

#### Issue 3: Notebooks Not Syncing from GitHub

**Solution:**
- Verify Git integration is connected (Workspace settings → Git integration)
- Click **Source control** → **Sync** → **Update all**
- Check notebook file names don't have special characters
- Ensure notebooks are in correct folder structure: `lakehouse/*/notebooks/*.py`

#### Issue 4: Pipeline Trigger Not Working

**Solution:**
- Open pipeline → **Triggers** tab
- Verify trigger is toggled **ON**
- Check trigger schedule (UTC timezone)
- Manually run pipeline once before enabling trigger

#### Issue 5: KQL Eventstream Not Receiving Data

**Solution:**
- Verify KQL table created: `SELECT * FROM claims_events`
- Check Eventstream status: should show "Running"
- Verify mapping name matches: `claims_mapping`
- Test with sample JSON in Eventstream **Test** tab

#### Issue 6: SCD Type 2 Columns Missing

**Solution:**
- Verify Silver notebooks have SCD2 code:
  ```python
  .withColumn("effective_from", col("ingestion_timestamp"))
  .withColumn("effective_to", lit(None).cast("timestamp"))
  .withColumn("is_current", lit(True))
  ```
- Re-run Silver pipeline
- Check `is_current = true` for latest records

#### Issue 7: Monthly Claims Summary Fails

**Error:** `AnalysisException: Column 'claim_date' not found`

**Solution:**
- Ensure Silver claims table has `claim_date` column (added in v1.1)
- Re-run Bronze ingestion to capture `claim_date` from source
- Check `clean_claims.py` includes: `.withColumn("claim_date", to_date(col("claim_date")))`
- Verify Bronze CSV has `claim_date` column

#### Issue 8: Process ID or Source File Name Missing

**Error:** Bronze tables missing `process_id` or `source_file_name` columns

**Solution:**
- Verify Bronze notebooks import: `import uuid` and `from pyspark.sql.functions import input_file_name`
- Check metadata enrichment code includes:
  ```python
  process_id = str(uuid.uuid4())
  .withColumn("process_id", lit(process_id))
  .withColumn("source_file_name", input_file_name())
  ```
- Re-run Bronze ingestion pipeline
- Update YAML schemas to include new metadata columns

### Pipeline Execution Checklist

**Before First Run:**
- [ ] All lakehouses created (lh_bronze, lh_silver, lh_gold)
- [ ] Sample CSV files uploaded to lh_bronze
- [ ] Schema YAML files uploaded to lh_bronze
- [ ] Git integration connected and synced
- [ ] Notebooks visible in workspace

**For Real-time Pipeline:**
- [ ] KQL database created (insurance_realtime)
- [ ] KQL script executed (create_realtime_table.kql)
- [ ] Eventstream created with dual sink
- [ ] Test data sent successfully

---

## 📌 Known Limitations (By Design - Demo Purposes)

This is a **simplified demonstration platform**. The following limitations are intentional for ease of use:

1. **SCD Type 2**: Simplified append-only with `is_current` flag (no merge logic for updates)
2. **Incremental Processing**: Full refresh mode for simplicity (not optimized for large-scale production)
3. **Data Quality**: Inline logging and basic checks only (no comprehensive DQ dashboard)
4. **Eventstream Setup**: Requires manual creation via Fabric UI (Fabric platform limitation - not Git-supported)
5. **Authentication**: Assumes Fabric workspace access (no explicit credential management)

**These are acceptable tradeoffs for a demonstration and learning platform** ✅

### ✨ Recent Enhancements (v1.2 - Schema-Driven Architecture)

The platform now follows production-grade schema-driven medallion patterns:

✅ **Organized Schema Structure**: `bronze/` and `silver/` subdirectories  
✅ **Schema-Driven Type Casting**: YAML defines types, transformations, validations  
✅ **Reusable Transformation Logic**: `apply_schema_transformations()` function  
✅ **Streaming Schema Validation**: Real-time data follows same pattern as batch  
✅ **Bronze String Storage**: All columns stored as strings (`inferSchema=false`)  
✅ **Process Tracking**: `process_id` + `source_file_name` for complete audit trail  
✅ **Data Quality Metrics**: Pass rate logging including type casting failures  
✅ **Time-series Analysis**: Monthly claims aggregation for trend analysis  

### For production use, consider:
- Implementing full Delta merge operations for SCD2
- Adding comprehensive data quality framework (e.g., Great Expectations)
- Implementing incremental processing with watermarks
- Setting up proper authentication and secrets management
- Creating real-time monitoring dashboards
- Implementing data quality gates and alerting

---

## 👥 Author

**Patrick Cheung**  
Simplified dual Medallion (Batch + Event) demonstration platform for Microsoft Fabric

---

<div align="center">

**[⬆ Back to Top](#insurance-ml-data-platform)**

Simplified Demo | Batch + Event Medallion | Schema Validation | SCD Type 2 | Purview Ready | 100% Fabric Native

</div>
