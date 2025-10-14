# Data Dictionary

## Bronze Layer Tables

### bronze_policies
原始保單數據，不可變。
| Column | Type | Description | Source |
|--------|------|-------------|--------|
| policy_id | string | 保單唯一標識 | CSV |
| customer_id | string | 客戶 ID | CSV |
| product_type | string | 產品類型 | CSV |
| premium | double | 保費金額 | CSV |
| start_date | date | 保單開始日期 | CSV |
| end_date | date | 保單結束日期 | CSV |
| status | string | 保單狀態（ACTIVE/EXPIRED） | CSV |
| ingestion_timestamp | timestamp | 攝取時間戳 | System |
| ingestion_date | date | 攝取日期（分區鍵） | System |
| source_system | string | 數據來源系統 | System |

**Refresh Frequency**: Daily, 2AM UTC  
**Partition**: `ingestion_date`  
**Retention**: Unlimited（append-only）

### bronze_claims
原始理賠記錄。
| Column | Type | Description |
|--------|------|-------------|
| claim_id | string | 理賠唯一標識 |
| policy_id | string | 關聯保單 ID |
| claim_date | date | 理賠日期 |
| claim_amount | double | 理賠金額 |
| claim_status | string | 理賠狀態 |
| claim_type | string | 理賠類型 |
| ingestion_timestamp | timestamp | 攝取時間戳 |
| ingestion_date | date | 攝取日期（分區） |
| source_system | string | 數據來源 |

**Refresh Frequency**: Daily  
**Partition**: `ingestion_date`

### bronze_customers
原始客戶主數據。
| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | 客戶唯一標識 |
| name | string | 客戶姓名 |
| age | int | 年齡 |
| gender | string | 性別 |
| location | string | 地理位置 |
| join_date | date | 加入日期 |
| ingestion_timestamp | timestamp | 攝取時間戳 |
| ingestion_date | date | 攝取日期 |
| source_system | string | 數據來源 |

**Refresh Frequency**: Daily  
**Partition**: `ingestion_date`

### bronze_realtime_events
實時事件流（dual-sink from Eventstream）。
| Column | Type | Description |
|--------|------|-------------|
| event_time | timestamp | 事件時間 |
| claim_id | string | 理賠 ID |
| policy_id | string | 保單 ID |
| amount | double | 金額 |
| status | string | 狀態 |
| delta_ingestion_timestamp | timestamp | Delta 攝取時間 |
| ingestion_date | date | 分區鍵 |

**Refresh Frequency**: Continuous（micro-batches）  
**Partition**: `ingestion_date`

---

## Silver Layer Tables

### silver_policies
清洗、標準化後嘅保單數據，含 SCD Type 2。
| Column | Type | Description |
|--------|------|-------------|
| policy_id | string | 保單 ID（業務鍵） |
| customer_id | string | 客戶 ID |
| product_type | string | 產品類型（標準化大寫） |
| premium | double | 保費（驗證 > 0） |
| start_date | date | 保單開始日期 |
| end_date | date | 保單結束日期 |
| status | string | 狀態（標準化） |
| effective_from | timestamp | SCD2 有效起始時間 |
| effective_to | timestamp | SCD2 有效結束時間（NULL = 當前） |
| is_current | boolean | 是否當前有效記錄 |
| ingestion_timestamp | timestamp | 原始攝取時間 |
| ingestion_date | date | 分區鍵 |

**Refresh Frequency**: Daily after Bronze  
**Data Quality**: Null < 1%, No duplicates  
**SCD Type**: Type 2（歷史追蹤）

### silver_policies_enriched
Silver policies + Cosmos DB enrichment。
| Column | Type | Description |
|--------|------|-------------|
| *(All from silver_policies)* | - | - |
| risk_score | int | 外部風險評分（來自 Cosmos） |
| underwriting_flags | array<string> | 承保標記 |
| external_rating | string | 外部評級 |

**Refresh Frequency**: Daily after Silver  
**Cosmos Container**: `policy-enrichment`

### silver_claims
清洗後嘅理賠數據。
| Column | Type | Description |
|--------|------|-------------|
| claim_id | string | 理賠 ID |
| policy_id | string | 保單 ID |
| claim_date | date | 理賠日期 |
| claim_amount | double | 金額（驗證 > 0） |
| claim_status | string | 狀態（標準化） |
| claim_type | string | 類型（標準化） |

**Refresh Frequency**: Daily

### silver_customers
清洗後嘅客戶數據，含 SCD2。
| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | 客戶 ID |
| name | string | 姓名 |
| age | int | 年齡 |
| gender | string | 性別 |
| location | string | 位置 |
| join_date | date | 加入日期 |
| effective_from | timestamp | SCD2 有效起始 |
| effective_to | timestamp | SCD2 有效結束 |
| is_current | boolean | 當前記錄標記 |

**Refresh Frequency**: Daily

---

## Gold Layer Tables (ML Features)

### gold_claims_features
客戶級別嘅理賠特徵（時間窗口聚合）。
| Column | Type | Description | Calculation |
|--------|------|-------------|-------------|
| customer_id | string | 客戶 ID | - |
| claim_amount_sum_30d | double | 30天理賠總額 | SUM(claim_amount) WHERE claim_date >= current_date - 30 |
| claim_amount_avg_30d | double | 30天平均理賠額 | AVG(claim_amount) |
| claim_amount_max_30d | double | 30天最大理賠額 | MAX(claim_amount) |
| claim_amount_count_30d | long | 30天理賠次數 | COUNT(claim_id) |
| claims_count_30d | long | 30天理賠數 | COUNT(DISTINCT claim_id) |
| *(同理 90d, 365d 窗口)* | - | - | - |
| days_since_last_claim | int | 距上次理賠天數 | DATEDIFF(current_date, MAX(claim_date)) |
| feature_timestamp | timestamp | 特徵計算時間（PIT join 用） | - |
| load_timestamp | timestamp | 載入時間 | - |

**Refresh Frequency**: Daily after Silver  
**Partition**: `feature_timestamp`  
**Point-in-time**: 支援 as-of join

### gold_customer_features
客戶維度特徵。
| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | 客戶 ID |
| total_policies | long | 總保單數 |
| active_policies | long | 活躍保單數 |
| avg_premium | double | 平均保費 |
| total_premium | double | 總保費 |
| customer_tenure_days | int | 客戶任期（天） |
| customer_lifetime_value | double | 客戶終身價值（估算） |
| feature_timestamp | timestamp | 特徵時間 |
| load_timestamp | timestamp | 載入時間 |

**Refresh Frequency**: Daily

### gold_risk_features
綜合風險評估特徵。
| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | 客戶 ID |
| policy_id | string | 保單 ID |
| overall_risk_score | double | 綜合風險評分 = Cosmos risk_score + claims_count * 5 |
| high_value_claim_ratio | double | 高額理賠比例 = max_claim / sum_claim |
| risk_score | int | Cosmos 外部評分 |
| underwriting_flags | array<string> | 承保標記 |
| external_rating | string | 外部評級 |
| feature_timestamp | timestamp | 特徵時間 |
| load_timestamp | timestamp | 載入時間 |

**Refresh Frequency**: Daily after Claims & Customer features

---

## Streaming Tables (KQL Database)

### realtime_claims_events
即時理賠事件（KQL 表）。
| Column | Type | Description |
|--------|------|-------------|
| event_time | datetime | 事件時間 |
| claim_id | string | 理賠 ID |
| policy_id | string | 保單 ID |
| amount | double | 金額 |
| status | string | 狀態 |

**Refresh**: Real-time stream  
**Retention**: 30 days  
**Use Case**: Monitoring、low-latency queries

### gold_realtime_features (KQL Materialized View)
即時聚合特徵。
| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | 客戶 ID |
| claims_submitted_last_1hr | long | 過去1小時提交理賠數 |
| claims_submitted_last_24hr | long | 過去24小時提交理賠數 |
| avg_claim_amount_realtime | double | 實時平均理賠額 |

**Refresh**: Continuous（materialized view）  
**Use Case**: Online inference、real-time dashboards

---

## Notes
- **Point-in-time Join 範例**：
  ```sql
  SELECT f.*, c.name
  FROM gold_claims_features f
  JOIN silver_customers c
    ON f.customer_id = c.customer_id
   AND f.feature_timestamp >= c.effective_from
   AND (c.effective_to IS NULL OR f.feature_timestamp < c.effective_to)
  WHERE c.is_current = true
  ```
- **Data Lineage**: 可透過 Microsoft Purview 追蹤
- **Schema Evolution**: Delta Lake 支援 `mergeSchema` 自動適應
