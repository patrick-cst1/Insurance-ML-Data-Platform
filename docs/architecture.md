# Architecture Overview

## Medallion 架構設計

### Bronze Layer（原始數據層）
- **目的**：保存原始數據，不可變、append-only
- **數據源**：
  - Batch：CSV 文件（policies、claims、customers、agents）
  - Streaming：Eventstream 實時事件（claims submissions、customer interactions）
- **存儲格式**：Delta Lake（支援 ACID、time travel）
- **Partitioning**：按 `ingestion_date` 分區
- **Schema**：原始欄位 + metadata（`ingestion_timestamp`、`source_system`）

### Silver Layer（清洗與豐富層）
- **目的**：清洗、標準化、去重、豐富數據
- **處理邏輯**：
  - 數據清洗：去除 nulls、標準化字符串、類型轉換
  - 去重：基於業務鍵（policy_id、customer_id）
  - SCD Type 2：維度表歷史追蹤（`is_current`、`effective_from`、`effective_to`）
  - Cosmos DB Enrichment：外部風險評分、承保標記
- **數據質量門檻**：
  - Null 比例 < 1%（關鍵欄位）
  - 無重複記錄
  - Schema 驗證通過

### Gold Layer（ML 特徵層）
- **目的**：構建 ML-ready 特徵，優化查詢性能
- **特徵類型**：
  - **Claims Features**：30/90/365 天窗口聚合（count、sum、avg、max）
  - **Customer Features**：lifetime value、tenure、policy count
  - **Risk Features**：綜合風險評分（Cosmos + claims history）
- **Point-in-time 正確性**：
  - 每個特徵包含 `feature_timestamp`、`load_timestamp`
  - 支援 as-of join（配合 SCD2 `valid_from/valid_to`）
- **存儲優化**：按 `feature_timestamp` 分區、OPTIMIZE compaction

## Streaming 架構（Dual-Sink）

### Eventstream 配置
- **來源**：Custom Application endpoint（模擬實時事件）
- **輸出 1**：KQL Database（低延遲查詢，< 10ms）
- **輸出 2**：Lakehouse Bronze Delta（可回溯、支援批處理）

### KQL Database 用途
- 實時監控儀表板（streaming lag、吞吐量）
- 低延遲特徵查詢（online inference）
- Materialized views（1hr/24hr 窗口聚合）

### 數據合流策略
- Bronze Delta 與 KQL 事件對齊驗證
- Silver 層統一處理（去重/validation）
- Gold 層批處理與實時特徵分離但可合併

## Cosmos DB 整合

### 容器設計
- **policy-enrichment**：partition key `/policyId`
- **customer-risk-profiles**：partition key `/customerId`

### 讀取策略
- Spark Cosmos DB connector（批量讀取）
- Broadcast join（小資料集優化）
- 點讀取（按 partition key + id）

## CI/CD 流程

### Git 集成
- **源碼倉庫**：GitHub（代碼協作）
- **Fabric Git 集成**：Azure DevOps Git（官方支援最佳）
- **策略**：GitHub 作主倉，Azure DevOps 鏡像

### Deployment Pipeline
```
Dev Workspace → Test Workspace → Prod Workspace
     ↑              ↑                ↑
  自動觸發      手動審批         手動審批 + 回滾能力
```

### 環境參數化
- `devops/parameters/dev.yml`：Dev workspace IDs、Cosmos endpoints
- `devops/parameters/prod.yml`：Prod 配置
- Secrets 存於 Azure Key Vault，透過 Variable Groups 注入

## 安全與治理

### 數據治理
- **Microsoft Purview**：掃描、血緣追蹤、數據目錄
- **Schema Contracts**：YAML 定義，自動驗證

### 安全措施
- **Key Vault**：Cosmos 連線字串、Eventstream endpoints
- **RBAC**：Workspace 級別權限（Bronze 寫入、Gold 只讀）
- **RLS/Masking**：Lakehouse SQL Endpoint 敏感欄位遮蔽

### 數據質量
- **Great Expectations**：分層驗證套件
- **自定義規則**：`data_quality_rules.yaml`
- **失敗處理**：阻斷或降級（視嚴重性）

## 監控與告警

### 指標
- Pipeline 執行時長、成功率
- 數據新鮮度（Gold 更新 SLA）
- Streaming lag、吞吐量
- 數據質量分數

### 告警
- Gold 數據未於 8AM 前更新
- Pipeline 失敗
- 數據質量低於閾值
- Streaming lag > 5 分鐘

## 可重用性設計

### Configuration-Driven
- 所有環境配置外部化（YAML）
- 無硬編碼 workspace IDs、連線字串

### Template-Based
- Notebook 模板可快速適配新數據源
- Pipeline JSON 參數化

### Modular Framework
- `framework/libs/` 可獨立使用
- 單一職責、清晰介面

## 性能優化

### Delta Lake
- Partitioning（按日期/狀態）
- OPTIMIZE + Z-ORDER（高基數欄位）
- Auto-compaction（小檔案合併）
- VACUUM（清理舊版本）

### Spark
- Broadcast join（小維度表）
- Cache 熱數據
- Partition pruning

### KQL
- Retention policy（限制數據保留期）
- Materialized views（預聚合）
- Caching policy（熱查詢加速）
