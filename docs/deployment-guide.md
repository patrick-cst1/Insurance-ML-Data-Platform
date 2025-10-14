# Deployment Guide

## Fabric Workspaces & Lakehouses (p2)
- **建立 Workspace**：為 `Insurance-Dev`、`Insurance-Test`、`Insurance-Prod` 各建 1 個 Microsoft Fabric Workspace。
- **Lakehouse 結構**：每個 Workspace 內建立 `lh_bronze`、`lh_silver`、`lh_gold`。
- **權限**：以環境為界實施 RBAC，Bronze 讀寫權限與 Gold 的查閱權限分離。

## Eventstream 與 KQL Dual-Sink (p3)
- **建立 Eventstream**：命名 `es_insurance_realtime`，來源可連接模擬器或實際事件源。
- **KQL Database**：於各 Workspace 建 `kql_insurance_realtime_{env}`，Eventstream 輸出之一對應 KQL 表。
- **Lakehouse Sink**：同一 Eventstream 配置第二個輸出至 `lh_bronze` Delta 表，保持原始事件可回溯。

## Cosmos DB 與 Key Vault (p4)
- **Cosmos 帳戶**：使用 NoSQL API，建立 `policy-enrichment`、`customer-risk-profiles` 容器並啟用 Analytical store。
- **密鑰管理**：將 Cosmos 連線字串與 Eventstream endpoint 等 secret 存入 Azure Key Vault。
- **Variable Group**：於 Azure DevOps 建立 `Fabric-Dev-Secrets`、`Fabric-Prod-Secrets`，並連結 Key Vault。

## CI/CD Pipeline (p18-p19)
- **CI**：`devops/pipelines/azure-pipelines-ci.yml` 對應 PR 驗證（lint、tests、config schema）。
- **Deployment pipeline**：Fabric 內建 Deployment pipeline 作環境推進，使用 Azure DevOps pipeline (`azure-pipelines-cd-dev.yml` → `azure-pipelines-cd-prod.yml`) 觸發並注入參數。
- **審批與回滾**：在 Prod stage 加入手動審批，部署後執行 smoke tests，失敗時觸發回滾腳本。
