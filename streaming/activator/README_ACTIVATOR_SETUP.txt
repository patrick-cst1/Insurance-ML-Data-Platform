===========================================
Fabric Activator Setup Instructions
Author: Patrick Cheung
===========================================

Fabric Activator 係 no-code real-time alerting service，唔需要寫 Python code。

📋 SETUP STEPS
===========================================

1. 喺 Fabric Workspace 建立 Reflex (Activator)
   -------------------------------------------
   a) 去 Fabric workspace: Insurance-ML-Platform
   b) Click "+ New" → "Reflex" (under Real-Time Intelligence)
   c) Name: "HighValueClaimsActivator"
   d) Description: "Real-time alerting for high-value claims"

2. Connect Data Source
   -------------------------------------------
   a) Click "Get data"
   b) Select "Eventstream"
   c) Choose: "es_insurance_realtime"
   d) Select fields to monitor:
      - claim_id
      - policy_id
      - amount
      - status
      - event_time

3. Configure Triggers (3 個 alerts)
   -------------------------------------------
   
   TRIGGER 1: High-Value Claims
   - Name: "HighValueClaimDetected"
   - Condition: amount > 50000
   - Action: Email to claims-team@insurance.com
   - Subject: "🚨 High-Value Claim Alert"
   
   TRIGGER 2: Urgent Status
   - Name: "UrgentClaimStatus"  
   - Condition: status == "URGENT"
   - Action: Email to urgent-claims@insurance.com
   - Subject: "⚡ Urgent Claim Notification"
   
   TRIGGER 3: Potential Fraud
   - Name: "MultipleClaimsSamePolicy"
   - Condition: COUNT(policy_id) > 3 in 1 hour
   - Action: Email to fraud-detection@insurance.com + Webhook
   - Subject: "🔍 Potential Fraud Alert"

4. Configure Actions
   -------------------------------------------
   Available action types:
   ✅ Email notification
   ✅ Microsoft Teams message
   ✅ Power Automate flow
   ✅ Webhook (call external API)
   ✅ Fabric item (trigger another pipeline)

5. Test & Activate
   -------------------------------------------
   a) Click "Test" to validate triggers
   b) Send sample event to Eventstream
   c) Verify alert is triggered
   d) Click "Activate" to enable real-time monitoring

📊 MONITORING
===========================================

View Activator metrics:
- Workspace → Reflex → "HighValueClaimsActivator"
- Metrics:
  * Triggers fired (count)
  * Actions executed
  * Failed alerts
  * Latency (event to alert)

📝 CONFIGURATION FILE
===========================================

JSON config: streaming/activator/high_value_claims_reflex.json

呢個 file 係 reference only，Fabric Activator 係 no-code UI setup。
JSON file 用嚟 document configuration for version control。

🔧 ADVANCED FEATURES
===========================================

1. Custom Properties
   - Derive new fields from event data
   - Example: risk_level = IF(amount > 100000, "HIGH", "MEDIUM")

2. Multiple Conditions
   - Combine conditions with AND/OR
   - Example: amount > 50000 AND status == "PENDING"

3. Scheduled Checks
   - Check condition on schedule (e.g., daily summary)
   - Example: Daily email with all high-value claims

4. Historical Context
   - Access previous events for comparison
   - Example: Alert if amount > average of last 7 days

🚀 BENEFITS
===========================================

✅ No Python code required
✅ Real-time (< 1 second latency)
✅ Native Fabric integration
✅ Easy to modify triggers via UI
✅ Built-in monitoring and logging
✅ Multiple action types (email, Teams, webhook)

📚 REFERENCE
===========================================

Microsoft Docs:
https://learn.microsoft.com/fabric/real-time-intelligence/data-activator/

Video Tutorial:
Search "Fabric Activator tutorial" on Microsoft Learn
