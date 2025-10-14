"""
Data Quality Alerting Module
Sends notifications for DQ failures and critical issues
Note: Fabric has built-in monitoring, this provides custom alerting for DQ-specific events
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import json


class AlertingService:
    """
    Custom alerting service for data quality issues.
    
    Note: Microsoft Fabric provides built-in alerting via:
    - Fabric Monitoring Hub (pipeline failures, performance)
    - Azure Monitor integration (metrics, logs)
    - Power BI alerts (data refresh failures)
    
    This service provides DQ-specific alerting that can be integrated with:
    - Azure Logic Apps (email, Teams, Slack)
    - Azure Functions (custom webhooks)
    - Service Bus (event-driven architectures)
    """
    
    def __init__(self, webhook_url: Optional[str] = None, log_only: bool = False):
        """
        Initialize alerting service.
        
        Args:
            webhook_url: Optional webhook URL for notifications (Logic App, Teams, etc.)
            log_only: If True, only logs alerts without sending notifications
        """
        self.webhook_url = webhook_url
        self.log_only = log_only
        self.logger = logging.getLogger(__name__)
    
    def send_dq_failure_alert(
        self,
        table_name: str,
        failed_checks: List[Dict],
        severity: str = "WARNING"
    ) -> bool:
        """
        Send alert for data quality check failures.
        
        Args:
            table_name: Name of the table with DQ failures
            failed_checks: List of failed check details
            severity: Alert severity (INFO/WARNING/CRITICAL)
        
        Returns:
            bool: True if alert sent successfully
        """
        alert_payload = {
            "alert_type": "data_quality_failure",
            "timestamp": datetime.utcnow().isoformat(),
            "severity": severity,
            "table_name": table_name,
            "failed_checks": failed_checks,
            "message": f"Data quality checks failed for {table_name}",
            "recommendation": "Review data source and transformation logic"
        }
        
        return self._send_alert(alert_payload)
    
    def send_freshness_alert(
        self,
        table_name: str,
        age_hours: float,
        threshold_hours: float
    ) -> bool:
        """
        Send alert for data freshness SLA violations.
        
        Args:
            table_name: Name of the table
            age_hours: Actual data age in hours
            threshold_hours: Expected threshold
        
        Returns:
            bool: True if alert sent successfully
        """
        alert_payload = {
            "alert_type": "freshness_sla_violation",
            "timestamp": datetime.utcnow().isoformat(),
            "severity": "CRITICAL" if age_hours > threshold_hours * 2 else "WARNING",
            "table_name": table_name,
            "age_hours": age_hours,
            "threshold_hours": threshold_hours,
            "message": f"Data freshness SLA violated for {table_name} (age: {age_hours:.1f}h, threshold: {threshold_hours}h)",
            "recommendation": "Check upstream data pipelines and ingestion processes"
        }
        
        return self._send_alert(alert_payload)
    
    def send_schema_drift_alert(
        self,
        table_name: str,
        missing_columns: List[str],
        extra_columns: List[str],
        type_mismatches: Dict
    ) -> bool:
        """
        Send alert for schema drift detection.
        
        Args:
            table_name: Name of the table
            missing_columns: Columns missing from expected schema
            extra_columns: Unexpected columns found
            type_mismatches: Data type mismatches
        
        Returns:
            bool: True if alert sent successfully
        """
        alert_payload = {
            "alert_type": "schema_drift",
            "timestamp": datetime.utcnow().isoformat(),
            "severity": "CRITICAL",
            "table_name": table_name,
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "type_mismatches": type_mismatches,
            "message": f"Schema drift detected for {table_name}",
            "recommendation": "Update schema contracts or investigate source system changes"
        }
        
        return self._send_alert(alert_payload)
    
    def send_pipeline_failure_alert(
        self,
        pipeline_name: str,
        error_message: str,
        stage: str
    ) -> bool:
        """
        Send alert for pipeline failures.
        
        Args:
            pipeline_name: Name of the failed pipeline
            error_message: Error details
            stage: Pipeline stage that failed
        
        Returns:
            bool: True if alert sent successfully
        """
        alert_payload = {
            "alert_type": "pipeline_failure",
            "timestamp": datetime.utcnow().isoformat(),
            "severity": "CRITICAL",
            "pipeline_name": pipeline_name,
            "stage": stage,
            "error_message": error_message,
            "message": f"Pipeline {pipeline_name} failed at stage: {stage}",
            "recommendation": "Check pipeline logs and retry after fixing issues"
        }
        
        return self._send_alert(alert_payload)
    
    def _send_alert(self, alert_payload: Dict) -> bool:
        """
        Internal method to send alert via configured channel.
        
        Args:
            alert_payload: Alert details
        
        Returns:
            bool: True if sent successfully
        """
        # Log alert
        severity = alert_payload.get("severity", "INFO")
        message = alert_payload.get("message", "Alert")
        
        if severity == "CRITICAL":
            self.logger.error(f"[ALERT] {message}")
        elif severity == "WARNING":
            self.logger.warning(f"[ALERT] {message}")
        else:
            self.logger.info(f"[ALERT] {message}")
        
        # If log_only mode, don't send webhook
        if self.log_only:
            self.logger.info("Alert logged (log_only mode enabled)")
            return True
        
        # Send to webhook if configured
        if self.webhook_url:
            try:
                import requests
                response = requests.post(
                    self.webhook_url,
                    json=alert_payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                
                if response.status_code in [200, 201, 202]:
                    self.logger.info(f"Alert sent successfully to {self.webhook_url}")
                    return True
                else:
                    self.logger.error(f"Failed to send alert: {response.status_code} - {response.text}")
                    return False
                    
            except Exception as e:
                self.logger.error(f"Error sending alert: {str(e)}")
                return False
        
        return True


def create_alerting_service(webhook_url: Optional[str] = None) -> AlertingService:
    """
    Factory function to create alerting service.
    
    Args:
        webhook_url: Optional webhook URL (Azure Logic App, Teams, etc.)
    
    Returns:
        AlertingService instance
    
    Example:
        # Using Azure Logic App webhook
        alerting = create_alerting_service(
            webhook_url="https://prod-xx.logic.azure.com:443/workflows/..."
        )
        
        # Send DQ failure alert
        alerting.send_dq_failure_alert(
            table_name="silver_policies",
            failed_checks=[{"check": "null_ratio", "column": "customer_id"}],
            severity="WARNING"
        )
    """
    import os
    
    # Try to get webhook URL from environment if not provided
    if not webhook_url:
        webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
    
    # Default to log_only if no webhook configured
    log_only = webhook_url is None
    
    return AlertingService(webhook_url=webhook_url, log_only=log_only)
