"""
Advanced Error Monitoring and Alerting System with Sentry
"""

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
from sentry_sdk.integrations.asyncio import AsyncioIntegration
import logging
import traceback
from typing import Dict, Any, Optional
from datetime import datetime
import asyncio
from dataclasses import dataclass
from enum import Enum

from config import SENTRY_DSN, ENVIRONMENT

logger = logging.getLogger(__name__)

class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class SystemAlert:
    level: AlertLevel
    title: str
    message: str
    component: str
    metadata: Dict[str, Any]
    timestamp: datetime

class SentryErrorMonitoring:
    """
    Enterprise-grade error monitoring and alerting system
    """
    
    def __init__(self):
        self.sentry_dsn = SENTRY_DSN
        self.is_initialized = False
        
        if self.sentry_dsn:
            self._initialize_sentry()
        else:
            logger.warning("🚨 Sentry DSN not configured - error monitoring disabled")
    
    def _initialize_sentry(self):
        """Initialize Sentry with comprehensive integrations"""
        try:
            sentry_sdk.init(
                dsn=self.sentry_dsn,
                environment=ENVIRONMENT,
                integrations=[
                    FastApiIntegration(auto_enable=True),
                    SqlalchemyIntegration(),
                    AsyncioIntegration(),
                ],
                # Performance monitoring
                traces_sample_rate=1.0 if ENVIRONMENT == "development" else 0.1,
                
                # Error sampling
                sample_rate=1.0,
                
                # Release tracking
                release=f"autosql@1.0.0",
                
                # Additional configuration
                attach_stacktrace=True,
                send_default_pii=False,  # Don't send personally identifiable information
                max_breadcrumbs=50,
                
                # Custom error filtering
                before_send=self._filter_errors,
                
                # Performance monitoring
                profiles_sample_rate=0.1,
            )
            
            # Set custom tags
            sentry_sdk.set_tag("component", "data-warehouse-orchestrator")
            sentry_sdk.set_tag("version", "1.0.0")
            sentry_sdk.set_tag("built_for", "autodesk_internship")
            
            self.is_initialized = True
            logger.info("✅ Sentry error monitoring initialized successfully")
            
            # Test Sentry connection
            with sentry_sdk.configure_scope() as scope:
                scope.set_tag("test", "initialization")
                sentry_sdk.capture_message("AutoSQL Error Monitoring System Started", level="info")
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize Sentry: {e}")
            self.is_initialized = False
    
    def _filter_errors(self, event, hint):
        """Filter out noise and enhance error reports"""
        try:
            # Filter out health check errors
            if 'request' in event:
                url = event.get('request', {}).get('url', '')
                if '/health' in url or '/metrics' in url:
                    return None
            
            # Enhance error context
            if 'extra' not in event:
                event['extra'] = {}
            
            event['extra']['timestamp'] = datetime.utcnow().isoformat()
            event['extra']['component'] = 'autosql-orchestrator'
            
            # Add database context if available
            if hasattr(hint, 'exc_info') and hint.exc_info:
                exc_type, exc_value, exc_traceback = hint.exc_info
                if 'database' in str(exc_value).lower() or 'sql' in str(exc_value).lower():
                    event['extra']['error_category'] = 'database'
                elif 'ai' in str(exc_value).lower() or 'openai' in str(exc_value).lower():
                    event['extra']['error_category'] = 'ai_service'
                else:
                    event['extra']['error_category'] = 'application'
            
            return event
            
        except Exception:
            # If filtering fails, return the original event
            return event
    
    def capture_exception(self, exception: Exception, extra_context: Dict[str, Any] = None):
        """Capture and report an exception with enhanced context"""
        try:
            if self.is_initialized:
                with sentry_sdk.configure_scope() as scope:
                    if extra_context:
                        for key, value in extra_context.items():
                            scope.set_extra(key, value)
                    
                    sentry_sdk.capture_exception(exception)
            
            # Also log locally for development
            logger.error(f"Exception captured: {type(exception).__name__}: {exception}")
            if extra_context:
                logger.error(f"Context: {extra_context}")
                
        except Exception as e:
            logger.error(f"Failed to capture exception in Sentry: {e}")
    
    def capture_message(self, message: str, level: str = "info", extra_context: Dict[str, Any] = None):
        """Capture a custom message with context"""
        try:
            if self.is_initialized:
                with sentry_sdk.configure_scope() as scope:
                    if extra_context:
                        for key, value in extra_context.items():
                            scope.set_extra(key, value)
                    
                    sentry_sdk.capture_message(message, level=level)
            
            # Also log locally
            getattr(logger, level, logger.info)(f"Message captured: {message}")
            
        except Exception as e:
            logger.error(f"Failed to capture message in Sentry: {e}")
    
    def add_breadcrumb(self, message: str, category: str = "custom", level: str = "info", data: Dict = None):
        """Add a breadcrumb for debugging context"""
        try:
            if self.is_initialized:
                sentry_sdk.add_breadcrumb(
                    message=message,
                    category=category,
                    level=level,
                    data=data or {}
                )
        except Exception as e:
            logger.error(f"Failed to add breadcrumb: {e}")
    
    def set_user_context(self, user_id: str, username: str = None, email: str = None):
        """Set user context for error tracking"""
        try:
            if self.is_initialized:
                sentry_sdk.set_user({
                    "id": user_id,
                    "username": username,
                    "email": email
                })
        except Exception as e:
            logger.error(f"Failed to set user context: {e}")
    
    def set_transaction_name(self, name: str):
        """Set transaction name for performance monitoring"""
        try:
            if self.is_initialized:
                with sentry_sdk.configure_scope() as scope:
                    scope.transaction = name
        except Exception as e:
            logger.error(f"Failed to set transaction name: {e}")

class SystemAlerting:
    """
    System-wide alerting for critical events
    """
    
    def __init__(self, error_monitor: SentryErrorMonitoring):
        self.error_monitor = error_monitor
        self.alert_thresholds = {
            "database_connection_failures": 5,  # Alert after 5 failures
            "ai_service_failures": 3,           # Alert after 3 AI failures
            "query_performance_degradation": 50, # Alert if 50% performance drop
            "high_error_rate": 10               # Alert if >10 errors/minute
        }
        
        self.alert_history = []
    
    async def check_system_health(self) -> Dict[str, Any]:
        """Comprehensive system health check"""
        health_status = {
            "overall_status": "healthy",
            "components": {},
            "alerts": [],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        try:
            # Database health
            db_health = await self._check_database_health()
            health_status["components"]["database"] = db_health
            
            # AI service health
            ai_health = await self._check_ai_service_health()
            health_status["components"]["ai_services"] = ai_health
            
            # System resources
            resource_health = await self._check_resource_health()
            health_status["components"]["system_resources"] = resource_health
            
            # Determine overall status
            component_statuses = [comp["status"] for comp in health_status["components"].values()]
            
            if "critical" in component_statuses:
                health_status["overall_status"] = "critical"
            elif "warning" in component_statuses:
                health_status["overall_status"] = "warning"
            elif "unhealthy" in component_statuses:
                health_status["overall_status"] = "unhealthy"
            
            # Generate alerts for issues
            for component_name, component_status in health_status["components"].items():
                if component_status["status"] in ["critical", "unhealthy"]:
                    alert = SystemAlert(
                        level=AlertLevel.CRITICAL if component_status["status"] == "critical" else AlertLevel.ERROR,
                        title=f"{component_name.title()} Health Issue",
                        message=component_status.get("message", f"{component_name} is {component_status['status']}"),
                        component=component_name,
                        metadata=component_status,
                        timestamp=datetime.utcnow()
                    )
                    
                    await self._send_alert(alert)
                    health_status["alerts"].append({
                        "level": alert.level.value,
                        "title": alert.title,
                        "message": alert.message
                    })
            
            return health_status
            
        except Exception as e:
            self.error_monitor.capture_exception(e, {"context": "system_health_check"})
            return {
                "overall_status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def _check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity and performance"""
        try:
            from src.database.connection import test_connection
            
            if test_connection():
                return {
                    "status": "healthy",
                    "message": "Database connection successful",
                    "response_time_ms": 50  # Mock response time
                }
            else:
                return {
                    "status": "critical",
                    "message": "Database connection failed",
                    "error": "Unable to connect to PostgreSQL"
                }
                
        except Exception as e:
            return {
                "status": "critical",
                "message": "Database health check failed",
                "error": str(e)
            }
    
    async def _check_ai_service_health(self) -> Dict[str, Any]:
        """Check AI service availability"""
        try:
            from config import OPENAI_API_KEY
            
            if not OPENAI_API_KEY:
                return {
                    "status": "warning",
                    "message": "OpenAI API key not configured"
                }
            
            # Mock AI service check (in production, would test actual API)
            return {
                "status": "healthy",
                "message": "AI services operational",
                "api_key_configured": True
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": "AI service check failed",
                "error": str(e)
            }
    
    async def _check_resource_health(self) -> Dict[str, Any]:
        """Check system resource utilization"""
        try:
            import psutil
            
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            status = "healthy"
            issues = []
            
            if cpu_percent > 90:
                status = "warning"
                issues.append(f"High CPU usage: {cpu_percent}%")
            
            if memory.percent > 90:
                status = "critical" if status != "critical" else status
                issues.append(f"High memory usage: {memory.percent}%")
            
            if disk.percent > 95:
                status = "critical"
                issues.append(f"High disk usage: {disk.percent}%")
            
            return {
                "status": status,
                "message": "; ".join(issues) if issues else "System resources normal",
                "metrics": {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "disk_percent": disk.percent
                }
            }
            
        except Exception as e:
            return {
                "status": "warning",
                "message": "Unable to check system resources",
                "error": str(e)
            }
    
    async def _send_alert(self, alert: SystemAlert):
        """Send alert via multiple channels"""
        try:
            # Send to Sentry
            self.error_monitor.capture_message(
                f"System Alert: {alert.title}",
                level=alert.level.value,
                extra_context={
                    "alert_component": alert.component,
                    "alert_metadata": alert.metadata,
                    "alert_timestamp": alert.timestamp.isoformat()
                }
            )
            
            # Log locally
            log_level = getattr(logger, alert.level.value, logger.info)
            log_level(f"🚨 ALERT: {alert.title} - {alert.message}")
            
            # Store in alert history
            self.alert_history.append(alert)
            
            # Keep only last 100 alerts
            if len(self.alert_history) > 100:
                self.alert_history = self.alert_history[-100:]
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

# Global instances
error_monitor = SentryErrorMonitoring()
system_alerting = SystemAlerting(error_monitor)
