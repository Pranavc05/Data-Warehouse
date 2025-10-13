"""
Advanced Data Quality Monitoring Engine with Automated Alerts & Lineage Tracking
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from enum import Enum
import logging
import re
from statistics import mean, stdev
import hashlib

import asyncpg
import pandas as pd
import numpy as np

from src.database.connection import get_async_db
from src.services.error_monitoring import error_monitor
from src.streaming.kafka_processor import publish_system_alert, StreamEventType
from src.services.email_service import email_service
from config import PROJECT_ROOT

logger = logging.getLogger(__name__)

class QualityCheckType(Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    REFERENTIAL_INTEGRITY = "referential_integrity"

class Severity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class QualityRule:
    rule_id: str
    rule_name: str
    check_type: QualityCheckType
    table_name: str
    column_name: Optional[str]
    rule_definition: str
    sql_check: str
    threshold: float
    severity: Severity
    is_active: bool
    description: str

@dataclass
class QualityResult:
    rule_id: str
    check_timestamp: datetime
    passed: bool
    score: float
    threshold: float
    severity: Severity
    details: Dict[str, Any]
    affected_rows: int
    sample_failures: List[Dict]

@dataclass
class DataLineageNode:
    object_name: str
    object_type: str  # table, view, function, query
    dependencies: List[str]
    dependents: List[str]
    last_modified: datetime
    metadata: Dict[str, Any]

class DataQualityEngine:
    """
    Enterprise Data Quality Monitoring with Automated Alerts & Lineage Tracking
    """
    
    def __init__(self):
        self.quality_rules = []
        self.lineage_graph = {}
        self.quality_history = []
        
        # Initialize default quality rules
        self._initialize_default_rules()
    
    async def initialize(self) -> bool:
        """Initialize data quality monitoring system"""
        try:
            # Create quality monitoring tables
            await self._create_quality_tables()
            
            # Load custom rules from database
            await self._load_quality_rules()
            
            # Build data lineage graph
            await self._build_lineage_graph()
            
            logger.info("✅ Data Quality Engine initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Data Quality Engine: {e}")
            error_monitor.capture_exception(e, {"context": "data_quality_initialization"})
            return False
    
    def _initialize_default_rules(self):
        """Initialize comprehensive default data quality rules"""
        
        # Completeness checks
        self.quality_rules.extend([
            QualityRule(
                rule_id="COMP_001",
                rule_name="Customer Email Completeness",
                check_type=QualityCheckType.COMPLETENESS,
                table_name="customers",
                column_name="email",
                rule_definition="Email field must not be null or empty",
                sql_check="SELECT COUNT(*) as failures FROM customers WHERE email IS NULL OR email = ''",
                threshold=0.0,
                severity=Severity.HIGH,
                is_active=True,
                description="Ensures all customers have valid email addresses"
            ),
            QualityRule(
                rule_id="COMP_002", 
                rule_name="Order Total Amount Completeness",
                check_type=QualityCheckType.COMPLETENESS,
                table_name="orders",
                column_name="total_amount",
                rule_definition="Order total amount must not be null",
                sql_check="SELECT COUNT(*) as failures FROM orders WHERE total_amount IS NULL",
                threshold=0.0,
                severity=Severity.CRITICAL,
                is_active=True,
                description="Ensures all orders have total amounts"
            )
        ])
        
        # Uniqueness checks
        self.quality_rules.extend([
            QualityRule(
                rule_id="UNIQ_001",
                rule_name="Customer Email Uniqueness",
                check_type=QualityCheckType.UNIQUENESS,
                table_name="customers", 
                column_name="email",
                rule_definition="Customer emails must be unique",
                sql_check="SELECT COUNT(*) as failures FROM (SELECT email, COUNT(*) as cnt FROM customers WHERE email IS NOT NULL GROUP BY email HAVING COUNT(*) > 1) t",
                threshold=0.0,
                severity=Severity.HIGH,
                is_active=True,
                description="Prevents duplicate customer email addresses"
            ),
            QualityRule(
                rule_id="UNIQ_002",
                rule_name="Order Number Uniqueness", 
                check_type=QualityCheckType.UNIQUENESS,
                table_name="orders",
                column_name="order_number",
                rule_definition="Order numbers must be unique",
                sql_check="SELECT COUNT(*) as failures FROM (SELECT order_number, COUNT(*) as cnt FROM orders GROUP BY order_number HAVING COUNT(*) > 1) t",
                threshold=0.0,
                severity=Severity.CRITICAL,
                is_active=True,
                description="Ensures order numbers are unique across system"
            )
        ])
        
        # Validity checks
        self.quality_rules.extend([
            QualityRule(
                rule_id="VAL_001",
                rule_name="Email Format Validity",
                check_type=QualityCheckType.VALIDITY,
                table_name="customers",
                column_name="email", 
                rule_definition="Email addresses must follow valid format",
                sql_check="SELECT COUNT(*) as failures FROM customers WHERE email IS NOT NULL AND email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
                threshold=0.0,
                severity=Severity.MEDIUM,
                is_active=True,
                description="Validates email address format using regex"
            ),
            QualityRule(
                rule_id="VAL_002", 
                rule_name="Order Amount Validity",
                check_type=QualityCheckType.VALIDITY,
                table_name="orders",
                column_name="total_amount",
                rule_definition="Order amounts must be positive", 
                sql_check="SELECT COUNT(*) as failures FROM orders WHERE total_amount IS NOT NULL AND total_amount <= 0",
                threshold=0.0,
                severity=Severity.HIGH,
                is_active=True,
                description="Ensures order amounts are positive values"
            )
        ])
        
        # Consistency checks
        self.quality_rules.extend([
            QualityRule(
                rule_id="CONS_001",
                rule_name="Order Item Total Consistency",
                check_type=QualityCheckType.CONSISTENCY,
                table_name="orders",
                column_name="total_amount",
                rule_definition="Order total should match sum of line items",
                sql_check="""
                SELECT COUNT(*) as failures 
                FROM orders o 
                LEFT JOIN (
                    SELECT order_id, SUM(line_total) as calculated_total 
                    FROM order_items 
                    GROUP BY order_id
                ) oi ON o.order_id = oi.order_id
                WHERE ABS(COALESCE(o.total_amount, 0) - COALESCE(oi.calculated_total, 0)) > 0.01
                """,
                threshold=0.0,
                severity=Severity.HIGH,
                is_active=True,
                description="Validates order totals match item line totals"
            )
        ])
        
        # Timeliness checks
        self.quality_rules.extend([
            QualityRule(
                rule_id="TIME_001",
                rule_name="Recent Order Data Timeliness",
                check_type=QualityCheckType.TIMELINESS,
                table_name="orders",
                column_name="created_at",
                rule_definition="Orders should be created within reasonable time of order_date",
                sql_check="SELECT COUNT(*) as failures FROM orders WHERE ABS(EXTRACT(EPOCH FROM (created_at - order_date))/3600) > 24",
                threshold=5.0,  # Allow up to 5 failures
                severity=Severity.MEDIUM,
                is_active=True,
                description="Ensures orders are created promptly after order date"
            )
        ])
        
        # Referential Integrity checks  
        self.quality_rules.extend([
            QualityRule(
                rule_id="REF_001",
                rule_name="Order Customer Reference Integrity",
                check_type=QualityCheckType.REFERENTIAL_INTEGRITY,
                table_name="orders",
                column_name="customer_id",
                rule_definition="All orders must reference valid customers",
                sql_check="SELECT COUNT(*) as failures FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL",
                threshold=0.0,
                severity=Severity.CRITICAL,
                is_active=True,
                description="Validates all orders have valid customer references"
            ),
            QualityRule(
                rule_id="REF_002", 
                rule_name="Order Items Product Reference Integrity",
                check_type=QualityCheckType.REFERENTIAL_INTEGRITY,
                table_name="order_items",
                column_name="product_id",
                rule_definition="All order items must reference valid products",
                sql_check="SELECT COUNT(*) as failures FROM order_items oi LEFT JOIN products p ON oi.product_id = p.product_id WHERE p.product_id IS NULL",
                threshold=0.0,
                severity=Severity.CRITICAL,
                is_active=True,
                description="Validates all order items have valid product references"
            )
        ])
    
    async def run_quality_checks(self, table_filter: Optional[List[str]] = None) -> List[QualityResult]:
        """Run comprehensive data quality checks"""
        logger.info("🔍 Starting comprehensive data quality assessment...")
        
        results = []
        
        # Filter rules if table filter provided
        rules_to_run = self.quality_rules
        if table_filter:
            rules_to_run = [rule for rule in self.quality_rules if rule.table_name in table_filter]
        
        # Run each quality rule
        for rule in rules_to_run:
            if not rule.is_active:
                continue
                
            try:
                result = await self._execute_quality_rule(rule)
                if result:
                    results.append(result)
                    
                    # Store result in database
                    await self._store_quality_result(result)
                    
                    # Check for alerts
                    if not result.passed:
                        await self._handle_quality_failure(rule, result)
                
            except Exception as e:
                logger.error(f"Quality rule {rule.rule_id} failed: {e}")
                error_monitor.capture_exception(e, {
                    "context": "quality_check_execution",
                    "rule_id": rule.rule_id
                })
        
        # Generate quality report
        await self._generate_quality_report(results)
        
        logger.info(f"✅ Completed quality assessment: {len(results)} checks executed")
        return results
    
    async def _execute_quality_rule(self, rule: QualityRule) -> Optional[QualityResult]:
        """Execute a single data quality rule"""
        try:
            async with get_async_db() as conn:
                # Execute the quality check SQL
                result = await conn.fetchrow(rule.sql_check)
                
                if result is None:
                    return None
                
                failures = result.get('failures', 0)
                
                # Calculate score and pass/fail status
                if rule.check_type == QualityCheckType.COMPLETENESS:
                    # For completeness, calculate percentage of complete records
                    total_rows = await conn.fetchval(f"SELECT COUNT(*) FROM {rule.table_name}")
                    if total_rows > 0:
                        score = ((total_rows - failures) / total_rows) * 100
                    else:
                        score = 100.0
                else:
                    # For other checks, failures should be minimal
                    score = max(0, 100 - failures) if failures <= 100 else 0
                
                passed = failures <= rule.threshold
                
                # Get sample failures for detailed analysis
                sample_failures = []
                if failures > 0 and rule.column_name:
                    sample_failures = await self._get_sample_failures(rule, conn)
                
                return QualityResult(
                    rule_id=rule.rule_id,
                    check_timestamp=datetime.utcnow(),
                    passed=passed,
                    score=score,
                    threshold=rule.threshold,
                    severity=rule.severity,
                    details={
                        'rule_name': rule.rule_name,
                        'check_type': rule.check_type.value,
                        'table_name': rule.table_name,
                        'column_name': rule.column_name,
                        'description': rule.description
                    },
                    affected_rows=failures,
                    sample_failures=sample_failures
                )
                
        except Exception as e:
            logger.error(f"Failed to execute quality rule {rule.rule_id}: {e}")
            return None
    
    async def _get_sample_failures(self, rule: QualityRule, conn: asyncpg.Connection) -> List[Dict]:
        """Get sample records that failed the quality check"""
        try:
            # Build sample query based on rule type
            if rule.check_type == QualityCheckType.COMPLETENESS:
                if rule.column_name:
                    sample_query = f"""
                    SELECT * FROM {rule.table_name} 
                    WHERE {rule.column_name} IS NULL OR {rule.column_name} = '' 
                    LIMIT 5
                    """
                else:
                    return []
            
            elif rule.check_type == QualityCheckType.VALIDITY:
                if 'email' in rule.rule_definition.lower():
                    sample_query = f"""
                    SELECT * FROM {rule.table_name}
                    WHERE {rule.column_name} IS NOT NULL 
                    AND {rule.column_name} !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$'
                    LIMIT 5
                    """
                elif 'positive' in rule.rule_definition.lower():
                    sample_query = f"""
                    SELECT * FROM {rule.table_name}
                    WHERE {rule.column_name} IS NOT NULL AND {rule.column_name} <= 0
                    LIMIT 5
                    """
                else:
                    return []
            
            else:
                return []  # Complex rules handled separately
            
            sample_results = await conn.fetch(sample_query)
            return [dict(row) for row in sample_results]
            
        except Exception as e:
            logger.error(f"Failed to get sample failures: {e}")
            return []
    
    async def _handle_quality_failure(self, rule: QualityRule, result: QualityResult):
        """Handle quality check failures with appropriate alerts"""
        try:
            # Create alert based on severity
            alert_data = {
                'alert_type': 'data_quality_failure',
                'rule_id': rule.rule_id,
                'rule_name': rule.rule_name,
                'severity': rule.severity.value,
                'table_name': rule.table_name,
                'column_name': rule.column_name,
                'affected_rows': result.affected_rows,
                'quality_score': result.score,
                'threshold': result.threshold,
                'check_timestamp': result.check_timestamp.isoformat()
            }
            
            # Publish alert to streaming system
            await publish_system_alert(alert_data)
            
            # Send email alert for high/critical severity
            if rule.severity in [Severity.HIGH, Severity.CRITICAL]:
                await self._send_quality_alert_email(rule, result)
            
            logger.warning(f"🚨 Data Quality Alert: {rule.rule_name} - {result.affected_rows} failures")
            
        except Exception as e:
            logger.error(f"Failed to handle quality failure: {e}")
    
    async def _send_quality_alert_email(self, rule: QualityRule, result: QualityResult):
        """Send email alert for data quality issues"""
        try:
            subject = f"🚨 Data Quality Alert: {rule.rule_name}"
            
            html_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px;">
                <div style="background: {'#dc3545' if rule.severity == Severity.CRITICAL else '#fd7e14'}; color: white; padding: 20px;">
                    <h2>🚨 Data Quality Alert</h2>
                    <p>Severity: <strong>{rule.severity.value.upper()}</strong></p>
                </div>
                
                <div style="padding: 20px; background: #f8f9fa;">
                    <h3>Quality Check Failed</h3>
                    <p><strong>Rule:</strong> {rule.rule_name}</p>
                    <p><strong>Table:</strong> {rule.table_name}</p>
                    <p><strong>Column:</strong> {rule.column_name or 'Multiple'}</p>
                    <p><strong>Description:</strong> {rule.description}</p>
                    
                    <h4>Failure Details:</h4>
                    <ul>
                        <li><strong>Affected Rows:</strong> {result.affected_rows}</li>
                        <li><strong>Quality Score:</strong> {result.score:.1f}%</li>
                        <li><strong>Threshold:</strong> {result.threshold}</li>
                        <li><strong>Check Time:</strong> {result.check_timestamp}</li>
                    </ul>
                    
                    <p><strong>Action Required:</strong> Please investigate and resolve this data quality issue immediately.</p>
                </div>
            </div>
            """
            
            # Would send to data team email list
            # For demo, just log the email
            logger.info(f"📧 Quality alert email would be sent: {subject}")
            
        except Exception as e:
            logger.error(f"Failed to send quality alert email: {e}")
    
    async def _build_lineage_graph(self) -> Dict[str, DataLineageNode]:
        """Build comprehensive data lineage graph"""
        logger.info("🔗 Building data lineage graph...")
        
        try:
            async with get_async_db() as conn:
                # Get table dependencies (foreign keys)
                table_deps = await conn.fetch("""
                    SELECT 
                        tc.table_name as source_table,
                        ccu.table_name as target_table,
                        tc.constraint_name,
                        tc.constraint_type
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu
                        ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = 'public'
                """)
                
                # Get view dependencies
                view_deps = await conn.fetch("""
                    SELECT 
                        schemaname,
                        viewname,
                        definition
                    FROM pg_views
                    WHERE schemaname = 'public'
                """)
                
                # Build lineage nodes
                for table_dep in table_deps:
                    source_table = table_dep['source_table']
                    target_table = table_dep['target_table']
                    
                    # Create or update source node
                    if source_table not in self.lineage_graph:
                        self.lineage_graph[source_table] = DataLineageNode(
                            object_name=source_table,
                            object_type='table',
                            dependencies=[],
                            dependents=[],
                            last_modified=datetime.utcnow(),
                            metadata={'constraint_type': 'foreign_key'}
                        )
                    
                    # Add dependency
                    if target_table not in self.lineage_graph[source_table].dependencies:
                        self.lineage_graph[source_table].dependencies.append(target_table)
                
                # Process views
                for view in view_deps:
                    view_name = view['viewname']
                    view_def = view['definition']
                    
                    # Extract table dependencies from view definition (simplified)
                    referenced_tables = self._extract_tables_from_sql(view_def)
                    
                    self.lineage_graph[view_name] = DataLineageNode(
                        object_name=view_name,
                        object_type='view',
                        dependencies=referenced_tables,
                        dependents=[],
                        last_modified=datetime.utcnow(),
                        metadata={'definition': view_def}
                    )
                
                logger.info(f"✅ Built lineage graph with {len(self.lineage_graph)} objects")
                return self.lineage_graph
                
        except Exception as e:
            logger.error(f"Failed to build lineage graph: {e}")
            return {}
    
    def _extract_tables_from_sql(self, sql_text: str) -> List[str]:
        """Extract table names from SQL text (simplified approach)"""
        try:
            # Simple regex to find table names after FROM and JOIN
            pattern = r'(?:FROM|JOIN)\s+(\w+)'
            matches = re.findall(pattern, sql_text, re.IGNORECASE)
            
            # Filter out common SQL keywords
            sql_keywords = {'select', 'where', 'order', 'group', 'having', 'limit'}
            tables = [match.lower() for match in matches if match.lower() not in sql_keywords]
            
            return list(set(tables))  # Remove duplicates
            
        except Exception as e:
            logger.error(f"Failed to extract tables from SQL: {e}")
            return []
    
    async def get_impact_analysis(self, object_name: str) -> Dict[str, Any]:
        """Get impact analysis for a database object"""
        try:
            if object_name not in self.lineage_graph:
                return {"error": f"Object {object_name} not found in lineage graph"}
            
            node = self.lineage_graph[object_name]
            
            # Find all downstream dependencies (recursive)
            downstream_objects = set()
            self._find_downstream_objects(object_name, downstream_objects)
            
            # Find all upstream dependencies (recursive)
            upstream_objects = set()
            self._find_upstream_objects(object_name, upstream_objects)
            
            return {
                "object_name": object_name,
                "object_type": node.object_type,
                "direct_dependencies": node.dependencies,
                "direct_dependents": node.dependents,
                "all_upstream_objects": list(upstream_objects),
                "all_downstream_objects": list(downstream_objects),
                "total_impact_scope": len(upstream_objects) + len(downstream_objects),
                "last_modified": node.last_modified.isoformat(),
                "impact_analysis_timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Impact analysis failed for {object_name}: {e}")
            return {"error": str(e)}
    
    def _find_downstream_objects(self, object_name: str, found_objects: Set[str], visited: Optional[Set[str]] = None):
        """Recursively find all downstream dependent objects"""
        if visited is None:
            visited = set()
        
        if object_name in visited or object_name not in self.lineage_graph:
            return
        
        visited.add(object_name)
        node = self.lineage_graph[object_name]
        
        for dependent in node.dependents:
            if dependent not in found_objects:
                found_objects.add(dependent)
                self._find_downstream_objects(dependent, found_objects, visited)
    
    def _find_upstream_objects(self, object_name: str, found_objects: Set[str], visited: Optional[Set[str]] = None):
        """Recursively find all upstream dependency objects"""
        if visited is None:
            visited = set()
        
        if object_name in visited or object_name not in self.lineage_graph:
            return
        
        visited.add(object_name)
        node = self.lineage_graph[object_name]
        
        for dependency in node.dependencies:
            if dependency not in found_objects:
                found_objects.add(dependency)
                self._find_upstream_objects(dependency, found_objects, visited)
    
    async def _create_quality_tables(self):
        """Create tables for storing quality monitoring results"""
        async with get_async_db() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_rules (
                    rule_id VARCHAR(50) PRIMARY KEY,
                    rule_name VARCHAR(200) NOT NULL,
                    check_type VARCHAR(50) NOT NULL,
                    table_name VARCHAR(100) NOT NULL,
                    column_name VARCHAR(100),
                    rule_definition TEXT NOT NULL,
                    sql_check TEXT NOT NULL,
                    threshold FLOAT NOT NULL,
                    severity VARCHAR(20) NOT NULL,
                    is_active BOOLEAN DEFAULT true,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_results (
                    result_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    rule_id VARCHAR(50) NOT NULL,
                    check_timestamp TIMESTAMP NOT NULL,
                    passed BOOLEAN NOT NULL,
                    score FLOAT NOT NULL,
                    threshold FLOAT NOT NULL,
                    severity VARCHAR(20) NOT NULL,
                    affected_rows INTEGER NOT NULL,
                    details JSONB,
                    sample_failures JSONB,
                    FOREIGN KEY (rule_id) REFERENCES data_quality_rules(rule_id)
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_quality_results_timestamp 
                ON data_quality_results(check_timestamp)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_quality_results_rule 
                ON data_quality_results(rule_id, check_timestamp)
            """)
    
    async def _load_quality_rules(self):
        """Load quality rules from database"""
        try:
            async with get_async_db() as conn:
                # Load custom rules from database
                custom_rules = await conn.fetch("SELECT * FROM data_quality_rules WHERE is_active = true")
                
                for rule_row in custom_rules:
                    # Convert to QualityRule object and add to rules list
                    pass  # Implementation would convert DB rows to QualityRule objects
                    
        except Exception as e:
            logger.debug(f"No custom quality rules loaded: {e}")
    
    async def _store_quality_result(self, result: QualityResult):
        """Store quality check result in database"""
        try:
            async with get_async_db() as conn:
                await conn.execute("""
                    INSERT INTO data_quality_results 
                    (rule_id, check_timestamp, passed, score, threshold, severity, 
                     affected_rows, details, sample_failures)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, 
                result.rule_id, result.check_timestamp, result.passed, result.score,
                result.threshold, result.severity.value, result.affected_rows,
                json.dumps(result.details), json.dumps(result.sample_failures)
                )
        except Exception as e:
            logger.error(f"Failed to store quality result: {e}")
    
    async def _generate_quality_report(self, results: List[QualityResult]):
        """Generate comprehensive quality assessment report"""
        try:
            total_checks = len(results)
            passed_checks = sum(1 for r in results if r.passed)
            failed_checks = total_checks - passed_checks
            
            overall_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
            
            # Group by severity
            critical_failures = sum(1 for r in results if not r.passed and r.severity == Severity.CRITICAL)
            high_failures = sum(1 for r in results if not r.passed and r.severity == Severity.HIGH)
            
            report = {
                'assessment_timestamp': datetime.utcnow().isoformat(),
                'overall_score': overall_score,
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'failed_checks': failed_checks,
                'critical_failures': critical_failures,
                'high_failures': high_failures,
                'quality_grade': self._calculate_quality_grade(overall_score),
                'recommendations': self._generate_recommendations(results)
            }
            
            logger.info(f"📊 Quality Report: {overall_score:.1f}% overall score, {failed_checks} failures")
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate quality report: {e}")
            return {}
    
    def _calculate_quality_grade(self, score: float) -> str:
        """Calculate quality grade based on score"""
        if score >= 95:
            return "A+"
        elif score >= 90:
            return "A"
        elif score >= 85:
            return "B+"
        elif score >= 80:
            return "B"
        elif score >= 75:
            return "C+"
        elif score >= 70:
            return "C"
        else:
            return "F"
    
    def _generate_recommendations(self, results: List[QualityResult]) -> List[str]:
        """Generate actionable recommendations based on quality results"""
        recommendations = []
        
        failed_results = [r for r in results if not r.passed]
        
        if any(r.severity == Severity.CRITICAL for r in failed_results):
            recommendations.append("🚨 URGENT: Address critical data quality issues immediately")
        
        completeness_failures = [r for r in failed_results if r.details.get('check_type') == 'completeness']
        if completeness_failures:
            recommendations.append("📝 Implement data validation at point of entry to improve completeness")
        
        validity_failures = [r for r in failed_results if r.details.get('check_type') == 'validity']
        if validity_failures:
            recommendations.append("✅ Add format validation and constraints to ensure data validity")
        
        if len(failed_results) > 5:
            recommendations.append("🔧 Consider implementing automated data quality monitoring with real-time alerts")
        
        return recommendations

# Global data quality engine instance
quality_engine = DataQualityEngine()
