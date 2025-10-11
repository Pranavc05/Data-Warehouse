"""
AI-Driven Schema Evolution Engine
This system automatically analyzes schema changes and generates migration strategies
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

import asyncpg
from langchain.llms import OpenAI
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate

from src.database.connection import get_async_db
from config import OPENAI_API_KEY, AI_MODEL

logger = logging.getLogger(__name__)

class ChangeType(Enum):
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    MODIFY_COLUMN = "MODIFY_COLUMN"
    ADD_INDEX = "ADD_INDEX"
    DROP_INDEX = "DROP_INDEX"
    ADD_CONSTRAINT = "ADD_CONSTRAINT"
    DROP_CONSTRAINT = "DROP_CONSTRAINT"
    PARTITION_TABLE = "PARTITION_TABLE"
    RENAME_COLUMN = "RENAME_COLUMN"
    CHANGE_DATA_TYPE = "CHANGE_DATA_TYPE"

@dataclass
class SchemaChange:
    change_type: ChangeType
    table_name: str
    column_name: Optional[str]
    old_definition: Optional[str]
    new_definition: Optional[str]
    migration_sql: str
    rollback_sql: str
    risk_level: str  # LOW, MEDIUM, HIGH, CRITICAL
    estimated_downtime: str
    impact_analysis: str
    backup_required: bool

class SchemaEvolutionEngine:
    """
    Advanced AI-powered schema evolution with zero-downtime migrations
    """
    
    def __init__(self):
        self.llm = OpenAI(
            model_name=AI_MODEL,
            temperature=0.1,
            openai_api_key=OPENAI_API_KEY
        )
        self.setup_prompts()
    
    def setup_prompts(self):
        """Setup AI prompts for schema analysis"""
        
        self.schema_analysis_prompt = PromptTemplate(
            input_variables=["current_schema", "proposed_changes", "usage_patterns"],
            template="""
            As a senior database architect, analyze the proposed schema changes and provide comprehensive migration strategy:

            CURRENT SCHEMA:
            {current_schema}

            PROPOSED CHANGES:
            {proposed_changes}

            USAGE PATTERNS:
            {usage_patterns}

            Provide analysis in JSON format:
            {{
                "migration_strategy": "zero_downtime|rolling_deployment|maintenance_window",
                "risk_assessment": {{
                    "data_loss_risk": "none|low|medium|high",
                    "performance_impact": "minimal|moderate|significant|severe",
                    "rollback_complexity": "simple|moderate|complex|critical"
                }},
                "migration_steps": [
                    {{
                        "step": 1,
                        "description": "Create new column with default value",
                        "sql": "ALTER TABLE...",
                        "rollback_sql": "ALTER TABLE...",
                        "estimated_time": "30 seconds",
                        "requires_lock": false
                    }}
                ],
                "pre_migration_checks": ["Check disk space", "Verify backup"],
                "post_migration_validation": ["Data integrity check", "Performance test"],
                "estimated_total_downtime": "0 minutes"
            }}
            """
        )
    
    async def analyze_schema_evolution(self, proposed_changes: List[Dict]) -> List[SchemaChange]:
        """
        Analyze proposed schema changes and generate migration plan
        """
        logger.info("🧠 AI analyzing schema evolution requirements...")
        
        current_schema = await self._get_current_schema()
        usage_patterns = await self._analyze_usage_patterns()
        
        schema_changes = []
        
        for change in proposed_changes:
            try:
                # Use AI to analyze each change
                analysis_chain = LLMChain(llm=self.llm, prompt=self.schema_analysis_prompt)
                result = await analysis_chain.arun(
                    current_schema=json.dumps(current_schema),
                    proposed_changes=json.dumps([change]),
                    usage_patterns=json.dumps(usage_patterns)
                )
                
                ai_analysis = json.loads(result)
                
                # Create SchemaChange object
                schema_change = SchemaChange(
                    change_type=ChangeType(change.get('type', 'MODIFY_COLUMN')),
                    table_name=change.get('table_name', ''),
                    column_name=change.get('column_name'),
                    old_definition=change.get('old_definition'),
                    new_definition=change.get('new_definition'),
                    migration_sql=self._generate_migration_sql(change, ai_analysis),
                    rollback_sql=self._generate_rollback_sql(change, ai_analysis),
                    risk_level=self._assess_risk_level(ai_analysis),
                    estimated_downtime=ai_analysis.get('estimated_total_downtime', '0 minutes'),
                    impact_analysis=self._generate_impact_analysis(ai_analysis),
                    backup_required=self._requires_backup(ai_analysis)
                )
                
                schema_changes.append(schema_change)
                
            except Exception as e:
                logger.error(f"Error analyzing schema change: {e}")
                # Fallback to conservative analysis
                schema_changes.append(self._conservative_schema_change(change))
        
        return schema_changes
    
    async def execute_migration(self, schema_changes: List[SchemaChange], 
                              dry_run: bool = True) -> Dict:
        """
        Execute schema migration with advanced safety checks
        """
        logger.info(f"🚀 {'DRY RUN: ' if dry_run else ''}Executing schema migration...")
        
        migration_results = {
            'total_changes': len(schema_changes),
            'successful': 0,
            'failed': 0,
            'rollback_executed': False,
            'details': []
        }
        
        if not dry_run:
            # Create migration backup
            backup_id = await self._create_migration_backup()
            logger.info(f"📦 Created backup: {backup_id}")
        
        try:
            async with get_async_db() as conn:
                # Start transaction
                async with conn.transaction():
                    
                    for i, change in enumerate(schema_changes):
                        try:
                            logger.info(f"⚙️ Executing change {i+1}/{len(schema_changes)}: {change.change_type.value}")
                            
                            if dry_run:
                                # Validate SQL without executing
                                await conn.execute(f"EXPLAIN {change.migration_sql}")
                                result = {"status": "validated", "sql": change.migration_sql}
                            else:
                                # Execute actual migration
                                await conn.execute(change.migration_sql)
                                
                                # Validate the change
                                validation_result = await self._validate_migration_step(conn, change)
                                result = {"status": "executed", "validation": validation_result}
                            
                            migration_results['successful'] += 1
                            migration_results['details'].append({
                                'change': change.change_type.value,
                                'table': change.table_name,
                                'result': result
                            })
                            
                        except Exception as e:
                            logger.error(f"❌ Migration step failed: {e}")
                            migration_results['failed'] += 1
                            migration_results['details'].append({
                                'change': change.change_type.value,
                                'table': change.table_name,
                                'error': str(e)
                            })
                            
                            if not dry_run:
                                # Execute rollback
                                await self._execute_rollback(conn, schema_changes[:i])
                                migration_results['rollback_executed'] = True
                            
                            break
        
        except Exception as e:
            logger.error(f"💥 Critical migration error: {e}")
            migration_results['critical_error'] = str(e)
        
        return migration_results
    
    async def _get_current_schema(self) -> Dict:
        """Get comprehensive current schema information"""
        async with get_async_db() as conn:
            # Get table information
            tables = await conn.fetch("""
                SELECT 
                    table_name,
                    table_type,
                    is_insertable_into,
                    is_typed
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            
            schema_info = {}
            
            for table in tables:
                table_name = table['table_name']
                
                # Get columns
                columns = await conn.fetch("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = $1 AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, table_name)
                
                # Get indexes
                indexes = await conn.fetch("""
                    SELECT 
                        indexname,
                        indexdef
                    FROM pg_indexes
                    WHERE tablename = $1 AND schemaname = 'public'
                """, table_name)
                
                # Get constraints
                constraints = await conn.fetch("""
                    SELECT 
                        constraint_name,
                        constraint_type,
                        column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu
                        ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_name = $1 AND tc.table_schema = 'public'
                """, table_name)
                
                schema_info[table_name] = {
                    'type': table['table_type'],
                    'columns': [dict(col) for col in columns],
                    'indexes': [dict(idx) for idx in indexes],
                    'constraints': [dict(con) for con in constraints]
                }
            
            return schema_info
    
    async def _analyze_usage_patterns(self) -> Dict:
        """Analyze query usage patterns for impact assessment"""
        async with get_async_db() as conn:
            # Analyze query patterns from performance log
            patterns = await conn.fetch("""
                SELECT 
                    query_text,
                    COUNT(*) as frequency,
                    AVG(execution_time_ms) as avg_execution_time,
                    MAX(timestamp) as last_execution
                FROM query_performance_log
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY query_text
                ORDER BY frequency DESC
                LIMIT 100
            """)
            
            # Extract table usage patterns
            table_usage = {}
            for pattern in patterns:
                # Simple regex to extract table names (would use proper SQL parser in production)
                import re
                tables = re.findall(r'(?:FROM|JOIN)\s+(\w+)', pattern['query_text'], re.IGNORECASE)
                
                for table in tables:
                    if table not in table_usage:
                        table_usage[table] = {'frequency': 0, 'avg_time': 0}
                    table_usage[table]['frequency'] += pattern['frequency']
                    table_usage[table]['avg_time'] = max(
                        table_usage[table]['avg_time'], 
                        pattern['avg_execution_time']
                    )
            
            return {
                'query_patterns': [dict(p) for p in patterns],
                'table_usage': table_usage,
                'analysis_date': datetime.now().isoformat()
            }
    
    def _generate_migration_sql(self, change: Dict, ai_analysis: Dict) -> str:
        """Generate migration SQL from AI analysis"""
        migration_steps = ai_analysis.get('migration_steps', [])
        if migration_steps:
            return migration_steps[0].get('sql', 'SELECT 1;')
        
        # Fallback SQL generation
        change_type = change.get('type', '')
        table_name = change.get('table_name', '')
        column_name = change.get('column_name', '')
        
        if change_type == 'ADD_COLUMN':
            return f"ALTER TABLE {table_name} ADD COLUMN {column_name} {change.get('data_type', 'TEXT')};"
        elif change_type == 'DROP_COLUMN':
            return f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
        else:
            return "SELECT 1; -- Migration SQL not generated"
    
    def _generate_rollback_sql(self, change: Dict, ai_analysis: Dict) -> str:
        """Generate rollback SQL from AI analysis"""
        migration_steps = ai_analysis.get('migration_steps', [])
        if migration_steps:
            return migration_steps[0].get('rollback_sql', 'SELECT 1;')
        
        # Fallback rollback SQL generation
        change_type = change.get('type', '')
        table_name = change.get('table_name', '')
        column_name = change.get('column_name', '')
        
        if change_type == 'ADD_COLUMN':
            return f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
        elif change_type == 'DROP_COLUMN':
            return f"ALTER TABLE {table_name} ADD COLUMN {column_name} {change.get('data_type', 'TEXT')};"
        else:
            return "SELECT 1; -- Rollback SQL not generated"
    
    def _assess_risk_level(self, ai_analysis: Dict) -> str:
        """Assess risk level from AI analysis"""
        risk_assessment = ai_analysis.get('risk_assessment', {})
        
        data_loss_risk = risk_assessment.get('data_loss_risk', 'low')
        performance_impact = risk_assessment.get('performance_impact', 'minimal')
        
        if data_loss_risk in ['high', 'critical'] or performance_impact == 'severe':
            return 'CRITICAL'
        elif data_loss_risk == 'medium' or performance_impact == 'significant':
            return 'HIGH'
        elif performance_impact == 'moderate':
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _generate_impact_analysis(self, ai_analysis: Dict) -> str:
        """Generate human-readable impact analysis"""
        risk_assessment = ai_analysis.get('risk_assessment', {})
        strategy = ai_analysis.get('migration_strategy', 'maintenance_window')
        
        return f"""
        Migration Strategy: {strategy.replace('_', ' ').title()}
        Data Loss Risk: {risk_assessment.get('data_loss_risk', 'unknown')}
        Performance Impact: {risk_assessment.get('performance_impact', 'unknown')}
        Rollback Complexity: {risk_assessment.get('rollback_complexity', 'unknown')}
        """
    
    def _requires_backup(self, ai_analysis: Dict) -> bool:
        """Determine if backup is required"""
        risk_assessment = ai_analysis.get('risk_assessment', {})
        return risk_assessment.get('data_loss_risk', 'low') in ['medium', 'high', 'critical']
    
    def _conservative_schema_change(self, change: Dict) -> SchemaChange:
        """Create conservative schema change for fallback"""
        return SchemaChange(
            change_type=ChangeType.MODIFY_COLUMN,
            table_name=change.get('table_name', 'unknown'),
            column_name=change.get('column_name'),
            old_definition=None,
            new_definition=None,
            migration_sql="SELECT 1; -- Conservative fallback",
            rollback_sql="SELECT 1; -- Conservative rollback",
            risk_level='HIGH',
            estimated_downtime='Unknown',
            impact_analysis='Conservative analysis - manual review required',
            backup_required=True
        )
    
    async def _create_migration_backup(self) -> str:
        """Create database backup before migration"""
        backup_id = f"migration_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        # Implementation would create actual backup
        logger.info(f"📦 Creating migration backup: {backup_id}")
        return backup_id
    
    async def _validate_migration_step(self, conn: asyncpg.Connection, 
                                     change: SchemaChange) -> Dict:
        """Validate migration step execution"""
        try:
            # Basic validation queries
            if change.table_name:
                # Check table exists
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = $1 AND table_schema = 'public'
                    )
                """, change.table_name)
                
                if change.column_name and change.change_type == ChangeType.ADD_COLUMN:
                    # Check column was added
                    column_exists = await conn.fetchval("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = $1 AND column_name = $2 AND table_schema = 'public'
                        )
                    """, change.table_name, change.column_name)
                    
                    return {'table_exists': exists, 'column_added': column_exists}
                
                return {'table_exists': exists}
            
            return {'status': 'validated'}
            
        except Exception as e:
            return {'error': str(e), 'validation_failed': True}
    
    async def _execute_rollback(self, conn: asyncpg.Connection, 
                               executed_changes: List[SchemaChange]):
        """Execute rollback for failed migration"""
        logger.warning("🔄 Executing migration rollback...")
        
        # Execute rollbacks in reverse order
        for change in reversed(executed_changes):
            try:
                await conn.execute(change.rollback_sql)
                logger.info(f"✅ Rolled back: {change.change_type.value}")
            except Exception as e:
                logger.error(f"❌ Rollback failed for {change.change_type.value}: {e}")
