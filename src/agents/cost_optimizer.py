"""
Advanced Cost Optimization Engine with AI-driven recommendations
"""

import asyncio
import json
from datetime import datetime, timedelta
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

class OptimizationCategory(Enum):
    STORAGE = "STORAGE"
    COMPUTE = "COMPUTE"
    INDEXING = "INDEXING"
    PARTITIONING = "PARTITIONING"
    ARCHIVING = "ARCHIVING"
    COMPRESSION = "COMPRESSION"
    QUERY_OPTIMIZATION = "QUERY_OPTIMIZATION"

@dataclass
class CostOptimization:
    category: OptimizationCategory
    target_object: str
    description: str
    current_cost: float
    projected_savings: float
    implementation_effort: str  # LOW, MEDIUM, HIGH
    risk_level: str  # LOW, MEDIUM, HIGH
    implementation_sql: List[str]
    monitoring_queries: List[str]
    roi_months: int

class CostOptimizationEngine:
    """
    AI-powered cost optimization engine for database infrastructure
    """
    
    def __init__(self):
        self.llm = OpenAI(
            model_name=AI_MODEL,
            temperature=0.1,
            openai_api_key=OPENAI_API_KEY
        )
        self.setup_prompts()
    
    def setup_prompts(self):
        """Setup AI prompts for cost analysis"""
        
        self.cost_analysis_prompt = PromptTemplate(
            input_variables=["storage_stats", "query_patterns", "infrastructure_metrics"],
            template="""
            As a cloud infrastructure cost optimization expert, analyze the database metrics and provide cost optimization recommendations:

            STORAGE STATISTICS:
            {storage_stats}

            QUERY PATTERNS:
            {query_patterns}

            INFRASTRUCTURE METRICS:
            {infrastructure_metrics}

            Provide detailed cost optimization analysis in JSON format:
            {{
                "storage_optimizations": [
                    {{
                        "type": "partitioning|compression|archiving|cleanup",
                        "target": "table_or_schema_name",
                        "current_size_gb": 150.5,
                        "projected_size_gb": 45.2,
                        "monthly_savings_usd": 85.50,
                        "implementation_complexity": "low|medium|high",
                        "estimated_roi_months": 3
                    }}
                ],
                "compute_optimizations": [
                    {{
                        "type": "query_optimization|index_optimization|connection_pooling",
                        "description": "Optimize slow running queries",
                        "cpu_reduction_percent": 25,
                        "monthly_savings_usd": 120.00,
                        "implementation_steps": ["Create index", "Rewrite query"]
                    }}
                ],
                "infrastructure_recommendations": [
                    {{
                        "type": "right_sizing|auto_scaling|reserved_instances",
                        "current_cost_monthly": 500.00,
                        "optimized_cost_monthly": 320.00,
                        "description": "Right-size database instances based on usage patterns"
                    }}
                ]
            }}
            """
        )
    
    async def analyze_cost_optimization_opportunities(self) -> List[CostOptimization]:
        """
        Comprehensive cost optimization analysis
        """
        logger.info("💰 Starting comprehensive cost optimization analysis...")
        
        # Gather comprehensive metrics
        storage_stats = await self._analyze_storage_usage()
        query_patterns = await self._analyze_query_costs()
        infrastructure_metrics = await self._gather_infrastructure_metrics()
        
        # Use AI for cost analysis
        try:
            analysis_chain = LLMChain(llm=self.llm, prompt=self.cost_analysis_prompt)
            result = await analysis_chain.arun(
                storage_stats=json.dumps(storage_stats),
                query_patterns=json.dumps(query_patterns),
                infrastructure_metrics=json.dumps(infrastructure_metrics)
            )
            
            ai_analysis = json.loads(result)
            
            # Convert AI analysis to CostOptimization objects
            optimizations = []
            
            # Process storage optimizations
            for storage_opt in ai_analysis.get('storage_optimizations', []):
                optimization = CostOptimization(
                    category=OptimizationCategory.STORAGE,
                    target_object=storage_opt.get('target', 'unknown'),
                    description=f"{storage_opt.get('type', 'storage')} optimization",
                    current_cost=storage_opt.get('current_size_gb', 0) * 0.10,  # $0.10/GB estimate
                    projected_savings=storage_opt.get('monthly_savings_usd', 0),
                    implementation_effort=storage_opt.get('implementation_complexity', 'medium').upper(),
                    risk_level='LOW',
                    implementation_sql=self._generate_storage_optimization_sql(storage_opt),
                    monitoring_queries=self._generate_monitoring_queries(storage_opt),
                    roi_months=storage_opt.get('estimated_roi_months', 6)
                )
                optimizations.append(optimization)
            
            # Process compute optimizations
            for compute_opt in ai_analysis.get('compute_optimizations', []):
                optimization = CostOptimization(
                    category=OptimizationCategory.COMPUTE,
                    target_object='database_compute',
                    description=compute_opt.get('description', 'compute optimization'),
                    current_cost=compute_opt.get('monthly_savings_usd', 0) / 0.25,  # Reverse calculate
                    projected_savings=compute_opt.get('monthly_savings_usd', 0),
                    implementation_effort='MEDIUM',
                    risk_level='LOW',
                    implementation_sql=compute_opt.get('implementation_steps', []),
                    monitoring_queries=['SELECT pg_stat_database_conflicts(*);'],
                    roi_months=2
                )
                optimizations.append(optimization)
            
            return optimizations
            
        except Exception as e:
            logger.error(f"AI cost analysis failed: {e}")
            return await self._fallback_cost_analysis()
    
    async def _analyze_storage_usage(self) -> Dict:
        """Comprehensive storage usage analysis"""
        async with get_async_db() as conn:
            # Table sizes and storage statistics
            table_sizes = await conn.fetch("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
                    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size,
                    (SELECT reltuples::bigint FROM pg_class WHERE relname = tablename) as row_count
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """)
            
            # Index usage statistics
            index_usage = await conn.fetch("""
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    idx_tup_read,
                    idx_tup_fetch,
                    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
                    pg_relation_size(indexrelid) as index_size_bytes
                FROM pg_stat_user_indexes
                ORDER BY pg_relation_size(indexrelid) DESC
            """)
            
            # Database bloat estimation
            bloat_analysis = await conn.fetch("""
                SELECT 
                    schemaname,
                    tablename,
                    n_dead_tup,
                    n_live_tup,
                    CASE WHEN n_live_tup > 0 
                         THEN (n_dead_tup::float / n_live_tup * 100)::int 
                         ELSE 0 
                    END as bloat_percent
                FROM pg_stat_user_tables
                WHERE n_dead_tup > 1000
                ORDER BY n_dead_tup DESC
            """)
            
            return {
                'table_sizes': [dict(row) for row in table_sizes],
                'index_usage': [dict(row) for row in index_usage],
                'bloat_analysis': [dict(row) for row in bloat_analysis],
                'total_database_size': await conn.fetchval("SELECT pg_database_size(current_database())")
            }
    
    async def _analyze_query_costs(self) -> Dict:
        """Analyze query costs and resource consumption"""
        async with get_async_db() as conn:
            # Expensive queries analysis
            expensive_queries = await conn.fetch("""
                SELECT 
                    query_hash,
                    query_text,
                    AVG(execution_time_ms) as avg_execution_time,
                    COUNT(*) as execution_count,
                    SUM(execution_time_ms) as total_execution_time,
                    AVG(query_cost) as avg_cost,
                    MAX(timestamp) as last_execution
                FROM query_performance_log
                WHERE timestamp >= NOW() - INTERVAL '30 days'
                GROUP BY query_hash, query_text
                ORDER BY SUM(execution_time_ms) DESC
                LIMIT 50
            """)
            
            # Resource consumption patterns
            resource_patterns = await conn.fetch("""
                SELECT 
                    DATE_TRUNC('hour', timestamp) as hour,
                    AVG(execution_time_ms) as avg_execution_time,
                    COUNT(*) as query_count,
                    SUM(CASE WHEN execution_time_ms > 5000 THEN 1 ELSE 0 END) as slow_queries
                FROM query_performance_log
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY DATE_TRUNC('hour', timestamp)
                ORDER BY hour DESC
            """)
            
            return {
                'expensive_queries': [dict(row) for row in expensive_queries],
                'resource_patterns': [dict(row) for row in resource_patterns],
                'analysis_period': '30 days'
            }
    
    async def _gather_infrastructure_metrics(self) -> Dict:
        """Gather infrastructure and system metrics"""
        async with get_async_db() as conn:
            # Connection statistics
            connection_stats = await conn.fetchrow("""
                SELECT 
                    numbackends as active_connections,
                    xact_commit as transactions_committed,
                    xact_rollback as transactions_rolled_back,
                    blks_read as blocks_read,
                    blks_hit as blocks_hit,
                    tup_returned as tuples_returned,
                    tup_fetched as tuples_fetched
                FROM pg_stat_database 
                WHERE datname = current_database()
            """)
            
            # System resource usage (simulated for demo)
            system_metrics = {
                'avg_cpu_percent': 45.2,
                'avg_memory_percent': 67.8,
                'avg_disk_io_mb_per_sec': 12.5,
                'network_throughput_mbps': 25.3,
                'estimated_monthly_compute_cost': 450.00,
                'estimated_monthly_storage_cost': 180.00
            }
            
            return {
                'connection_stats': dict(connection_stats) if connection_stats else {},
                'system_metrics': system_metrics,
                'cache_hit_ratio': await conn.fetchval("""
                    SELECT ROUND(
                        (blks_hit::float / NULLIF(blks_hit + blks_read, 0)) * 100, 2
                    ) FROM pg_stat_database WHERE datname = current_database()
                """) or 0
            }
    
    def _generate_storage_optimization_sql(self, storage_opt: Dict) -> List[str]:
        """Generate SQL for storage optimizations"""
        opt_type = storage_opt.get('type', '')
        target = storage_opt.get('target', '')
        
        if opt_type == 'partitioning':
            return [
                f"-- Partition table {target} by date",
                f"CREATE TABLE {target}_partitioned (LIKE {target}) PARTITION BY RANGE (created_at);",
                f"CREATE TABLE {target}_2024_q1 PARTITION OF {target}_partitioned FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');",
                f"CREATE TABLE {target}_2024_q2 PARTITION OF {target}_partitioned FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');"
            ]
        elif opt_type == 'compression':
            return [
                f"-- Enable compression for {target}",
                f"ALTER TABLE {target} SET (toast_tuple_target=128);",
                f"VACUUM FULL {target};"
            ]
        elif opt_type == 'archiving':
            return [
                f"-- Archive old data from {target}",
                f"CREATE TABLE {target}_archive (LIKE {target});",
                f"INSERT INTO {target}_archive SELECT * FROM {target} WHERE created_at < NOW() - INTERVAL '2 years';",
                f"DELETE FROM {target} WHERE created_at < NOW() - INTERVAL '2 years';"
            ]
        else:
            return [f"-- Storage optimization for {target}", "VACUUM ANALYZE;"]
    
    def _generate_monitoring_queries(self, optimization: Dict) -> List[str]:
        """Generate monitoring queries for optimizations"""
        return [
            "SELECT pg_size_pretty(pg_database_size(current_database())) as database_size;",
            f"SELECT pg_size_pretty(pg_total_relation_size('{optimization.get('target', 'unknown')}')) as table_size;",
            "SELECT * FROM pg_stat_user_tables WHERE n_dead_tup > 1000;"
        ]
    
    async def _fallback_cost_analysis(self) -> List[CostOptimization]:
        """Fallback cost analysis when AI fails"""
        logger.info("Using fallback cost analysis...")
        
        storage_stats = await self._analyze_storage_usage()
        
        optimizations = []
        
        # Identify large tables for partitioning
        for table in storage_stats['table_sizes'][:5]:  # Top 5 largest tables
            if table['size_bytes'] > 100_000_000:  # > 100MB
                optimization = CostOptimization(
                    category=OptimizationCategory.PARTITIONING,
                    target_object=table['tablename'],
                    description=f"Partition large table {table['tablename']} ({table['size']})",
                    current_cost=table['size_bytes'] / 1_000_000_000 * 0.10,  # $0.10/GB
                    projected_savings=table['size_bytes'] / 1_000_000_000 * 0.03,  # 30% savings
                    implementation_effort='MEDIUM',
                    risk_level='MEDIUM',
                    implementation_sql=[
                        f"CREATE TABLE {table['tablename']}_partitioned (LIKE {table['tablename']}) PARTITION BY RANGE (created_at);"
                    ],
                    monitoring_queries=[
                        f"SELECT pg_size_pretty(pg_total_relation_size('{table['tablename']}'));"
                    ],
                    roi_months=6
                )
                optimizations.append(optimization)
        
        # Identify unused indexes
        for index in storage_stats['index_usage']:
            if index['idx_tup_read'] == 0 and index['index_size_bytes'] > 10_000_000:  # Unused index > 10MB
                optimization = CostOptimization(
                    category=OptimizationCategory.INDEXING,
                    target_object=index['indexname'],
                    description=f"Drop unused index {index['indexname']} ({index['index_size']})",
                    current_cost=index['index_size_bytes'] / 1_000_000_000 * 0.10,
                    projected_savings=index['index_size_bytes'] / 1_000_000_000 * 0.10,
                    implementation_effort='LOW',
                    risk_level='LOW',
                    implementation_sql=[f"DROP INDEX CONCURRENTLY {index['indexname']};"],
                    monitoring_queries=[
                        f"SELECT * FROM pg_stat_user_indexes WHERE indexname = '{index['indexname']}';"
                    ],
                    roi_months=1
                )
                optimizations.append(optimization)
        
        return optimizations
    
    async def generate_cost_report(self, optimizations: List[CostOptimization]) -> Dict:
        """Generate comprehensive cost optimization report"""
        total_current_cost = sum(opt.current_cost for opt in optimizations)
        total_projected_savings = sum(opt.projected_savings for opt in optimizations)
        
        # Group by category
        by_category = {}
        for opt in optimizations:
            category = opt.category.value
            if category not in by_category:
                by_category[category] = {'count': 0, 'savings': 0, 'optimizations': []}
            by_category[category]['count'] += 1
            by_category[category]['savings'] += opt.projected_savings
            by_category[category]['optimizations'].append({
                'target': opt.target_object,
                'description': opt.description,
                'savings': opt.projected_savings,
                'effort': opt.implementation_effort,
                'roi_months': opt.roi_months
            })
        
        return {
            'summary': {
                'total_optimizations': len(optimizations),
                'total_current_monthly_cost': total_current_cost,
                'total_projected_monthly_savings': total_projected_savings,
                'savings_percentage': (total_projected_savings / total_current_cost * 100) if total_current_cost > 0 else 0,
                'average_roi_months': sum(opt.roi_months for opt in optimizations) / len(optimizations) if optimizations else 0
            },
            'by_category': by_category,
            'top_opportunities': sorted(
                [
                    {
                        'target': opt.target_object,
                        'category': opt.category.value,
                        'description': opt.description,
                        'savings': opt.projected_savings,
                        'effort': opt.implementation_effort,
                        'roi_months': opt.roi_months
                    }
                    for opt in optimizations
                ],
                key=lambda x: x['savings'],
                reverse=True
            )[:10],
            'quick_wins': [
                opt for opt in optimizations 
                if opt.implementation_effort == 'LOW' and opt.projected_savings > 10
            ],
            'generated_at': datetime.now().isoformat()
        }
