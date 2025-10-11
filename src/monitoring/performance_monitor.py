"""
Performance monitoring system with real-time metrics collection
"""

import asyncio
import time
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

import asyncpg
from src.database.connection import get_async_db

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """
    Real-time performance monitoring for database and system metrics
    """
    
    def __init__(self):
        self.metrics_history = []
        self.alert_thresholds = {
            'slow_query_threshold_ms': 1000,
            'cpu_threshold_percent': 80,
            'memory_threshold_percent': 85,
            'disk_io_threshold_mb': 100
        }
    
    async def collect_metrics(self):
        """Collect comprehensive system and database metrics"""
        try:
            metrics = {
                'timestamp': datetime.now(),
                'system': await self._collect_system_metrics(),
                'database': await self._collect_database_metrics(),
                'queries': await self._collect_query_metrics()
            }
            
            self.metrics_history.append(metrics)
            
            # Keep only last 1000 metrics to prevent memory issues
            if len(self.metrics_history) > 1000:
                self.metrics_history = self.metrics_history[-1000:]
                
            # Check for alerts
            await self._check_alerts(metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            return None
    
    async def _collect_system_metrics(self) -> Dict:
        """Collect system-level performance metrics"""
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage_percent': psutil.disk_usage('/').percent,
            'network_io': psutil.net_io_counters()._asdict(),
            'process_count': len(psutil.pids())
        }
    
    async def _collect_database_metrics(self) -> Dict:
        """Collect database performance metrics"""
        try:
            async with get_async_db() as conn:
                # Database connection stats
                db_stats = await conn.fetchrow("""
                    SELECT 
                        numbackends as active_connections,
                        xact_commit as transactions_committed,
                        xact_rollback as transactions_rolled_back,
                        blks_read as blocks_read,
                        blks_hit as blocks_hit,
                        tup_returned as tuples_returned,
                        tup_fetched as tuples_fetched,
                        tup_inserted as tuples_inserted,
                        tup_updated as tuples_updated,
                        tup_deleted as tuples_deleted
                    FROM pg_stat_database 
                    WHERE datname = current_database()
                """)
                
                # Cache hit ratio
                cache_hit_ratio = await conn.fetchval("""
                    SELECT 
                        ROUND(
                            (blks_hit::float / NULLIF(blks_hit + blks_read, 0)) * 100, 2
                        ) as cache_hit_ratio
                    FROM pg_stat_database 
                    WHERE datname = current_database()
                """)
                
                # Active queries count
                active_queries = await conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM pg_stat_activity 
                    WHERE state = 'active' AND query != '<IDLE>'
                """)
                
                return {
                    **dict(db_stats),
                    'cache_hit_ratio': cache_hit_ratio or 0,
                    'active_queries': active_queries
                }
                
        except Exception as e:
            logger.error(f"Database metrics collection failed: {e}")
            return {}
    
    async def _collect_query_metrics(self) -> Dict:
        """Collect query performance metrics"""
        try:
            async with get_async_db() as conn:
                # Recent slow queries
                slow_queries = await conn.fetch("""
                    SELECT 
                        COUNT(*) as slow_query_count,
                        AVG(execution_time_ms) as avg_execution_time,
                        MAX(execution_time_ms) as max_execution_time
                    FROM query_performance_log
                    WHERE timestamp >= NOW() - INTERVAL '5 minutes'
                    AND execution_time_ms > 1000
                """)
                
                # Query distribution by execution time
                query_distribution = await conn.fetch("""
                    SELECT 
                        CASE 
                            WHEN execution_time_ms < 100 THEN 'fast'
                            WHEN execution_time_ms < 1000 THEN 'moderate'
                            WHEN execution_time_ms < 5000 THEN 'slow'
                            ELSE 'critical'
                        END as performance_category,
                        COUNT(*) as query_count
                    FROM query_performance_log
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    GROUP BY performance_category
                """)
                
                return {
                    'slow_queries': dict(slow_queries[0]) if slow_queries else {},
                    'distribution': {row['performance_category']: row['query_count'] 
                                   for row in query_distribution}
                }
                
        except Exception as e:
            logger.error(f"Query metrics collection failed: {e}")
            return {}
    
    async def _check_alerts(self, metrics: Dict):
        """Check metrics against thresholds and generate alerts"""
        alerts = []
        
        # System alerts
        if metrics['system']['cpu_percent'] > self.alert_thresholds['cpu_threshold_percent']:
            alerts.append({
                'type': 'HIGH_CPU',
                'message': f"CPU usage at {metrics['system']['cpu_percent']:.1f}%",
                'severity': 'warning'
            })
        
        if metrics['system']['memory_percent'] > self.alert_thresholds['memory_threshold_percent']:
            alerts.append({
                'type': 'HIGH_MEMORY',
                'message': f"Memory usage at {metrics['system']['memory_percent']:.1f}%", 
                'severity': 'warning'
            })
        
        # Database alerts
        if metrics['database'].get('cache_hit_ratio', 100) < 90:
            alerts.append({
                'type': 'LOW_CACHE_HIT_RATIO',
                'message': f"Cache hit ratio at {metrics['database']['cache_hit_ratio']:.1f}%",
                'severity': 'warning'
            })
        
        # Query performance alerts
        slow_queries = metrics['queries'].get('slow_queries', {})
        if slow_queries.get('slow_query_count', 0) > 5:
            alerts.append({
                'type': 'HIGH_SLOW_QUERIES',
                'message': f"{slow_queries['slow_query_count']} slow queries in last 5 minutes",
                'severity': 'critical'
            })
        
        if alerts:
            logger.warning(f"Performance alerts: {alerts}")
            # In production, would send to alerting system
    
    def get_recent_metrics(self, minutes: int = 60) -> List[Dict]:
        """Get metrics from the last N minutes"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        return [
            metric for metric in self.metrics_history 
            if metric['timestamp'] >= cutoff_time
        ]
    
    def get_performance_summary(self) -> Dict:
        """Get summarized performance metrics"""
        if not self.metrics_history:
            return {}
        
        recent_metrics = self.get_recent_metrics(60)
        if not recent_metrics:
            return {}
        
        return {
            'avg_cpu_percent': sum(m['system']['cpu_percent'] for m in recent_metrics) / len(recent_metrics),
            'avg_memory_percent': sum(m['system']['memory_percent'] for m in recent_metrics) / len(recent_metrics),
            'avg_cache_hit_ratio': sum(m['database'].get('cache_hit_ratio', 0) for m in recent_metrics) / len(recent_metrics),
            'total_slow_queries': sum(m['queries']['slow_queries'].get('slow_query_count', 0) for m in recent_metrics),
            'metrics_collected': len(recent_metrics)
        }
