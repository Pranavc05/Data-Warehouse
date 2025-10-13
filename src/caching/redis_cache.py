"""
Advanced Redis Caching Layer for High-Performance Query Optimization
"""

import asyncio
import aioredis
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import logging
import pickle
import zlib
from dataclasses import dataclass

from src.services.error_monitoring import error_monitor
from config import REDIS_URL

logger = logging.getLogger(__name__)

@dataclass
class CacheStats:
    hits: int
    misses: int
    total_requests: int
    hit_rate: float
    avg_response_time_ms: float

class AdvancedRedisCache:
    """
    Enterprise-grade Redis caching system with intelligent cache strategies
    """
    
    def __init__(self):
        self.redis_url = REDIS_URL
        self.redis_client = None
        self.is_connected = False
        
        # Cache configuration
        self.cache_config = {
            'query_results': {'ttl': 3600, 'compress': True},       # 1 hour
            'dashboard_data': {'ttl': 300, 'compress': False},      # 5 minutes
            'user_sessions': {'ttl': 86400, 'compress': False},     # 24 hours
            'ml_predictions': {'ttl': 7200, 'compress': True},      # 2 hours
            'analytics_summary': {'ttl': 1800, 'compress': True},   # 30 minutes
            'real_time_metrics': {'ttl': 60, 'compress': False}     # 1 minute
        }
        
        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'total_size_mb': 0
        }
    
    async def initialize(self) -> bool:
        """Initialize Redis connection"""
        try:
            self.redis_client = aioredis.from_url(
                self.redis_url,
                encoding='utf-8',
                decode_responses=False,  # We'll handle encoding manually
                max_connections=20,
                retry_on_timeout=True
            )
            
            # Test connection
            await self.redis_client.ping()
            self.is_connected = True
            
            logger.info("✅ Redis cache system initialized successfully")
            
            # Start background maintenance
            asyncio.create_task(self._background_maintenance())
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis: {e}")
            error_monitor.capture_exception(e, {"context": "redis_initialization"})
            self.is_connected = False
            return False
    
    async def get_query_result(self, query_hash: str) -> Optional[Dict]:
        """Get cached query result"""
        return await self._get_cached_data(f"query:{query_hash}", 'query_results')
    
    async def cache_query_result(self, query_hash: str, result: Dict, custom_ttl: Optional[int] = None) -> bool:
        """Cache query result with intelligent compression"""
        cache_key = f"query:{query_hash}"
        ttl = custom_ttl or self.cache_config['query_results']['ttl']
        
        # Add metadata
        cache_data = {
            'result': result,
            'cached_at': datetime.utcnow().isoformat(),
            'query_hash': query_hash,
            'result_count': len(result) if isinstance(result, (list, dict)) else 1
        }
        
        return await self._set_cached_data(cache_key, cache_data, 'query_results', ttl)
    
    async def get_dashboard_data(self, dashboard_id: str) -> Optional[Dict]:
        """Get cached dashboard data"""
        return await self._get_cached_data(f"dashboard:{dashboard_id}", 'dashboard_data')
    
    async def cache_dashboard_data(self, dashboard_id: str, data: Dict) -> bool:
        """Cache dashboard data"""
        cache_key = f"dashboard:{dashboard_id}"
        
        # Add real-time timestamp
        cache_data = {
            'data': data,
            'generated_at': datetime.utcnow().isoformat(),
            'dashboard_id': dashboard_id
        }
        
        return await self._set_cached_data(cache_key, cache_data, 'dashboard_data')
    
    async def get_ml_prediction(self, prediction_key: str) -> Optional[Dict]:
        """Get cached ML prediction"""
        return await self._get_cached_data(f"ml:{prediction_key}", 'ml_predictions')
    
    async def cache_ml_prediction(self, prediction_key: str, prediction: Dict) -> bool:
        """Cache ML prediction result"""
        cache_key = f"ml:{prediction_key}"
        
        cache_data = {
            'prediction': prediction,
            'predicted_at': datetime.utcnow().isoformat(),
            'model_version': prediction.get('model_version', '1.0.0')
        }
        
        return await self._set_cached_data(cache_key, cache_data, 'ml_predictions')
    
    async def get_real_time_metric(self, metric_name: str) -> Optional[Any]:
        """Get real-time metric"""
        return await self._get_cached_data(f"metric:{metric_name}", 'real_time_metrics')
    
    async def set_real_time_metric(self, metric_name: str, value: Any) -> bool:
        """Set real-time metric"""
        cache_key = f"metric:{metric_name}"
        
        cache_data = {
            'value': value,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return await self._set_cached_data(cache_key, cache_data, 'real_time_metrics')
    
    async def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        """Increment a counter atomically"""
        try:
            if not self.is_connected:
                return 0
            
            cache_key = f"counter:{counter_name}"
            result = await self.redis_client.incrby(cache_key, increment)
            
            # Set expiry for counters (24 hours)
            await self.redis_client.expire(cache_key, 86400)
            
            return result
            
        except Exception as e:
            logger.error(f"Counter increment failed: {e}")
            return 0
    
    async def get_sorted_set(self, set_name: str, start: int = 0, end: int = -1, reverse: bool = True) -> List[Tuple[str, float]]:
        """Get items from sorted set (for leaderboards, rankings)"""
        try:
            if not self.is_connected:
                return []
            
            cache_key = f"sorted_set:{set_name}"
            
            if reverse:
                result = await self.redis_client.zrevrange(cache_key, start, end, withscores=True)
            else:
                result = await self.redis_client.zrange(cache_key, start, end, withscores=True)
            
            return [(item.decode('utf-8'), score) for item, score in result]
            
        except Exception as e:
            logger.error(f"Sorted set retrieval failed: {e}")
            return []
    
    async def add_to_sorted_set(self, set_name: str, score: float, member: str) -> bool:
        """Add item to sorted set"""
        try:
            if not self.is_connected:
                return False
            
            cache_key = f"sorted_set:{set_name}"
            await self.redis_client.zadd(cache_key, {member: score})
            
            # Set expiry
            await self.redis_client.expire(cache_key, 86400)
            
            return True
            
        except Exception as e:
            logger.error(f"Sorted set addition failed: {e}")
            return False
    
    async def cache_user_session(self, user_id: str, session_data: Dict) -> bool:
        """Cache user session data"""
        cache_key = f"session:{user_id}"
        return await self._set_cached_data(cache_key, session_data, 'user_sessions')
    
    async def get_user_session(self, user_id: str) -> Optional[Dict]:
        """Get user session data"""
        return await self._get_cached_data(f"session:{user_id}", 'user_sessions')
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern"""
        try:
            if not self.is_connected:
                return 0
            
            keys = await self.redis_client.keys(pattern)
            if keys:
                deleted = await self.redis_client.delete(*keys)
                logger.info(f"🗑️ Invalidated {deleted} cache entries matching '{pattern}'")
                return deleted
            
            return 0
            
        except Exception as e:
            logger.error(f"Cache invalidation failed: {e}")
            return 0
    
    async def _get_cached_data(self, cache_key: str, cache_type: str) -> Optional[Any]:
        """Internal method to get cached data"""
        try:
            if not self.is_connected:
                return None
            
            start_time = datetime.utcnow()
            
            # Get from Redis
            cached_data = await self.redis_client.get(cache_key)
            
            if cached_data:
                # Decompress if needed
                config = self.cache_config[cache_type]
                if config['compress']:
                    try:
                        cached_data = zlib.decompress(cached_data)
                    except Exception:
                        pass  # Data might not be compressed
                
                # Deserialize
                data = pickle.loads(cached_data)
                
                # Update stats
                self.stats['hits'] += 1
                response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                logger.debug(f"✅ Cache hit: {cache_key} ({response_time:.1f}ms)")
                return data
            else:
                # Cache miss
                self.stats['misses'] += 1
                logger.debug(f"❌ Cache miss: {cache_key}")
                return None
                
        except Exception as e:
            logger.error(f"Cache retrieval failed for {cache_key}: {e}")
            self.stats['misses'] += 1
            return None
    
    async def _set_cached_data(self, cache_key: str, data: Any, cache_type: str, custom_ttl: Optional[int] = None) -> bool:
        """Internal method to set cached data"""
        try:
            if not self.is_connected:
                return False
            
            config = self.cache_config[cache_type]
            ttl = custom_ttl or config['ttl']
            
            # Serialize data
            serialized_data = pickle.dumps(data)
            
            # Compress if configured
            if config['compress']:
                serialized_data = zlib.compress(serialized_data, level=6)
            
            # Store in Redis
            await self.redis_client.setex(cache_key, ttl, serialized_data)
            
            logger.debug(f"💾 Cached: {cache_key} (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            logger.error(f"Cache storage failed for {cache_key}: {e}")
            return False
    
    async def get_cache_stats(self) -> CacheStats:
        """Get comprehensive cache statistics"""
        try:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0
            
            # Get Redis info
            redis_info = {}
            if self.is_connected:
                redis_info = await self.redis_client.info('memory')
            
            return CacheStats(
                hits=self.stats['hits'],
                misses=self.stats['misses'],
                total_requests=total_requests,
                hit_rate=hit_rate,
                avg_response_time_ms=1.5  # Simplified for demo
            )
            
        except Exception as e:
            logger.error(f"Cache stats retrieval failed: {e}")
            return CacheStats(0, 0, 0, 0.0, 0.0)
    
    async def _background_maintenance(self):
        """Background maintenance tasks"""
        while self.is_connected:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                # Log cache statistics
                stats = await self.get_cache_stats()
                logger.info(f"📊 Cache Stats: {stats.hit_rate:.1f}% hit rate, {stats.total_requests} requests")
                
                # Cleanup expired sessions (additional safety)
                expired_sessions = await self.redis_client.eval("""
                    local keys = redis.call('keys', 'session:*')
                    local count = 0
                    for i=1,#keys do
                        local ttl = redis.call('ttl', keys[i])
                        if ttl == -1 then
                            redis.call('expire', keys[i], 86400)
                            count = count + 1
                        end
                    end
                    return count
                """, 0)
                
                if expired_sessions > 0:
                    logger.info(f"🧹 Added expiry to {expired_sessions} session keys")
                
            except Exception as e:
                logger.error(f"Background maintenance error: {e}")
                await asyncio.sleep(60)  # Wait longer on error

# Global cache instance
redis_cache = AdvancedRedisCache()
