"""
Enterprise Real-Time Data Streaming System with Apache Kafka
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum
import logging
from concurrent.futures import ThreadPoolExecutor
import threading

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import asyncpg

from src.database.connection import get_async_db
from src.services.error_monitoring import error_monitor
from config import KAFKA_BOOTSTRAP_SERVERS, REDIS_URL

logger = logging.getLogger(__name__)

class StreamEventType(Enum):
    ORDER_CREATED = "order_created"
    ORDER_UPDATED = "order_updated"
    CUSTOMER_ACTIVITY = "customer_activity"
    QUERY_EXECUTED = "query_executed"
    PERFORMANCE_METRIC = "performance_metric"
    SYSTEM_ALERT = "system_alert"
    DATA_QUALITY_CHECK = "data_quality_check"

@dataclass
class StreamEvent:
    event_type: StreamEventType
    event_id: str
    timestamp: datetime
    data: Dict[str, Any]
    source: str
    correlation_id: Optional[str] = None

class KafkaStreamProcessor:
    """
    Advanced real-time data streaming with Apache Kafka
    """
    
    def __init__(self):
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS or 'localhost:9092'
        self.producer = None
        self.consumers = {}
        self.is_running = False
        
        # Topic configuration
        self.topics = {
            'orders': {
                'name': 'autosql.orders',
                'partitions': 3,
                'replication_factor': 1,
                'config': {'retention.ms': 604800000}  # 7 days
            },
            'analytics': {
                'name': 'autosql.analytics', 
                'partitions': 6,
                'replication_factor': 1,
                'config': {'retention.ms': 2592000000}  # 30 days
            },
            'performance': {
                'name': 'autosql.performance',
                'partitions': 2,
                'replication_factor': 1,
                'config': {'retention.ms': 259200000}  # 3 days
            },
            'alerts': {
                'name': 'autosql.alerts',
                'partitions': 1,
                'replication_factor': 1,
                'config': {'retention.ms': 2592000000}  # 30 days
            }
        }
        
        # Event handlers
        self.event_handlers = {}
        
    async def initialize(self):
        """Initialize Kafka producer and create topics"""
        try:
            # Initialize producer
            self.producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                batch_size=16384,
                linger_ms=10,  # Wait up to 10ms to batch messages
                compression_type='gzip'
            )
            
            # Create topics if they don't exist
            await self._create_topics()
            
            # Register default event handlers
            self._register_default_handlers()
            
            logger.info("✅ Kafka streaming system initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka: {e}")
            error_monitor.capture_exception(e, {"context": "kafka_initialization"})
            return False
    
    async def _create_topics(self):
        """Create Kafka topics if they don't exist"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=[self.bootstrap_servers],
                client_id='autosql_admin'
            )
            
            # Get existing topics
            existing_topics = admin_client.list_topics()
            
            # Create missing topics
            topics_to_create = []
            for topic_key, topic_config in self.topics.items():
                if topic_config['name'] not in existing_topics:
                    topic = NewTopic(
                        name=topic_config['name'],
                        num_partitions=topic_config['partitions'],
                        replication_factor=topic_config['replication_factor'],
                        topic_configs=topic_config.get('config', {})
                    )
                    topics_to_create.append(topic)
            
            if topics_to_create:
                admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
                logger.info(f"📝 Created {len(topics_to_create)} Kafka topics")
            
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka topics: {e}")
    
    async def publish_event(self, event: StreamEvent) -> bool:
        """Publish event to appropriate Kafka topic"""
        try:
            if not self.producer:
                logger.error("Kafka producer not initialized")
                return False
            
            # Determine topic based on event type
            topic_name = self._get_topic_for_event(event.event_type)
            
            # Prepare message
            message = {
                'event_id': event.event_id,
                'event_type': event.event_type.value,
                'timestamp': event.timestamp.isoformat(),
                'data': event.data,
                'source': event.source,
                'correlation_id': event.correlation_id
            }
            
            # Send to Kafka
            future = self.producer.send(
                topic=topic_name,
                key=event.event_id,
                value=message,
                partition=self._calculate_partition(event.event_id, event.event_type)
            )
            
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"✅ Published event {event.event_id} to {topic_name} (partition {record_metadata.partition})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to publish event {event.event_id}: {e}")
            error_monitor.capture_exception(e, {
                "context": "kafka_publish",
                "event_id": event.event_id,
                "event_type": event.event_type.value
            })
            return False
    
    async def start_consumers(self):
        """Start consuming from all topics"""
        self.is_running = True
        
        # Start consumers for each topic
        consumer_tasks = []
        for topic_key, topic_config in self.topics.items():
            task = asyncio.create_task(
                self._consume_topic(topic_config['name'], f"autosql_{topic_key}_consumer")
            )
            consumer_tasks.append(task)
        
        logger.info("🚀 Started Kafka consumers for all topics")
        
        # Wait for all consumers (runs indefinitely)
        try:
            await asyncio.gather(*consumer_tasks)
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.is_running = False
    
    async def _consume_topic(self, topic_name: str, consumer_group: str):
        """Consume messages from a specific topic"""
        consumer = None
        try:
            # Create consumer
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=[self.bootstrap_servers],
                group_id=consumer_group,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                consumer_timeout_ms=1000,  # 1 second timeout
                max_poll_records=100
            )
            
            logger.info(f"👂 Started consuming from topic: {topic_name}")
            
            while self.is_running:
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            await self._process_message(message)
                    
                    # Commit offsets
                    consumer.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing messages from {topic_name}: {e}")
                    await asyncio.sleep(1)  # Brief pause before retrying
                    
        except Exception as e:
            logger.error(f"❌ Consumer error for {topic_name}: {e}")
            error_monitor.capture_exception(e, {"context": "kafka_consumer", "topic": topic_name})
        finally:
            if consumer:
                consumer.close()
    
    async def _process_message(self, message):
        """Process incoming Kafka message"""
        try:
            event_data = message.value
            event_type = StreamEventType(event_data['event_type'])
            
            # Reconstruct StreamEvent
            event = StreamEvent(
                event_type=event_type,
                event_id=event_data['event_id'],
                timestamp=datetime.fromisoformat(event_data['timestamp']),
                data=event_data['data'],
                source=event_data['source'],
                correlation_id=event_data.get('correlation_id')
            )
            
            # Call registered handlers
            handlers = self.event_handlers.get(event_type, [])
            for handler in handlers:
                try:
                    await handler(event)
                except Exception as e:
                    logger.error(f"Event handler failed for {event_type.value}: {e}")
            
            # Log successful processing
            logger.debug(f"✅ Processed event {event.event_id} of type {event_type.value}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process message: {e}")
            error_monitor.capture_exception(e, {"context": "message_processing"})
    
    def register_event_handler(self, event_type: StreamEventType, handler: Callable):
        """Register handler for specific event type"""
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        
        self.event_handlers[event_type].append(handler)
        logger.info(f"📋 Registered handler for {event_type.value}")
    
    def _register_default_handlers(self):
        """Register default event handlers"""
        
        # Order processing handler
        async def handle_order_event(event: StreamEvent):
            """Process order-related events"""
            try:
                async with get_async_db() as conn:
                    if event.event_type == StreamEventType.ORDER_CREATED:
                        # Real-time order processing
                        await conn.execute("""
                            INSERT INTO real_time_events (event_type, event_data, processed_at)
                            VALUES ('order_created', $1, CURRENT_TIMESTAMP)
                        """, json.dumps(event.data))
                        
                        # Trigger real-time analytics update
                        await self._update_real_time_analytics('orders', event.data)
                        
            except Exception as e:
                logger.error(f"Order event handler failed: {e}")
        
        # Performance monitoring handler
        async def handle_performance_event(event: StreamEvent):
            """Process performance monitoring events"""
            try:
                if event.event_type == StreamEventType.QUERY_EXECUTED:
                    # Store performance metrics
                    async with get_async_db() as conn:
                        await conn.execute("""
                            INSERT INTO query_performance_log 
                            (query_hash, query_text, execution_time_ms, rows_returned, timestamp)
                            VALUES ($1, $2, $3, $4, $5)
                        """, 
                        event.data.get('query_hash'),
                        event.data.get('query_text'),
                        event.data.get('execution_time_ms'),
                        event.data.get('rows_returned'),
                        event.timestamp
                        )
                    
                    # Check for performance issues
                    if event.data.get('execution_time_ms', 0) > 5000:
                        await self._trigger_performance_alert(event.data)
                        
            except Exception as e:
                logger.error(f"Performance event handler failed: {e}")
        
        # Register handlers
        self.register_event_handler(StreamEventType.ORDER_CREATED, handle_order_event)
        self.register_event_handler(StreamEventType.ORDER_UPDATED, handle_order_event)
        self.register_event_handler(StreamEventType.QUERY_EXECUTED, handle_performance_event)
    
    async def _update_real_time_analytics(self, metric_type: str, data: Dict):
        """Update real-time analytics dashboard"""
        try:
            # This would integrate with Redis for real-time dashboard updates
            logger.info(f"📊 Updating real-time analytics: {metric_type}")
            
            # Store in time-series format for dashboard
            timestamp = datetime.utcnow()
            metric_key = f"realtime:{metric_type}:{timestamp.strftime('%Y%m%d_%H%M')}"
            
            # Would store in Redis with TTL
            # redis_client.setex(metric_key, 3600, json.dumps(data))
            
        except Exception as e:
            logger.error(f"Real-time analytics update failed: {e}")
    
    async def _trigger_performance_alert(self, performance_data: Dict):
        """Trigger alert for performance issues"""
        try:
            alert_event = StreamEvent(
                event_type=StreamEventType.SYSTEM_ALERT,
                event_id=f"perf_alert_{datetime.utcnow().timestamp()}",
                timestamp=datetime.utcnow(),
                data={
                    'alert_type': 'performance_degradation',
                    'severity': 'warning',
                    'query_hash': performance_data.get('query_hash'),
                    'execution_time': performance_data.get('execution_time_ms'),
                    'threshold_exceeded': True
                },
                source='performance_monitor'
            )
            
            await self.publish_event(alert_event)
            
        except Exception as e:
            logger.error(f"Performance alert failed: {e}")
    
    def _get_topic_for_event(self, event_type: StreamEventType) -> str:
        """Determine Kafka topic based on event type"""
        topic_mapping = {
            StreamEventType.ORDER_CREATED: 'autosql.orders',
            StreamEventType.ORDER_UPDATED: 'autosql.orders',
            StreamEventType.CUSTOMER_ACTIVITY: 'autosql.analytics',
            StreamEventType.QUERY_EXECUTED: 'autosql.performance',
            StreamEventType.PERFORMANCE_METRIC: 'autosql.performance',
            StreamEventType.SYSTEM_ALERT: 'autosql.alerts',
            StreamEventType.DATA_QUALITY_CHECK: 'autosql.analytics'
        }
        
        return topic_mapping.get(event_type, 'autosql.analytics')
    
    def _calculate_partition(self, event_id: str, event_type: StreamEventType) -> Optional[int]:
        """Calculate partition for better distribution"""
        try:
            topic_name = self._get_topic_for_event(event_type)
            topic_config = None
            
            for config in self.topics.values():
                if config['name'] == topic_name:
                    topic_config = config
                    break
            
            if topic_config:
                # Hash-based partitioning
                partition = hash(event_id) % topic_config['partitions']
                return partition
            
        except Exception:
            pass
        
        return None  # Let Kafka decide
    
    async def stop(self):
        """Gracefully stop the streaming system"""
        self.is_running = False
        
        if self.producer:
            self.producer.close()
        
        logger.info("🛑 Kafka streaming system stopped")

# Global streaming instance
stream_processor = KafkaStreamProcessor()

# Convenience functions for publishing common events
async def publish_order_created(order_data: Dict) -> bool:
    """Publish order created event"""
    event = StreamEvent(
        event_type=StreamEventType.ORDER_CREATED,
        event_id=f"order_{order_data.get('order_id', datetime.utcnow().timestamp())}",
        timestamp=datetime.utcnow(),
        data=order_data,
        source='order_service'
    )
    return await stream_processor.publish_event(event)

async def publish_query_executed(query_data: Dict) -> bool:
    """Publish query execution event"""
    event = StreamEvent(
        event_type=StreamEventType.QUERY_EXECUTED,
        event_id=f"query_{datetime.utcnow().timestamp()}",
        timestamp=datetime.utcnow(),
        data=query_data,
        source='query_engine'
    )
    return await stream_processor.publish_event(event)

async def publish_system_alert(alert_data: Dict) -> bool:
    """Publish system alert event"""
    event = StreamEvent(
        event_type=StreamEventType.SYSTEM_ALERT,
        event_id=f"alert_{datetime.utcnow().timestamp()}",
        timestamp=datetime.utcnow(),
        data=alert_data,
        source='monitoring_system'
    )
    return await stream_processor.publish_event(event)
