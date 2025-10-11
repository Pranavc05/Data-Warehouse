"""
Advanced database models for the data warehouse
"""

from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Text, ForeignKey, Index, CheckConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from datetime import datetime
import uuid

from .connection import Base

class Customer(Base):
    """Customer dimension table with advanced indexing"""
    __tablename__ = "customers"
    
    customer_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code = Column(String(50), unique=True, nullable=False, index=True)
    company_name = Column(String(200), nullable=False)
    industry = Column(String(100), index=True)
    region = Column(String(100), index=True)
    country = Column(String(100), index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True, index=True)
    customer_tier = Column(String(20), index=True)  # Premium, Standard, Basic
    metadata_json = Column(JSONB)
    
    # Relationships
    orders = relationship("Order", back_populates="customer")
    analytics = relationship("CustomerAnalytics", back_populates="customer")
    
    # Advanced indexing for query optimization
    __table_args__ = (
        Index('idx_customer_industry_region', 'industry', 'region'),
        Index('idx_customer_active_tier', 'is_active', 'customer_tier'),
        Index('idx_customer_created_month', 'created_at'),  # Partitioning hint
    )

class Product(Base):
    """Product dimension with hierarchical categories"""
    __tablename__ = "products"
    
    product_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sku = Column(String(100), unique=True, nullable=False, index=True)
    product_name = Column(String(300), nullable=False)
    category = Column(String(100), index=True)
    subcategory = Column(String(100), index=True)
    brand = Column(String(100), index=True)
    unit_cost = Column(Float, nullable=False)
    unit_price = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True, index=True)
    attributes = Column(JSONB)  # Flexible product attributes
    
    # Relationships
    order_items = relationship("OrderItem", back_populates="product")
    inventory = relationship("InventoryLevel", back_populates="product")
    
    __table_args__ = (
        Index('idx_product_category_brand', 'category', 'brand'),
        Index('idx_product_price_range', 'unit_price'),
        CheckConstraint('unit_price > 0', name='positive_price'),
        CheckConstraint('unit_cost >= 0', name='non_negative_cost'),
    )

class Order(Base):
    """Order fact table with partitioning support"""
    __tablename__ = "orders"
    
    order_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_number = Column(String(100), unique=True, nullable=False, index=True)
    customer_id = Column(UUID(as_uuid=True), ForeignKey('customers.customer_id'), nullable=False, index=True)
    order_date = Column(DateTime, nullable=False, index=True)  # Partition key
    ship_date = Column(DateTime, index=True)
    order_status = Column(String(50), nullable=False, index=True)
    total_amount = Column(Float, nullable=False)
    discount_amount = Column(Float, default=0)
    tax_amount = Column(Float, default=0)
    shipping_cost = Column(Float, default=0)
    sales_rep_id = Column(String(50), index=True)
    order_source = Column(String(50), index=True)  # Online, Phone, Store
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    customer = relationship("Customer", back_populates="orders")
    order_items = relationship("OrderItem", back_populates="order")
    
    __table_args__ = (
        Index('idx_order_date_status', 'order_date', 'order_status'),
        Index('idx_order_customer_date', 'customer_id', 'order_date'),
        Index('idx_order_amount_range', 'total_amount'),
        # Partitioning hint - would be implemented at database level
    )

class OrderItem(Base):
    """Order line items with detailed analytics"""
    __tablename__ = "order_items"
    
    item_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), ForeignKey('orders.order_id'), nullable=False, index=True)
    product_id = Column(UUID(as_uuid=True), ForeignKey('products.product_id'), nullable=False, index=True)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    discount_percent = Column(Float, default=0)
    line_total = Column(Float, nullable=False)
    
    # Relationships
    order = relationship("Order", back_populates="order_items")
    product = relationship("Product", back_populates="order_items")
    
    __table_args__ = (
        Index('idx_orderitem_order_product', 'order_id', 'product_id'),
        CheckConstraint('quantity > 0', name='positive_quantity'),
        CheckConstraint('unit_price > 0', name='positive_unit_price'),
    )

class InventoryLevel(Base):
    """Inventory tracking with time-series data"""
    __tablename__ = "inventory_levels"
    
    inventory_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_id = Column(UUID(as_uuid=True), ForeignKey('products.product_id'), nullable=False, index=True)
    warehouse_location = Column(String(100), nullable=False, index=True)
    quantity_on_hand = Column(Integer, nullable=False)
    quantity_reserved = Column(Integer, default=0)
    reorder_point = Column(Integer, nullable=False)
    max_stock_level = Column(Integer, nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    product = relationship("Product", back_populates="inventory")
    
    __table_args__ = (
        Index('idx_inventory_product_location', 'product_id', 'warehouse_location'),
        Index('idx_inventory_updated', 'last_updated'),
        CheckConstraint('quantity_on_hand >= 0', name='non_negative_quantity'),
    )

class CustomerAnalytics(Base):
    """Pre-computed customer analytics for fast querying"""
    __tablename__ = "customer_analytics"
    
    analytics_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), ForeignKey('customers.customer_id'), nullable=False, index=True)
    analysis_date = Column(DateTime, nullable=False, index=True)
    total_orders = Column(Integer, default=0)
    total_revenue = Column(Float, default=0)
    avg_order_value = Column(Float, default=0)
    days_since_last_order = Column(Integer)
    customer_lifetime_value = Column(Float, default=0)
    churn_probability = Column(Float, default=0)  # AI prediction
    preferred_categories = Column(ARRAY(String))
    seasonality_pattern = Column(JSONB)  # AI-generated insights
    
    # Relationships
    customer = relationship("Customer", back_populates="analytics")
    
    __table_args__ = (
        Index('idx_analytics_customer_date', 'customer_id', 'analysis_date'),
        Index('idx_analytics_clv', 'customer_lifetime_value'),
        Index('idx_analytics_churn', 'churn_probability'),
    )

class QueryPerformanceLog(Base):
    """Query performance monitoring and optimization"""
    __tablename__ = "query_performance_log"
    
    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    query_hash = Column(String(64), nullable=False, index=True)
    query_text = Column(Text, nullable=False)
    execution_time_ms = Column(Float, nullable=False, index=True)
    rows_returned = Column(Integer)
    query_cost = Column(Float)
    execution_plan = Column(JSONB)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    user_id = Column(String(100), index=True)
    database_name = Column(String(100), index=True)
    
    __table_args__ = (
        Index('idx_perf_hash_time', 'query_hash', 'timestamp'),
        Index('idx_perf_execution_time', 'execution_time_ms'),
        Index('idx_perf_cost', 'query_cost'),
    )

class AIOptimizationSuggestion(Base):
    """AI-generated optimization suggestions"""
    __tablename__ = "ai_optimization_suggestions"
    
    suggestion_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    suggestion_type = Column(String(50), nullable=False, index=True)  # INDEX, PARTITION, QUERY_REWRITE
    target_table = Column(String(100), index=True)
    target_query_hash = Column(String(64), index=True)
    suggestion_text = Column(Text, nullable=False)
    estimated_improvement = Column(Float)  # Percentage improvement
    implementation_sql = Column(Text)
    confidence_score = Column(Float, index=True)
    status = Column(String(20), default="PENDING", index=True)  # PENDING, APPLIED, REJECTED
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    applied_at = Column(DateTime)
    
    __table_args__ = (
        Index('idx_suggestion_type_status', 'suggestion_type', 'status'),
        Index('idx_suggestion_confidence', 'confidence_score'),
    )
