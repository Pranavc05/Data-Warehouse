"""
Database connection and ORM models for the Intelligent Data Warehouse Orchestrator
"""

from sqlalchemy import create_engine, MetaData, event, text
from contextlib import asynccontextmanager
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import asyncio
import asyncpg
from typing import AsyncGenerator
import logging

from config import DATABASE_URL, DB_POOL_SIZE, DB_MAX_OVERFLOW, DB_POOL_TIMEOUT, DB_POOL_RECYCLE

logger = logging.getLogger(__name__)

# SQLAlchemy setup
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT,
    pool_recycle=DB_POOL_RECYCLE,
    echo=False  # Set to True for SQL debugging
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database connection management
def get_db() -> Session:
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@asynccontextmanager
async def get_async_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """Async context manager for database connection"""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()

# Query monitoring setup
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Log queries for performance monitoring"""
    context._query_start_time = asyncio.get_event_loop().time()
    logger.debug(f"Executing query: {statement[:100]}...")

@event.listens_for(engine, "after_cursor_execute")
def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Log query completion and performance metrics"""
    total = asyncio.get_event_loop().time() - context._query_start_time
    logger.debug(f"Query completed in {total:.4f}s")
    
    # TODO: Send metrics to monitoring system
    if total > 1.0:  # Log slow queries
        logger.warning(f"Slow query detected ({total:.4f}s): {statement[:200]}...")

def init_database():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)
    logger.info("Database initialized successfully")

def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False
