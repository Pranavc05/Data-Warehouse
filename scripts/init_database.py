"""
Database initialization script
"""

import asyncio
import logging
from src.database.connection import init_database, test_connection, engine
from src.database.models import Base
import asyncpg
from config import DATABASE_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def initialize_database():
    """Initialize the database with schema and sample data"""
    
    logger.info("🚀 Initializing Intelligent Data Warehouse Orchestrator Database")
    
    # Test connection
    if not test_connection():
        logger.error("❌ Database connection failed")
        return False
    
    logger.info("✅ Database connection successful")
    
    # Create tables
    logger.info("📊 Creating database schema...")
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Database schema created")
    
    # Execute advanced schema SQL
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Read and execute schema files
        with open('/Users/chandrasekhargopal/DataForge/sql/schemas/advanced_warehouse_schema.sql', 'r') as f:
            schema_sql = f.read()
        
        logger.info("🏗️ Executing advanced schema creation...")
        await conn.execute(schema_sql)
        logger.info("✅ Advanced schema created")
        
        # Load sample data
        with open('/Users/chandrasekhargopal/DataForge/sql/schemas/sample_data.sql', 'r') as f:
            sample_data_sql = f.read()
        
        logger.info("📈 Loading sample data...")
        await conn.execute(sample_data_sql)
        logger.info("✅ Sample data loaded")
        
        await conn.close()
        
    except Exception as e:
        logger.error(f"❌ Schema/data initialization failed: {e}")
        return False
    
    logger.info("🎯 Database initialization completed successfully!")
    logger.info("🚀 Ready for Autodesk-level demonstrations!")
    
    return True

if __name__ == "__main__":
    asyncio.run(initialize_database())
