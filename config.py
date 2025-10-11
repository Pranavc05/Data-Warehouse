import os
from pathlib import Path

# Environment Variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://autosql_user:autosql_password@localhost:5432/autosql_warehouse")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Project Paths
PROJECT_ROOT = Path(__file__).parent
SQL_DIR = PROJECT_ROOT / "sql"
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"

# AI Configuration
AI_MODEL = "gpt-4-turbo-preview"
AI_TEMPERATURE = 0.1
MAX_TOKENS = 4000

# Database Configuration
DB_POOL_SIZE = 20
DB_MAX_OVERFLOW = 30
DB_POOL_TIMEOUT = 30
DB_POOL_RECYCLE = 3600

# Query Performance Thresholds
SLOW_QUERY_THRESHOLD = 1000  # milliseconds
EXPENSIVE_QUERY_THRESHOLD = 10000  # cost units
INDEX_SUGGESTION_THRESHOLD = 5  # number of slow queries before suggesting index

# Cost Optimization Settings
COST_ANALYSIS_INTERVAL = 3600  # seconds
PARTITION_SIZE_THRESHOLD = 1000000  # rows
ARCHIVE_AGE_THRESHOLD = 365  # days

# Monitoring Configuration
METRICS_PORT = 8080
HEALTH_CHECK_INTERVAL = 30  # seconds
ALERT_COOLDOWN = 300  # seconds

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Create directories
for directory in [LOGS_DIR, DATA_DIR]:
    directory.mkdir(exist_ok=True)
