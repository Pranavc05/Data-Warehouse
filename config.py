"""
Environment configuration loader with validation
"""

import os
from pathlib import Path
from typing import Optional
import logging

# Load environment variables from .env file
from dotenv import load_dotenv

# Get project root directory
PROJECT_ROOT = Path(__file__).parent
ENV_FILE = PROJECT_ROOT / '.env'

# Load .env file if it exists
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
    print(f"✅ Loaded environment variables from {ENV_FILE}")
else:
    print(f"⚠️  No .env file found at {ENV_FILE}")
    print(f"📝 Please copy .env.example to .env and configure your API keys")

# =============================================================================
# 🤖 AI CONFIGURATION - CRITICAL!
# =============================================================================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
if not OPENAI_API_KEY:
    print("🚨 CRITICAL: OPENAI_API_KEY not set!")
    print("💡 Get your API key from: https://platform.openai.com/api-keys")
    print("📝 Add it to your .env file: OPENAI_API_KEY=sk-...")

AI_MODEL = os.getenv("AI_MODEL", "gpt-4-turbo-preview")
AI_TEMPERATURE = float(os.getenv("AI_TEMPERATURE", "0.1"))
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "4000"))

# =============================================================================
# 🗄️ DATABASE CONFIGURATION
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://autosql_user:autosql_password@localhost:5432/autosql_warehouse")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Database pool settings
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "30"))
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))

# =============================================================================
# 🔐 SECURITY SETTINGS
# =============================================================================
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret-key-change-in-production")
API_RATE_LIMIT = int(os.getenv("API_RATE_LIMIT", "100"))

# =============================================================================
# 📊 MONITORING
# =============================================================================
SENTRY_DSN = os.getenv("SENTRY_DSN", "")
DATADOG_API_KEY = os.getenv("DATADOG_API_KEY", "")

# =============================================================================
# 🚀 APPLICATION SETTINGS
# =============================================================================
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEBUG = os.getenv("DEBUG", "true").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Project paths
SQL_DIR = PROJECT_ROOT / "sql"
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"

# Query performance thresholds
SLOW_QUERY_THRESHOLD = int(os.getenv("SLOW_QUERY_THRESHOLD", "1000"))  # milliseconds
EXPENSIVE_QUERY_THRESHOLD = int(os.getenv("EXPENSIVE_QUERY_THRESHOLD", "10000"))  # cost units
INDEX_SUGGESTION_THRESHOLD = int(os.getenv("INDEX_SUGGESTION_THRESHOLD", "5"))  # number of slow queries

# Cost optimization settings
COST_ANALYSIS_INTERVAL = int(os.getenv("COST_ANALYSIS_INTERVAL", "3600"))  # seconds
PARTITION_SIZE_THRESHOLD = int(os.getenv("PARTITION_SIZE_THRESHOLD", "1000000"))  # rows
ARCHIVE_AGE_THRESHOLD = int(os.getenv("ARCHIVE_AGE_THRESHOLD", "365"))  # days

# Monitoring configuration
METRICS_PORT = int(os.getenv("METRICS_PORT", "8080"))
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))  # seconds
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", "300"))  # seconds

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Create necessary directories
for directory in [LOGS_DIR, DATA_DIR]:
    directory.mkdir(exist_ok=True)

# Configuration validation
def validate_config() -> bool:
    """Validate critical configuration settings"""
    issues = []
    
    if not OPENAI_API_KEY:
        issues.append("🚨 OPENAI_API_KEY is required for AI features")
    
    if ENVIRONMENT == "production" and JWT_SECRET_KEY == "dev-secret-key-change-in-production":
        issues.append("🚨 JWT_SECRET_KEY must be changed in production")
    
    if issues:
        print("\n" + "="*60)
        print("🚨 CONFIGURATION ISSUES DETECTED:")
        for issue in issues:
            print(f"   {issue}")
        print("="*60)
        return False
    
    return True

# Validate on import
if __name__ == "__main__":
    print("\n🔧 Configuration Status:")
    print(f"   Environment: {ENVIRONMENT}")
    print(f"   Debug Mode: {DEBUG}")
    print(f"   AI Model: {AI_MODEL}")
    print(f"   Database: {'✅ Configured' if 'localhost' in DATABASE_URL else '✅ External DB'}")
    print(f"   OpenAI API: {'✅ Set' if OPENAI_API_KEY else '❌ Missing'}")
    print(f"   Redis: {'✅ Configured' if 'localhost' in REDIS_URL else '✅ External Redis'}")
    
    if validate_config():
        print("\n✅ Configuration is valid!")
    else:
        print("\n❌ Please fix configuration issues above")