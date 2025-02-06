import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database type
DB_TYPE = os.getenv("DB_TYPE", "sqlite")

# SQLite settings
DB_PATH = os.getenv("DB_PATH", "arbitrage.db")

# PostgreSQL settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "arbitrage")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Connection pool settings (PostgreSQL only)
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))

# Debug settings
DB_ECHO = os.getenv("DB_ECHO", "False").lower() == "true"

# Construct database URL based on type
if DB_TYPE == "sqlite":
    DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"
else:
    DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}" 