import os

from dotenv import load_dotenv

load_dotenv()

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8080"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "pass")
PG_DB = os.getenv("PG_DB", "inventory")
PG_POOL_SIZE = int(os.getenv("PG_POOL_SIZE", "3"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_POOL_SIZE = int(os.getenv("REDIS_POOL_SIZE", "3"))

USE_CONTAINERS_FOR_TESTS = (
    os.getenv("USE_CONTAINERS_FOR_TESTS", "false").lower() == "true"
)

RECEIPT_CHANNEL = "inventory.receipt"
ISSUE_CHANNEL = "inventory.issue"
CHANNELS = (RECEIPT_CHANNEL, ISSUE_CHANNEL)


def database_url(host=None, port=None, user=None, password=None, db=None):
    return (
        f"postgresql+asyncpg://{user or PG_USER}:{password or PG_PASSWORD}"
        f"@{host or PG_HOST}:{port or PG_PORT}/{db or PG_DB}"
    )


def redis_url(host=None, port=None, db=None):
    return f"redis://{host or REDIS_HOST}:{port or REDIS_PORT}/{db or REDIS_DB}"
