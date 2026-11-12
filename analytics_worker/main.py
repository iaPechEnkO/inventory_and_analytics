import asyncio

from analytics_worker.worker_logic import run_worker
from connections.redis_connector import RedisConnector
from database.db_connector import DBConnector
from settings.logger import setup_logger

logger = setup_logger(__name__)


async def main():
    db = DBConnector()
    redis_conn = RedisConnector()
    try:
        await run_worker(db, redis_conn)
    finally:
        await redis_conn.close()
        await db.dispose()


if __name__ == "__main__":
    asyncio.run(main())
