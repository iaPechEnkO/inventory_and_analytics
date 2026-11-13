import asyncio
from aiohttp import web

from connections.redis_connector import RedisConnector
from database.db_connector import DBConnector
from inventory_api.api import receipt, issue, stock, summary
from settings.config import API_HOST, API_PORT
from settings.logger import setup_logger

logger = setup_logger(__name__)


async def create_app() -> web.Application:
    app = web.Application()

    logger.info("Trying to create database and redis connectors...")
    try:
        db = DBConnector()
        redis_conn = RedisConnector()

        app["db"] = db
        app["redis"] = redis_conn
    except Exception as err:
        error_message = "An error occurred during database or redis connectors creation"
        logger.error(f"{error_message}: {err}")
        raise

    app.router.add_post("/receipts", receipt)
    app.router.add_post("/issues", issue)
    app.router.add_get("/stock", stock)
    app.router.add_get("/stock/summary", summary)

    async def cleanup(app):
        await db.dispose()
        await redis_conn.close()

    app.on_cleanup.append(cleanup)
    return app


async def main(host: str = API_HOST, port: int = API_PORT):
    app = await create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"inventory_api on {host}:{port}")
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
