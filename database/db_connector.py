from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from database.tables import metadata
from settings.config import database_url, PG_POOL_SIZE


class DBConnector:
    def __init__(self, url: str | None = None):
        self._engine: AsyncEngine = create_async_engine(
            url or database_url(),
            echo=False,
            pool_pre_ping=True,
            pool_size=PG_POOL_SIZE,
        )

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    def connect(self):
        return self._engine.connect()

    def begin(self):
        return self._engine.begin()

    async def create_all(self):
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def dispose(self):
        await self._engine.dispose()
