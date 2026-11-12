import asyncio

import redis.asyncio as redis

from settings.config import REDIS_POOL_SIZE, redis_url
from settings.logger import setup_logger

logger = setup_logger(__name__)


class RedisConnector:
    def __init__(self, url: str | None = None):
        self._client = redis.from_url(
            url or redis_url(),
            decode_responses=True,
            max_connections=REDIS_POOL_SIZE,
        )

    @property
    def client(self):
        return self._client

    async def publish(self, channel: str, message: str) -> int:
        return await self._client.publish(channel, message)

    async def publish_with_retry(
        self, channel: str, message: str, attempts: int = 3
    ) -> None:
        for i in range(attempts):
            try:
                await self.publish(channel, message)
                return
            except Exception as err:
                if i == attempts - 1:
                    logger.error(f"[publish] failed after {attempts} attempts: {err}")
                    raise
                logger.warning(f"[publish] attempt {i+1} failed: {err}, retrying")
                await asyncio.sleep(0.1 * (2**i))

    def pubsub(self):
        return self._client.pubsub()

    async def close(self):
        await self._client.aclose()
