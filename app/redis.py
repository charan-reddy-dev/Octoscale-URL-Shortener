import time
from typing import AsyncGenerator, Optional

import redis.asyncio as aioredis

from app.config import settings
from app.logger import logger


class RedisManager:
    """Owns the shared Redis connection pool and a short circuit breaker for outages."""

    def __init__(self):
        self.redis: Optional[aioredis.Redis] = None
        self._down_until = 0.0

    def initialize(self):
        """Create the connection pool. Connections are opened lazily on first use."""
        pool = aioredis.ConnectionPool.from_url(
            settings.REDIS_URL,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            socket_timeout=settings.REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=settings.REDIS_SOCKET_TIMEOUT,
            health_check_interval=30,
            decode_responses=True,
            # RESP2 works with every Redis server version (RESP3 needs Redis 6+).
            protocol=2,
        )
        self.redis = aioredis.Redis(connection_pool=pool)
        logger.info("redis_pool_created", url=settings.REDIS_URL, max_connections=settings.REDIS_MAX_CONNECTIONS)

    async def close(self):
        if self.redis:
            await self.redis.aclose()
            logger.info("redis_disconnected")

    async def get_client(self) -> aioredis.Redis:
        if not self.redis:
            self.initialize()
        return self.redis

    @property
    def available(self) -> bool:
        return time.monotonic() >= self._down_until

    def mark_down(self, error: Exception) -> None:
        if self.available:
            logger.warning("redis_unavailable", error=str(error), retry_in_s=settings.REDIS_RETRY_AFTER_SECONDS)
        self._down_until = time.monotonic() + settings.REDIS_RETRY_AFTER_SECONDS

    def mark_up(self) -> None:
        self._down_until = 0.0


redis_manager = RedisManager()


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    client = await redis_manager.get_client()
    yield client
