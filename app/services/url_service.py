from typing import Optional, Tuple

import redis.asyncio as aioredis
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.logger import logger
from app.models.url import URL
from app.redis import redis_manager
from app.repositories.url_repository import URLRepository

BASE62_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
COUNTER_START = 10000000  # Codes stay 4 characters until the counter passes 62^4 (14,776,336).
COUNTER_KEY = "url_id_counter"


class ShortCodeUnavailable(Exception):
    """Raised when a new short code cannot be allocated because Redis is down."""


def encode_base62(num: int) -> str:
    """Encode an integer to a Base62 string."""
    if num == 0:
        return BASE62_ALPHABET[0]
    arr = []
    while num:
        num, rem = divmod(num, 62)
        arr.append(BASE62_ALPHABET[rem])
    arr.reverse()
    return "".join(arr)


class URLService:
    def __init__(self, repository: URLRepository, redis_client: aioredis.Redis):
        self.repo = repository
        self.redis = redis_client

    # ---------- cache helpers: Redis is an accelerator, never a hard dependency for reads ----------

    async def _cache_get(self, key: str) -> Optional[str]:
        if not redis_manager.available:
            return None
        try:
            return await self.redis.get(key)
        except (RedisError, OSError) as exc:
            redis_manager.mark_down(exc)
            return None

    async def _cache_mapping(self, short_code: str, original_url: str) -> None:
        if not redis_manager.available:
            return
        try:
            ttl = settings.CACHE_TTL_SECONDS
            async with self.redis.pipeline(transaction=False) as pipe:
                pipe.set(f"code:{short_code}", original_url, ex=ttl)
                pipe.set(f"orig:{original_url}", short_code, ex=ttl)
                await pipe.execute()
        except (RedisError, OSError) as exc:
            redis_manager.mark_down(exc)

    # ---------- short-code allocation ----------

    async def _get_next_id(self) -> int:
        """Allocate the next ID from the Redis counter.

        If the counter is missing (first run, or Redis lost its data) it is seeded from the highest ID
        in the database with SET NX, so concurrent first requests cannot reset it after another
        request has already incremented it. INCR is atomic, so every caller gets a distinct ID.
        """
        try:
            if not await self.redis.exists(COUNTER_KEY):
                result = await self.repo.db.execute(select(func.max(URL.id)))
                seed = max(result.scalar() or 0, COUNTER_START)
                if await self.redis.set(COUNTER_KEY, seed, nx=True):
                    logger.info("initialized_redis_counter", value=seed)
            return int(await self.redis.incr(COUNTER_KEY))
        except (RedisError, OSError) as exc:
            redis_manager.mark_down(exc)
            raise ShortCodeUnavailable("Short-code allocation is temporarily unavailable") from exc

    # ---------- use cases ----------

    async def shorten_url(self, original_url: str, custom_code: Optional[str] = None) -> Tuple[URL, bool]:
        """Shorten a URL. Returns (URL, created_new); an existing mapping for the same URL is reused."""
        if custom_code:
            if await self._cache_get(f"code:{custom_code}") or await self.repo.get_by_short_code(custom_code):
                raise ValueError("Custom code already in use")
            try:
                created_url = await self.repo.create(URL(original_url=original_url, short_code=custom_code))
            except IntegrityError as exc:
                # Another request claimed the same code between the check and the insert.
                await self.repo.db.rollback()
                raise ValueError("Custom code already in use") from exc
            await self._cache_mapping(custom_code, original_url)
            logger.info("url_shortened_custom", short_code=custom_code)
            return created_url, True

        cached_code = await self._cache_get(f"orig:{original_url}")
        if cached_code:
            existing = await self.repo.get_by_short_code(cached_code)
            if existing:
                return existing, False

        existing = await self.repo.get_by_original_url(original_url)
        if existing:
            await self._cache_mapping(existing.short_code, original_url)
            return existing, False

        next_id = await self._get_next_id()
        short_code = encode_base62(next_id)
        created_url = await self.repo.create(URL(id=next_id, original_url=original_url, short_code=short_code))
        await self._cache_mapping(short_code, original_url)
        logger.info("url_shortened", short_code=short_code, internal_id=next_id)
        return created_url, True

    async def resolve_url(self, short_code: str) -> str:
        """Resolve a short code: Redis first, then the database. Raises KeyError if unknown."""
        cached_url = await self._cache_get(f"code:{short_code}")
        if cached_url:
            return cached_url

        url_record = await self.repo.get_by_short_code(short_code)
        if not url_record:
            raise KeyError("Short code not found")

        await self._cache_mapping(short_code, url_record.original_url)
        return url_record.original_url

    async def record_click(self, short_code: str, referrer: Optional[str], user_agent: Optional[str]) -> None:
        """Record one click synchronously (the redirect path uses the batched ClickRecorder instead)."""
        url_record = await self.repo.get_by_short_code(short_code)
        if url_record:
            await self.repo.increment_clicks(url_record.id)
            await self.repo.create_click_analytic(url_record.id, referrer, user_agent)

    async def get_analytics(self, short_code: str) -> URL:
        url_record = await self.repo.get_by_short_code(short_code)
        if not url_record:
            raise KeyError("Short code not found")
        return url_record
