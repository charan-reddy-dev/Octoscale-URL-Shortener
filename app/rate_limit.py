"""Sliding-window rate limiting.

The Redis path runs one Lua script per request (loaded once, then called with EVALSHA), so trimming
the window, counting and recording the request happen atomically on the Redis server using the
Redis clock. Rejected requests are not recorded, so a client that keeps retrying is not locked out
for longer than the window. If Redis is unavailable the limiter falls back to an in-process sliding
window, which keeps enforcing limits per worker process instead of failing open.
"""

import math
import time
import uuid
from collections import deque
from dataclasses import dataclass

from fastapi import HTTPException, Request, Response, status
from redis.exceptions import RedisError

from app.config import settings
from app.logger import logger
from app.redis import redis_manager

SLIDING_WINDOW_LUA = """
if redis.replicate_commands then pcall(redis.replicate_commands) end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])
if count < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], window)
  return {1, limit - count - 1, 0}
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local retry = window
if oldest[2] then retry = tonumber(oldest[2]) + window - now end
return {0, 0, retry}
"""


@dataclass(frozen=True)
class Decision:
    allowed: bool
    remaining: int
    retry_after_ms: int
    backend: str


class MemorySlidingWindow:
    """Per-process fallback with the same semantics as the Lua script."""

    def __init__(self, max_keys: int = 50_000):
        self._hits: dict[str, deque[float]] = {}
        self._max_keys = max_keys

    def hit(self, key: str, limit: int, window_ms: int) -> Decision:
        now = time.monotonic() * 1000
        hits = self._hits.get(key)
        if hits is None:
            if len(self._hits) >= self._max_keys:
                self._evict(now, window_ms)
            hits = self._hits[key] = deque()
        while hits and hits[0] <= now - window_ms:
            hits.popleft()
        if len(hits) < limit:
            hits.append(now)
            return Decision(True, limit - len(hits), 0, "memory")
        retry = max(1, math.ceil(hits[0] + window_ms - now))
        return Decision(False, 0, retry, "memory")

    def _evict(self, now: float, window_ms: int) -> None:
        for key in [k for k, v in self._hits.items() if not v or v[-1] <= now - window_ms]:
            del self._hits[key]

    def clear(self) -> None:
        self._hits.clear()


class SlidingWindowRateLimiter:
    def __init__(self):
        self.memory = MemorySlidingWindow()
        self._script = None
        self._script_client = None

    async def hit(self, key: str, limit: int, window_ms: int) -> Decision:
        if redis_manager.available:
            try:
                client = await redis_manager.get_client()
                if self._script is None or self._script_client is not client:
                    # register_script calls EVALSHA and reloads the script on NOSCRIPT.
                    self._script = client.register_script(SLIDING_WINDOW_LUA)
                    self._script_client = client
                allowed, remaining, retry = await self._script(keys=[key], args=[limit, window_ms, uuid.uuid4().hex])
                redis_manager.mark_up()
                return Decision(bool(int(allowed)), int(remaining), int(retry), "redis")
            except (RedisError, OSError) as exc:
                redis_manager.mark_down(exc)
        return self.memory.hit(key, limit, window_ms)


limiter = SlidingWindowRateLimiter()


def client_identity(request: Request) -> str:
    if settings.TRUST_PROXY_HEADERS:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def _enforce(request: Request, response: Response, scope: str, limit: int) -> None:
    window_ms = settings.RATE_LIMIT_WINDOW_SECONDS * 1000
    client = client_identity(request)
    decision = await limiter.hit(f"rl:{scope}:{client}", limit, window_ms)
    headers = {
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": str(decision.remaining),
        "X-RateLimit-Backend": decision.backend,
    }
    if not decision.allowed:
        retry_s = max(1, math.ceil(decision.retry_after_ms / 1000))
        logger.warning("rate_limit_exceeded", scope=scope, client=client, path=request.url.path)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests. Please retry later.",
            headers={**headers, "Retry-After": str(retry_s)},
        )
    # Routes that return their own Response (the redirect) copy these from request.state.
    request.state.rate_limit_headers = headers
    response.headers.update(headers)


async def limit_api(request: Request, response: Response) -> None:
    """Applied to /api/v1 routes."""
    await _enforce(request, response, "api", settings.RATE_LIMIT_PER_MINUTE)


async def limit_redirect(request: Request, response: Response) -> None:
    """Applied to GET /{short_code}."""
    await _enforce(request, response, "redirect", settings.REDIRECT_RATE_LIMIT_PER_MINUTE)
