import asyncio

import pytest
from fastapi import status
from redis.exceptions import ConnectionError as RedisConnectionError

from app.clicks import click_recorder
from app.config import settings
from app.rate_limit import MemorySlidingWindow, limiter
from app.redis import redis_manager
from app.repositories.url_repository import URLRepository
from app.services.url_service import COUNTER_KEY, URLService


@pytest.fixture
def small_limits(monkeypatch):
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_MINUTE", 5)
    monkeypatch.setattr(settings, "REDIRECT_RATE_LIMIT_PER_MINUTE", 8)


async def test_api_limit_is_enforced_exactly(client, small_limits):
    codes = []
    for i in range(7):
        r = await client.post("/api/v1/shorten", json={"original_url": f"https://example.com/{i}"})
        codes.append(r.status_code)
    assert codes == [201] * 5 + [429] * 2
    assert r.headers["X-RateLimit-Limit"] == "5"
    assert r.headers["X-RateLimit-Remaining"] == "0"
    assert r.headers["X-RateLimit-Backend"] == "redis"
    assert 1 <= int(r.headers["Retry-After"]) <= 60


async def test_successful_response_reports_remaining(client, small_limits):
    r = await client.post("/api/v1/shorten", json={"original_url": "https://example.com/a"})
    assert r.headers["X-RateLimit-Remaining"] == "4"


async def test_rejected_requests_are_not_counted(mock_redis):
    for _ in range(12):
        await limiter.hit("rl:test:client", 5, 60_000)
    assert await mock_redis.zcard("rl:test:client") == 5
    assert 0 < await mock_redis.pttl("rl:test:client") <= 60_000


async def test_concurrent_burst_allows_exactly_the_limit():
    decisions = await asyncio.gather(*(limiter.hit("rl:burst:client", 50, 60_000) for _ in range(80)))
    assert sum(d.allowed for d in decisions) == 50
    assert all(d.backend == "redis" for d in decisions)


async def test_redirect_and_api_use_separate_buckets(client, small_limits):
    await client.post("/api/v1/shorten", json={"original_url": "https://example.com/r", "custom_code": "abc123"})
    statuses = [(await client.get("/abc123")).status_code for _ in range(10)]
    assert statuses == [307] * 8 + [429] * 2
    r = await client.post("/api/v1/shorten", json={"original_url": "https://example.com/s"})
    assert r.status_code == 201


async def test_redirect_carries_rate_limit_headers(client):
    await client.post("/api/v1/shorten", json={"original_url": "https://example.com/h", "custom_code": "hdr"})
    r = await client.get("/hdr")
    assert r.status_code == 307
    assert r.headers["X-RateLimit-Limit"] == str(settings.REDIRECT_RATE_LIMIT_PER_MINUTE)


async def test_forwarded_for_is_only_trusted_when_enabled(client, small_limits, monkeypatch):
    for i in range(5):
        await client.post("/api/v1/shorten", json={"original_url": f"https://example.com/x{i}"}, headers={"X-Forwarded-For": "1.1.1.1"})
    blocked = await client.post("/api/v1/shorten", json={"original_url": "https://example.com/y"}, headers={"X-Forwarded-For": "2.2.2.2"})
    assert blocked.status_code == 429  # header ignored: every request shares the socket address

    monkeypatch.setattr(settings, "TRUST_PROXY_HEADERS", True)
    allowed = await client.post("/api/v1/shorten", json={"original_url": "https://example.com/z"}, headers={"X-Forwarded-For": "3.3.3.3"})
    assert allowed.status_code == 201


async def test_falls_back_to_memory_when_redis_is_down(client, small_limits, mocker, mock_redis):
    mocker.patch.object(mock_redis, "evalsha", side_effect=RedisConnectionError("down"))
    mocker.patch.object(mock_redis, "eval", side_effect=RedisConnectionError("down"))
    statuses = []
    for i in range(6):
        r = await client.post("/api/v1/shorten", json={"original_url": f"https://example.com/m{i}"})
        statuses.append(r.status_code)
    assert statuses == [201] * 5 + [429]
    assert r.headers["X-RateLimit-Backend"] == "memory"
    assert not redis_manager.available


def test_memory_window_slides(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("app.rate_limit.time.monotonic", lambda: clock[0])
    window = MemorySlidingWindow()
    assert [window.hit("k", 2, 1000).allowed for _ in range(3)] == [True, True, False]
    clock[0] += 1.001
    assert window.hit("k", 2, 1000).allowed is True


async def test_counter_seeding_is_race_free(session_factory, mock_redis):
    await mock_redis.delete(COUNTER_KEY)

    async def allocate():
        async with session_factory() as session:
            return await URLService(URLRepository(session), mock_redis)._get_next_id()

    ids = await asyncio.gather(*(allocate() for _ in range(25)))
    assert len(set(ids)) == 25
    assert sorted(ids) == list(range(10000001, 10000026))


async def test_redirect_survives_redis_outage(client, mocker, mock_redis):
    await client.post("/api/v1/shorten", json={"original_url": "https://example.com/o", "custom_code": "outage"})
    for name in ("get", "evalsha", "eval", "pipeline"):
        mocker.patch.object(mock_redis, name, side_effect=RedisConnectionError("down"))
    r = await client.get("/outage")
    assert r.status_code == 307
    assert r.headers["location"] == "https://example.com/o"


async def test_shorten_returns_503_when_codes_cannot_be_allocated(client, mocker, mock_redis):
    mocker.patch.object(mock_redis, "exists", side_effect=RedisConnectionError("down"))
    r = await client.post("/api/v1/shorten", json={"original_url": "https://example.com/new"})
    assert r.status_code == 503
    assert r.headers["Retry-After"] == "5"


async def test_clicks_are_written_in_batches(client):
    await client.post("/api/v1/shorten", json={"original_url": "https://example.com/c", "custom_code": "clicks"})
    for _ in range(5):
        await client.get("/clicks", headers={"referer": "https://ref.example"})
    assert click_recorder.pending == 5
    assert await click_recorder.flush() == 5
    data = (await client.get("/api/v1/analytics/clicks")).json()
    assert data["clicks"] == 5
    assert len(data["recent_clicks"]) == 5
