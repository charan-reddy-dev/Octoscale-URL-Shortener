from typing import AsyncGenerator

import fakeredis
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Point the app at an in-memory database BEFORE importing it.
from app.config import settings

settings.DATABASE_URL = "sqlite+aiosqlite:///:memory:"

import app.database  # noqa: E402
from app.clicks import click_recorder  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.main import app as fastapi_app  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.redis import get_redis, redis_manager  # noqa: E402

test_engine = create_async_engine(settings.DATABASE_URL, connect_args={"check_same_thread": False})
TestAsyncSessionLocal = async_sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False, class_=AsyncSession)
# Background click writes open their own sessions through this module attribute.
app.database.AsyncSessionLocal = TestAsyncSessionLocal

# fakeredis with Lua support (lupa) runs the real rate-limit script, ZSETs and TTLs in-process.
fake_redis = fakeredis.FakeAsyncRedis(decode_responses=True)


async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
    async with TestAsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def override_get_redis():
    return fake_redis


fastapi_app.dependency_overrides[get_db] = override_get_db
fastapi_app.dependency_overrides[get_redis] = override_get_redis


@pytest.fixture(scope="function", autouse=True)
async def reset_state():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await fake_redis.flushall()
    redis_manager.redis = fake_redis
    redis_manager.mark_up()
    limiter.memory.clear()
    limiter._script = None
    while click_recorder.pending:
        await click_recorder.flush()
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    async with TestAsyncSessionLocal() as session:
        yield session


@pytest.fixture
def session_factory():
    return TestAsyncSessionLocal


@pytest.fixture
def mock_redis() -> fakeredis.FakeAsyncRedis:
    return fake_redis


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://test") as ac:
        yield ac
