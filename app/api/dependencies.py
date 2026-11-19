import redis.asyncio as aioredis
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limit_api, limit_redirect
from app.redis import get_redis
from app.repositories.url_repository import URLRepository
from app.services.url_service import URLService

__all__ = ["get_url_service", "limit_api", "limit_redirect"]


async def get_url_service(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> URLService:
    return URLService(URLRepository(db), redis)
