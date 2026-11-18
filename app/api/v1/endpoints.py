import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_url_service, limit_api
from app.clicks import click_recorder
from app.config import settings
from app.database import get_db
from app.redis import get_redis, redis_manager
from app.schemas.url import URLAnalyticsResponse, URLCreate, URLResponse
from app.services.url_service import ShortCodeUnavailable, URLService

router = APIRouter()


@router.post(
    "/shorten",
    response_model=URLResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(limit_api)],
)
async def shorten_url(payload: URLCreate, service: URLService = Depends(get_url_service)):
    """Create a shortened URL from a long URL."""
    try:
        url_record, _ = await service.shorten_url(payload.original_url, payload.custom_code)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ShortCodeUnavailable as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e), headers={"Retry-After": "5"})
    return URLResponse(
        original_url=url_record.original_url,
        short_code=url_record.short_code,
        short_url=f"{settings.BASE_URL.rstrip('/')}/{url_record.short_code}",
        clicks=url_record.clicks,
        created_at=url_record.created_at,
    )


@router.get(
    "/analytics/{short_code}",
    response_model=URLAnalyticsResponse,
    dependencies=[Depends(limit_api)],
)
async def get_url_analytics(short_code: str, service: URLService = Depends(get_url_service)):
    """Retrieve click analytics for a short code (most recent 100 clicks)."""
    try:
        url_record = await service.get_analytics(short_code)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Short URL not found")
    recent = sorted(url_record.analytics, key=lambda x: x.clicked_at, reverse=True)[:100]
    return URLAnalyticsResponse(
        short_code=url_record.short_code,
        original_url=url_record.original_url,
        clicks=url_record.clicks,
        created_at=url_record.created_at,
        recent_clicks=recent,
    )


@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check(db: AsyncSession = Depends(get_db), redis_client: aioredis.Redis = Depends(get_redis)):
    """Check database and Redis connectivity. Not rate limited, so load balancers can poll it."""
    health = {"status": "healthy", "components": {}, "clicks_pending": click_recorder.pending}

    try:
        await db.execute(text("SELECT 1"))
        health["components"]["database"] = "up"
    except Exception as e:
        health["status"] = "unhealthy"
        health["components"]["database"] = f"down: {e}"

    try:
        await redis_client.ping()
        redis_manager.mark_up()
        health["components"]["redis"] = "up"
    except Exception as e:
        health["status"] = "unhealthy"
        health["components"]["redis"] = f"down: {e}"

    if health["status"] == "unhealthy":
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=health)
    return health
