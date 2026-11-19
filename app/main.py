import asyncio
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.dependencies import get_url_service, limit_redirect
from app.api.v1.endpoints import router as api_v1_router
from app.clicks import click_recorder
from app.config import settings
from app.database import Base, engine
from app.logger import logger
from app.redis import redis_manager
from app.services.url_service import URLService


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("app_startup", env=settings.APP_ENV)
    await create_schema()
    redis_manager.initialize()
    click_recorder.start()
    yield
    await click_recorder.stop()
    await redis_manager.close()
    await engine.dispose()
    logger.info("app_shutdown")


async def create_schema(attempts: int = 5) -> None:
    """Create tables; with several workers starting at once one may lose the race, so retry."""
    for attempt in range(1, attempts + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            return
        except Exception as exc:
            if attempt == attempts:
                raise
            logger.warning("schema_create_retry", attempt=attempt, error=str(exc))
            await asyncio.sleep(0.2 * attempt)


app = FastAPI(
    title="OctoScale URL Shortener",
    description="URL shortener with Redis-cached redirects, atomic sliding-window rate limiting and batched click analytics.",
    version="1.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AccessLogMiddleware:
    """Pure ASGI access log; avoids the per-request overhead of BaseHTTPMiddleware."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        start = time.perf_counter()
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            logger.info(
                "request_completed",
                method=scope["method"],
                path=scope["path"],
                status_code=status_code,
                duration_ms=round((time.perf_counter() - start) * 1000, 2),
            )


app.add_middleware(AccessLogMiddleware)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
app.include_router(api_v1_router, prefix="/api/v1")


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/{short_code}", dependencies=[Depends(limit_redirect)])
async def redirect_to_original(short_code: str, request: Request, service: URLService = Depends(get_url_service)):
    """Resolve a short code (Redis, then database) and redirect. The click is queued, not written inline."""
    try:
        original_url = await service.resolve_url(short_code)
    except KeyError:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={"error": f"Short URL code '{short_code}' does not exist."},
            status_code=status.HTTP_404_NOT_FOUND,
        )

    click_recorder.record(short_code, request.headers.get("referer"), request.headers.get("user-agent"))
    response = RedirectResponse(url=original_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)
    response.headers.update(getattr(request.state, "rate_limit_headers", {}))
    return response
