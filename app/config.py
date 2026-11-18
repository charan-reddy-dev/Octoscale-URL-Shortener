from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_ENV: str = "development"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    BASE_URL: str = "http://localhost:8000"
    LOG_LEVEL: str = "info"

    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/url_shortener"
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10

    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_MAX_CONNECTIONS: int = 200
    REDIS_SOCKET_TIMEOUT: float = 0.5
    # After a Redis error, skip Redis for this long instead of paying a timeout on every request.
    REDIS_RETRY_AFTER_SECONDS: float = 5.0
    CACHE_TTL_SECONDS: int = 86400

    # Sliding-window rate limits per client, per route group.
    RATE_LIMIT_PER_MINUTE: int = 100
    REDIRECT_RATE_LIMIT_PER_MINUTE: int = 600
    RATE_LIMIT_WINDOW_SECONDS: int = 60
    # Use the first X-Forwarded-For address as the client identity (only behind a trusted proxy).
    TRUST_PROXY_HEADERS: bool = False

    # Click analytics are buffered in memory and written in batches.
    CLICK_FLUSH_INTERVAL_SECONDS: float = 0.5
    CLICK_BATCH_SIZE: int = 1000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


settings = Settings()
