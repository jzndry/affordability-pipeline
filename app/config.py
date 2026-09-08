from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PLAID_CLIENT_ID: Optional[str] = None
    PLAID_SECRET: Optional[str] = None
    PLAID_ENV: str = "sandbox"
    REDIS_URL: str = "redis://localhost:6379/0"

    JOB_BROKER: Literal["celery", "inprocess"] = "celery"
    CORS_ALLOW_ORIGINS: str = "http://localhost:5173"
    MAX_CONCURRENT_ASSESSMENTS: int = 5
    MAX_WS_CONNECTIONS: int = 50
    MAX_REQUEST_BYTES: int = 262_144
    INGEST_PER_IP_LIMIT: str = "5/minute"
    INGEST_GLOBAL_LIMIT: str = "60/minute"
    EVENT_LOG_TTL_SECONDS: int = 900

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def cors_allow_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ALLOW_ORIGINS.split(",") if o.strip()]


# Create a single reusable instance
settings = Settings()
