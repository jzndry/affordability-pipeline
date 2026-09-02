from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PLAID_CLIENT_ID: Optional[str] = None  # Typing added to remove error suggestion in IDE
    PLAID_SECRET: Optional[str] = None
    PLAID_ENV: str = "sandbox"
    REDIS_URL: str = "redis://localhost:6379/0"

    # Automatically read from a local .env file
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Create a single reusable instance
settings = Settings()
