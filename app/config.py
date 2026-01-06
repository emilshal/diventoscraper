from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    OPENAI_API_KEY: str | None = None
    OPENAI_TEMP_MODEL: str = "gpt-5-mini"
    RESULT_DIR: str = "./data"
    LOG_DIR: str = "./logs"
    LOG_LEVEL: str = "INFO"


settings = Settings()
