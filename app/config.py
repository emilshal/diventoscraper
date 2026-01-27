from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import DotEnvSettingsSource, EnvSettingsSource, InitSettingsSource


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    OPENAI_API_KEY: str | None = None
    OPENAI_TEMP_MODEL: str = "gpt-5-mini"
    OPENAI_TEMP_SEARCH_MODEL: str | None = None
    OPENAI_TEMP_TRANSLATION_MODEL: str = "gpt-5-nano"
    OPENAI_TEMP_TRANSLATION_FALLBACK_MODEL: str = "gpt-5-mini"
    OPENAI_TEMP_COPY_MODEL: str = "gpt-5-mini"
    TEMP_COPY_CONCURRENCY: int = 2
    TEMP_TRANSLATION_CONCURRENCY: int = 4
    TEMP_SEARCH_PASSES: int = 2
    TEMP_MAX_CITIES: int = 0
    TEMP_MAX_EXHIBITIONS: int = 20
    RESULT_DIR: str = "./data"
    LOG_DIR: str = "./logs"
    LOG_LEVEL: str = "INFO"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings: InitSettingsSource,
        env_settings: EnvSettingsSource,
        dotenv_settings: DotEnvSettingsSource,
        file_secret_settings,
    ):
        # Prefer `.env` over the process environment so changing `.env` reliably updates config.
        # This is especially useful when a long-running server inherits an old `OPENAI_API_KEY`
        # from the shell/IDE environment.
        return (dotenv_settings, env_settings, init_settings, file_secret_settings)


settings = Settings()
