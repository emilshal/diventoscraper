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
    # Search strategy controls (temporary exhibitions scraper).
    TEMP_SEARCH_PASSES: int = 5
    TEMP_TARGET_MIN_EXHIBITIONS: int = 15
    # When `TEMP_MAX_EXHIBITIONS=0`, we try to find "as many as possible" but still
    # enforce a hard safety cap to avoid runaway runs.
    TEMP_HARD_MAX_EXHIBITIONS: int = 200
    # Soft per-pass cap to keep prompts+responses bounded.
    TEMP_SEARCH_PASS_MAX_ITEMS: int = 60
    TEMP_TARGET_MAX_EXHIBITIONS: int = 40
    # Curated venue list (per city) to prioritize coverage.
    TEMP_CURATED_VENUES_ENABLED: int = 1
    TEMP_CURATED_VENUES_MAX_VENUES: int = 0  # 0 = all venues in the curated list
    TEMP_CURATED_VENUES_MAX_ITEMS_PER_VENUE: int = 8
    TEMP_VENUE_DISCOVERY_ENABLED: int = 1
    TEMP_VENUE_DISCOVERY_MAX: int = 50
    TEMP_VENUE_DEEPEN_PASSES: int = 1
    TEMP_VENUE_DEEPEN_MAX_VENUES: int = 12
    TEMP_VENUE_DEEPEN_MAX_PER_VENUE: int = 3
    TEMP_GEO_CONCURRENCY: int = 4
    TEMP_VENUE_HOURS_BACKFILL_ENABLED: int = 1
    TEMP_VENUE_HOURS_BACKFILL_CONCURRENCY: int = 4
    TEMP_VENUE_HOURS_FALLBACK_VALUE: str = "See venue website"
    TEMP_IMAGE_FALLBACK_URL: str = "https://placehold.co/1200x800/png?text=Divento"
    TEMP_IMAGE_FAVICON_SIZE: int = 256
    TEMP_MAX_CITIES: int = 0
    # `0` means "no explicit cap" (subject to `TEMP_HARD_MAX_EXHIBITIONS`).
    TEMP_MAX_EXHIBITIONS: int = 40
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
