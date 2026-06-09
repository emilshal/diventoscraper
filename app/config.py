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
    TEMP_DURATION_BACKFILL_ENABLED: int = 1
    TEMP_DURATION_BACKFILL_CONCURRENCY: int = 4
    # Venue image licensing (best-effort; not legal advice). When strict, only accept images
    # with explicit reuse-friendly licensing signals and otherwise fall back.
    TEMP_IMAGE_LICENSE_MODE: str = "soft"  # off|soft|strict
    TEMP_IMAGE_ALLOWED_LICENSE_KEYWORDS: str = "cc0,public domain,cc by,cc-by,cc by-sa,cc-by-sa"
    TEMP_IMAGE_ALLOWED_SOURCE_DOMAINS: str = (
        "commons.wikimedia.org,wikimedia.org,wikipedia.org,europeana.eu"
    )
    # Additional open-access museum/collection domains with explicit reuse terms.
    TEMP_IMAGE_OPEN_ACCESS_DOMAINS: str = ""
    # Allow a soft fallback to official venue page images when strict reuse fails.
    TEMP_IMAGE_SOFT_FALLBACK_ENABLED: int = 1
    # Rights enrichment for non-strict image modes.
    TEMP_IMAGE_RIGHTS_LEGAL_LINK_MAX: int = 3
    TEMP_IMAGE_RIGHTS_WEB_SEARCH_ENABLED: int = 1
    TEMP_IMAGE_FALLBACK_URL: str = "https://placehold.co/1200x800/png?text=Divento"
    TEMP_IMAGE_FAVICON_SIZE: int = 256
    # Aggressive duplicate policy: keep only one exhibition per venue + exact start/end dates.
    TEMP_DEDUPE_BY_VENUE_DATES_ONLY: int = 1
    TEMP_MAX_CITIES: int = 0
    # `0` means "no explicit cap" (subject to `TEMP_HARD_MAX_EXHIBITIONS`).
    # NOTE: the temporary scraper also enforces `TEMP_ABSOLUTE_MAX_EXHIBITIONS` as a hard per-city cap.
    TEMP_MAX_EXHIBITIONS: int = 0
    TEMP_ABSOLUTE_MAX_EXHIBITIONS: int = 0
    # Multi-city runs: cap the total number of exhibitions returned across all cities in a single run.
    # `0` means "no explicit cap", but we still clamp to `TEMP_ABSOLUTE_MAX_TOTAL_EXHIBITIONS`.
    TEMP_TOTAL_MAX_EXHIBITIONS: int = 0
    TEMP_ABSOLUTE_MAX_TOTAL_EXHIBITIONS: int = 0

    # ────────────────────────────────────────────────────────────────────────
    # Permanent venues scraper (web-search-grounded, no Google Places).
    # Parallel to the TEMP_* knobs above. Replaces the Google Places +
    # gpt-4.1-mini path that used to live in scrape_destinations.py.
    # ────────────────────────────────────────────────────────────────────────
    OPENAI_PERM_MODEL: str = "gpt-5.2"
    OPENAI_PERM_SEARCH_MODEL: str | None = None  # falls back to OPENAI_PERM_MODEL
    # Copy + translation models restored to the originals from the
    # Google-Places-era scrape_destinations.py (client asked for identical
    # copy/translation behaviour). Search stays on gpt-5.2 — that phase is
    # new functionality replacing Google Places, no original to match.
    OPENAI_PERM_COPY_MODEL: str = "gpt-5"
    OPENAI_PERM_TRANSLATION_MODEL: str = "gpt-4.1-mini"
    OPENAI_PERM_TRANSLATION_FALLBACK_MODEL: str = "gpt-4.1-mini"
    # Concurrency knobs.
    PERM_COPY_CONCURRENCY: int = 2
    PERM_TRANSLATION_CONCURRENCY: int = 4
    PERM_GEO_CONCURRENCY: int = 4
    PERM_VENUE_HOURS_BACKFILL_CONCURRENCY: int = 4
    PERM_PHOTO_VERIFY_CONCURRENCY: int = 2
    # Search strategy.
    PERM_SEARCH_PASSES: int = 5
    PERM_TARGET_MIN_VENUES: int = 15
    PERM_TARGET_MAX_VENUES: int = 50
    PERM_HARD_MAX_VENUES: int = 200       # safety cap to bound a runaway prompt
    PERM_SEARCH_PASS_MAX_ITEMS: int = 60  # cap items returned per search pass
    # Min reviews/popularity filter (caller sets, but we cap).
    PERM_DEFAULT_MIN_REVIEWS: int = 1000
    # Photo URL handling.
    PERM_PHOTO_VERIFY_ENABLED: int = 1     # HEAD-check each photo URL, drop dead ones
    PERM_PHOTO_VERIFY_TIMEOUT_S: float = 5.0
    PERM_PHOTO_FALLBACK_URL: str = "https://placehold.co/1200x800/png?text=Divento"
    # Coordinate sanity check — reject anything outside reasonable city bbox.
    PERM_COORD_SANITY_CHECK_ENABLED: int = 1
    PERM_COORD_MAX_DRIFT_KM: float = 50.0  # if model gives a venue >50km from city center, reject
    # Hours/duration backfill defaults.
    PERM_VENUE_HOURS_FALLBACK_VALUE: str = "See venue website"
    PERM_DURATION_FALLBACK_HOURS: float = 2.0
    # Web-search tool enablement.
    PERM_ENABLE_WEB_SEARCH: int = 1
    # Multi-city run caps.
    PERM_MAX_CITIES: int = 0
    PERM_TOTAL_MAX_VENUES: int = 0
    PERM_ABSOLUTE_MAX_TOTAL_VENUES: int = 0

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
