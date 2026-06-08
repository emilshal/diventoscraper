"""Permanent venues scraper — AI-search-grounded.

Replaces the Google Places + gpt-4.1-mini pipeline that lived in
`scripts/scrape_destinations.py`. Produces the SAME 40-column Excel that
Filament expects (`save_excel()` shape), just collects the underlying data
with OpenAI Responses API + web_search instead of Places.

Flow per run (`run_permanent_scrape`):
    Phase 1  search       — multi-pass web_search call per city → list of venues
    Phase 2  enrich       — coord sanity check + photo HEAD-verify + backfill
    Phase 3  copy         — generate short_en / long_en / meta_en per venue
    Phase 4  translate    — fr / es / it / ru / zh bundle per venue
    Phase 5  write        — Excel writer (in app/ui.py, called by run)

Run state and per-venue checkpoints live in `app/core/run_store.py` so a
FastAPI restart resumes mid-run rather than redoing everything.

This module reuses several helpers from `temp_scraper`:
    - `_call_with_backoff` (OpenAI 429/transient retry)
    - `_extract_response_text`, `_clean_json_content`, `_extract_json_array`,
      `_extract_json_object` (JSON extraction from Responses API output)
    - `_get_openai_client` (single async client with usage tracking)
    - `_sanitize_excel_cell` (illegal-character stripping for Excel)
"""

from __future__ import annotations

import hashlib
import logging
import math
from typing import Any
from urllib.parse import urlparse

from app.config import settings
from app.core.run_store import RunStore
from app.core.temp_scraper import (
    _call_with_backoff,
    _clean_json_content,
    _extract_json_array,
    _extract_json_object,
    _extract_response_text,
    _get_openai_client,
)

logger = logging.getLogger(__name__)

# 5 language codes match the existing permanent Excel: fr, es, it, ru, zh
# (the temp scraper uses zh-CN; that's a separate concern — the permanent
# Excel's column header is "Long description zh".)
PERMANENT_LANGUAGES = ["fr", "es", "it", "ru", "zh"]

# Model selection — config-driven, falls back to OPENAI_PERM_MODEL.
PERM_MODEL = settings.OPENAI_PERM_MODEL
PERM_SEARCH_MODEL = (
    settings.OPENAI_PERM_SEARCH_MODEL or settings.OPENAI_PERM_MODEL
).strip()
PERM_COPY_MODEL = settings.OPENAI_PERM_COPY_MODEL
PERM_TRANSLATION_MODEL = settings.OPENAI_PERM_TRANSLATION_MODEL
PERM_TRANSLATION_FALLBACK_MODEL = settings.OPENAI_PERM_TRANSLATION_FALLBACK_MODEL


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _venue_id(name: str, address: str) -> str:
    """Stable ID for a venue inside a run. Used as the checkpoint primary key
    so re-running the search phase doesn't duplicate rows."""
    key = f"{(name or '').strip().lower()}|{(address or '').strip().lower()}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km — used by the coord sanity check."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [s.strip() for s in (str(x) for x in value) if s and s.strip()]
    if isinstance(value, str) and value.strip():
        return [s.strip() for s in value.split(",") if s.strip()]
    return []


def _is_likely_image_url(url: str) -> bool:
    """Cheap structural check before we spend a HEAD request."""
    if not url:
        return False
    if not url.startswith(("http://", "https://")):
        return False
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    if any(path.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif")):
        return True
    # WordPress / Wikimedia URLs without an extension are common — let HEAD decide.
    if "/wp-content/" in path or "/commons/" in path or "/thumb/" in path:
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Phase 1 — search
# ──────────────────────────────────────────────────────────────────────────────


_SEARCH_SYSTEM_PROMPT = """You are a careful travel-guide editor. You verify every venue with web_search before listing it. You NEVER invent venues. You NEVER invent ratings, review counts, addresses, or coordinates. When you don't know a field, you leave it empty or null."""


def _build_search_prompt(
    *,
    city: str,
    country: str,
    min_reviews: int,
    pass_max_items: int,
    already_covered: list[str],
) -> str:
    covered_block = (
        "(none yet)"
        if not already_covered
        else "\n".join(f"  - {name}" for name in already_covered[:60])
    )
    return f"""You are compiling a list of permanent visitor attractions in {city}, {country}
for a travel guide. Use web_search to verify each venue exists and is currently
open to the public. Return at most {pass_max_items} items as a JSON array.

Include ONLY permanent attractions: museums, galleries, historic monuments,
cathedrals/churches open to visitors, palaces, archaeological sites, gardens,
parks, viewpoints, observation decks, zoos/aquariums, themed visitor centers.

EXCLUDE: temporary exhibitions, restaurants, bars, hotels, shops, generic
neighborhoods/streets, transit stations, vague "districts", private buildings
not open to public, anything currently closed for restoration with no reopen
date.

Each venue MUST have at least {min_reviews} TripAdvisor or Google reviews,
OR be a major recognized cultural landmark (UNESCO site, national monument,
or listed in standard guidebooks like Lonely Planet, Michelin, Frommer's).

Do NOT include any of these venues (already covered in earlier passes):
{covered_block}

For each venue return EXACTLY these keys (use empty string for unknown text,
null for unknown numbers — never invent values):

{{
  "name":            string,    // English name
  "name_local":      string,    // local-language name (or "" if same as name)
  "address":         string,    // full street address with postal code
  "latitude":        number,
  "longitude":       number,
  "rating":          number,    // 0.0-5.0
  "reviews_count":   number,    // approximate, integer
  "official_url":    string,    // venue's own website if it has one
  "photo_url":       string,    // direct image URL (jpg/png/webp) — venue
                                // exterior or main hall. NOT a thumbnail,
                                // logo, or favicon. Prefer Wikipedia /
                                // Wikimedia Commons / the official site.
  "photo_credit":    string,    // attribution line for the photo
  "opening_hours":   string,    // e.g. "Tue-Sun 9:00-18:00, closed Mondays".
                                // Use "See venue website" if complex/seasonal.
  "duration_hours":  number,    // typical visit duration in hours (e.g. 1.5)
  "categories":      [string],  // 1-3 short tags, e.g. ["Museum","Art","Renaissance"]
  "source_url":      string     // primary citation used to verify
}}

Return ONLY the JSON array, no surrounding prose, no markdown fences."""


def _normalise_search_item(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Coerce a raw model item to a typed venue dict. Returns None if the
    item is unusable (no name or no address)."""
    name = _coerce_str(raw.get("name"))
    address = _coerce_str(raw.get("address"))
    if not name or not address:
        return None
    return {
        "venue_id": _venue_id(name, address),
        "name": name,
        "name_local": _coerce_str(raw.get("name_local")),
        "address": address,
        "latitude": _coerce_float(raw.get("latitude")),
        "longitude": _coerce_float(raw.get("longitude")),
        "rating": _coerce_float(raw.get("rating")),
        "reviews_count": _coerce_int(raw.get("reviews_count")),
        "official_url": _coerce_str(raw.get("official_url")),
        "photo_url": _coerce_str(raw.get("photo_url")),
        "photo_credit": _coerce_str(raw.get("photo_credit")),
        "opening_hours": _coerce_str(raw.get("opening_hours")),
        "duration_hours": _coerce_float(raw.get("duration_hours")),
        "categories": _coerce_str_list(raw.get("categories")),
        "source_url": _coerce_str(raw.get("source_url")),
    }


async def _run_one_search_pass(
    *,
    client,
    city: str,
    country: str,
    min_reviews: int,
    pass_max_items: int,
    already_covered: list[str],
    pass_index: int,
) -> list[dict[str, Any]]:
    """One web_search pass. Returns normalised venue dicts."""
    prompt = _build_search_prompt(
        city=city,
        country=country,
        min_reviews=min_reviews,
        pass_max_items=pass_max_items,
        already_covered=already_covered,
    )
    tools: list[dict[str, str]] | None = (
        [{"type": "web_search"}] if settings.PERM_ENABLE_WEB_SEARCH else None
    )
    logger.info(
        "perm.search.pass start city=%s pass=%d already_covered=%d",
        city,
        pass_index,
        len(already_covered),
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=PERM_SEARCH_MODEL,
                input=[
                    {"role": "system", "content": _SEARCH_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                tools=tools,
                max_output_tokens=8000,
            ),
            max_attempts=3,
        )
    except Exception as exc:
        logger.error("perm.search.pass error city=%s pass=%d err=%r", city, pass_index, exc)
        return []

    raw_text = resp.output_text or _extract_response_text(resp) or ""
    cleaned = _clean_json_content(raw_text)
    items = _extract_json_array(cleaned)
    if not isinstance(items, list):
        # Some models wrap in {"venues": [...]} despite the instruction.
        obj = _extract_json_object(cleaned)
        if isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, list):
                    items = v
                    break
    if not isinstance(items, list):
        logger.warning(
            "perm.search.pass parse-failed city=%s pass=%d head=%r",
            city,
            pass_index,
            raw_text[:200],
        )
        return []

    out: list[dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        item = _normalise_search_item(raw)
        if item is not None:
            out.append(item)
    logger.info(
        "perm.search.pass done city=%s pass=%d kept=%d (raw=%d)",
        city,
        pass_index,
        len(out),
        len(items),
    )
    return out


async def search_city(
    *,
    client,
    city: str,
    country: str,
    min_reviews: int,
    target_min: int,
    target_max: int,
    run_store: RunStore | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Multi-pass web_search until we hit target_max or exhaust PERM_SEARCH_PASSES.

    Dedups across passes by `venue_id`. If a `run_store` + `run_id` are
    supplied, every pass appends fresh items to the per-venue checkpoint
    table so a restart can resume without losing what was already found.
    """
    deduped: dict[str, dict[str, Any]] = {}
    seen_names: list[str] = []
    hard_max = max(target_max, settings.PERM_HARD_MAX_VENUES)
    pass_max_items = settings.PERM_SEARCH_PASS_MAX_ITEMS

    for pass_index in range(1, settings.PERM_SEARCH_PASSES + 1):
        if len(deduped) >= target_max:
            logger.info(
                "perm.search.city stop-target city=%s have=%d target_max=%d",
                city,
                len(deduped),
                target_max,
            )
            break
        items = await _run_one_search_pass(
            client=client,
            city=city,
            country=country,
            min_reviews=min_reviews,
            pass_max_items=pass_max_items,
            already_covered=seen_names,
            pass_index=pass_index,
        )

        fresh_this_pass: list[dict[str, Any]] = []
        for v in items:
            if v["venue_id"] in deduped:
                continue
            deduped[v["venue_id"]] = v
            seen_names.append(v["name"])
            fresh_this_pass.append(v)
            if len(deduped) >= hard_max:
                break

        if run_store is not None and run_id is not None and fresh_this_pass:
            await run_store.append_venues(run_id, city, fresh_this_pass)

        # Diminishing returns: if a pass after we have target_min produced
        # nothing new, the model has exhausted what it can find — stop.
        if not fresh_this_pass and len(deduped) >= target_min:
            logger.info(
                "perm.search.city stop-empty-pass city=%s pass=%d have=%d",
                city,
                pass_index,
                len(deduped),
            )
            break

    final = list(deduped.values())
    logger.info(
        "perm.search.city done city=%s passes_used=%d found=%d",
        city,
        pass_index,
        len(final),
    )
    return final


# ──────────────────────────────────────────────────────────────────────────────
# Phase 2 — enrich (stub — implemented in sub-checkpoint 2b)
# ──────────────────────────────────────────────────────────────────────────────


async def enrich_venues(
    *,
    client,
    city: str,
    country: str,
    city_lat: float | None,
    city_lon: float | None,
    venues: list[dict[str, Any]],
    run_store: RunStore | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Coord sanity check + photo HEAD-verify + hours/duration backfill.
    Implemented in sub-checkpoint 2b."""
    raise NotImplementedError("enrich_venues — implemented in sub-checkpoint 2b")


# ──────────────────────────────────────────────────────────────────────────────
# Phase 3-4 — copy + translate (stub — implemented in sub-checkpoint 2c)
# ──────────────────────────────────────────────────────────────────────────────


async def generate_copy(
    *,
    client,
    venue: dict[str, Any],
    city: str,
    country: str,
) -> dict[str, str]:
    """English short_en / long_en / meta_en. Implemented in 2c."""
    raise NotImplementedError("generate_copy — implemented in sub-checkpoint 2c")


async def translate_venue(
    *,
    client,
    english: dict[str, str],
    city: str,
) -> dict[str, dict[str, str]]:
    """fr/es/it/ru/zh bundle for short/long/meta + city name. Implemented in 2c."""
    raise NotImplementedError("translate_venue — implemented in sub-checkpoint 2c")


# ──────────────────────────────────────────────────────────────────────────────
# Top-level orchestrator (stub — implemented after 2b + 2c)
# ──────────────────────────────────────────────────────────────────────────────


async def run_permanent_scrape(
    *,
    run_id: str,
    cities: list[str],
    min_reviews: int,
    target_min: int,
    target_max: int,
    run_store: RunStore,
) -> dict[str, Any]:
    """Top-level entry point — search → enrich → copy → translate → write.

    Stub until 2b + 2c land. The search phase is callable directly via
    `search_city()` for prompt testing in the meantime.
    """
    raise NotImplementedError(
        "run_permanent_scrape — full orchestrator implemented after sub-checkpoints 2b + 2c"
    )
