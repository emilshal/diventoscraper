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

import asyncio
import hashlib
import logging
import math
import random
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

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
  "latitude":        number,    // IMPORTANT: search for coordinates when not
                                // immediately known. Only null if truly
                                // unfindable after a focused web_search.
  "longitude":       number,
  "rating":          number,    // 0.0-5.0
  "reviews_count":   number,    // approximate, integer
  "official_url":    string,    // venue's own website if it has one
  "photo_url":       string,    // direct image URL (jpg/png/webp) — venue
                                // exterior or main hall. NOT a thumbnail,
                                // logo, or favicon. Prefer Wikimedia Commons,
                                // then Wikipedia, then the official venue
                                // site. If none found after searching, leave
                                // empty — do NOT use Google image-search
                                // hotlinks, social media URLs, or stock-photo
                                // sites.
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
# Phase 2 — enrich
# ──────────────────────────────────────────────────────────────────────────────


# City-center coords are looked up once per (city, country) and reused for the
# sanity check on every venue in that city. Module-level cache is fine — the
# FastAPI process is single-tenant and the cache is bounded by city count.
_CITY_CENTER_CACHE: dict[str, tuple[float, float] | None] = {}


async def _lookup_city_center(
    *,
    client,
    city: str,
    country: str,
) -> tuple[float, float] | None:
    """Single web_search call per (city, country). Returns (lat, lon) or None."""
    key = f"{city.strip().lower()}|{country.strip().lower()}"
    if key in _CITY_CENTER_CACHE:
        return _CITY_CENTER_CACHE[key]

    tools = [{"type": "web_search"}] if settings.PERM_ENABLE_WEB_SEARCH else None
    prompt = (
        f"What are the geographic coordinates of the center of {city}, {country}?\n"
        'Return ONLY JSON: {"latitude": <number>, "longitude": <number>}.\n'
        "Use decimal degrees. If unsure, return null for both."
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=PERM_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=400,
            ),
            max_attempts=2,
        )
    except Exception as exc:
        logger.warning("perm.city_center.error city=%s err=%r", city, exc)
        _CITY_CENTER_CACHE[key] = None
        return None

    raw = resp.output_text or _extract_response_text(resp) or ""
    obj = _extract_json_object(_clean_json_content(raw))
    if not isinstance(obj, dict):
        _CITY_CENTER_CACHE[key] = None
        return None
    lat = _coerce_float(obj.get("latitude"))
    lon = _coerce_float(obj.get("longitude"))
    if lat is None or lon is None:
        _CITY_CENTER_CACHE[key] = None
        return None
    _CITY_CENTER_CACHE[key] = (lat, lon)
    logger.info("perm.city_center city=%s lat=%s lon=%s", city, lat, lon)
    return (lat, lon)


_HEAD_UA = "DiventoScraper/1.0 (https://divento.com; contact@divento.com)"


def _head_check_image(url: str, *, timeout: float) -> bool:
    """Run in a thread. True if HEAD returns 2xx and Content-Type starts with
    image/. Falls back to a small GET if HEAD is rejected (some CDNs do 405).
    Retries once on transient failures (network errors, 429 rate limits)
    because Wikimedia throttles bursts of concurrent HEADs."""
    if not url:
        return False
    headers = {"User-Agent": _HEAD_UA}

    def _attempt() -> bool | None:
        """Returns True/False on confirmed result, None for retry-worthy fail."""
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True, headers=headers)
            if r.status_code in (405, 403):
                # Retry with a tiny GET — some hosts block HEAD.
                r = requests.get(
                    url, timeout=timeout, allow_redirects=True, headers=headers, stream=True
                )
                r.close()
            if r.status_code == 429 or 400 <= r.status_code < 500 and r.status_code != 404:
                # Wikimedia returns 400 for genuine throttling, not just 429.
                # 404 is a real "doesn't exist". Other 4xx (400/403/429) are
                # retry-worthy under burst load.
                return None
            if 500 <= r.status_code < 600:
                return None
            if r.status_code >= 400:
                return False  # confirmed 404 / not-found
            ctype = (r.headers.get("Content-Type") or "").lower()
            if ctype and not ctype.startswith("image/"):
                return False
            return True
        except requests.exceptions.RequestException:
            return None  # network glitch, retry

    # Up to 3 attempts with exponential-ish backoff. Wikimedia is sensitive to
    # bursts even at concurrency=2, and we'd rather burn 2s on a retry than
    # ship a placeholder for a venue that has a real photo.
    for attempt_idx in range(3):
        result = _attempt()
        if result is not None:
            return result
        time.sleep(0.5 * (attempt_idx + 1) + random.random() * 0.5)
    return False


_PERM_PHOTO_CACHE: dict[str, dict[str, str] | None] = {}

_WIKIPEDIA_UA = "DiventoScraper/1.0 (https://divento.com; contact@divento.com)"

# Cache the chosen Wikipedia language code per country so we hit the AI
# lookup at most once per (country) per process lifetime.
_COUNTRY_TO_WIKI_LANG: dict[str, str] = {}


def _wikipedia_api_for(lang: str) -> str:
    """Build the Wikipedia REST API endpoint for a given language code."""
    return f"https://{lang}.wikipedia.org/w/api.php"


def _detect_script_language(text: str) -> str | None:
    """Return a Wikipedia language code based on the script of `text`, or
    None if the text is Latin script (ambiguous — caller falls back to the
    country-based AI lookup)."""
    if not text:
        return None
    # Sample the first 30 non-whitespace chars; covers the common case where
    # a venue name has a few Latin punctuation chars at the start/end.
    sample = "".join(c for c in text if not c.isspace())[:30]
    if not sample:
        return None
    # Per-script ranges. Each tuple is (lang_code, min_codepoint, max_codepoint).
    ranges = [
        ("ru", 0x0400, 0x04FF),   # Cyrillic
        ("zh", 0x4E00, 0x9FFF),   # CJK Unified Ideographs
        ("ja", 0x3040, 0x30FF),   # Hiragana + Katakana (Japanese)
        ("ko", 0xAC00, 0xD7AF),   # Hangul Syllables (Korean)
        ("ar", 0x0600, 0x06FF),   # Arabic
        ("he", 0x0590, 0x05FF),   # Hebrew
        ("el", 0x0370, 0x03FF),   # Greek
        ("th", 0x0E00, 0x0E7F),   # Thai
        ("hi", 0x0900, 0x097F),   # Devanagari (Hindi)
    ]
    counts: dict[str, int] = {}
    for ch in sample:
        cp = ord(ch)
        for lang_code, lo, hi in ranges:
            if lo <= cp <= hi:
                counts[lang_code] = counts.get(lang_code, 0) + 1
                break
    if not counts:
        return None
    # Return the script with the most characters in the sample.
    return max(counts, key=counts.get)


async def _lookup_country_wiki_lang(*, client, country: str) -> str:
    """Ask the model once per country which Wikipedia language code it
    should map to. Cached. Returns 'en' on any failure (safe default)."""
    if not country:
        return "en"
    key = country.strip().lower()
    if key in _COUNTRY_TO_WIKI_LANG:
        return _COUNTRY_TO_WIKI_LANG[key]
    if client is None:
        _COUNTRY_TO_WIKI_LANG[key] = "en"
        return "en"
    prompt = (
        f"What is the primary Wikipedia language code (ISO 639-1, two letters) "
        f"for {country}?\n"
        'Return ONLY JSON: {"lang": "xx"}.\n'
        "Examples: Italy -> it, France -> fr, Spain -> es, Germany -> de, "
        "Portugal -> pt, Brazil -> pt, Russia -> ru, Japan -> ja, China -> zh, "
        "United Kingdom -> en, United States -> en, Greece -> el, "
        "Netherlands -> nl, Poland -> pl, Turkey -> tr."
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=PERM_COPY_MODEL,
                input=prompt,
                max_output_tokens=200,
            ),
            max_attempts=2,
        )
    except Exception as exc:
        logger.warning("perm.wiki_lang.error country=%s err=%r", country, exc)
        _COUNTRY_TO_WIKI_LANG[key] = "en"
        return "en"
    raw = resp.output_text or _extract_response_text(resp) or ""
    obj = _extract_json_object(_clean_json_content(raw))
    lang = "en"
    if isinstance(obj, dict):
        candidate = str(obj.get("lang") or "").strip().lower()
        if len(candidate) == 2 and candidate.isalpha():
            lang = candidate
    _COUNTRY_TO_WIKI_LANG[key] = lang
    logger.info("perm.wiki_lang country=%s -> %s", country, lang)
    return lang


def _venue_name_match_tokens(venue_name: str, *, name_local: str = "") -> set[str]:
    """Lowercase content tokens for filename matching. We drop city-name
    tokens (too generic) but keep building-type tokens. Includes any
    name_local tokens too, since Wikimedia filenames often use the local
    language ('Palazzo_Pitti', 'Cattedrale_di_Santa_Maria_del_Fiore')."""
    parts = [venue_name, name_local]
    text = " ".join(p for p in parts if p)
    cleaned = re.sub(r"\([^)]*\)", " ", text)
    tokens = re.findall(r"[A-Za-z]+", cleaned.lower())
    stop = {
        "the", "of", "and", "national", "gallery", "museum", "garden", "gardens",
        # City-name stop tokens: too generic.
        "florence", "firenze", "florencia", "rome", "roma", "venice", "venezia",
        "milan", "milano", "naples", "napoli", "paris", "london", "madrid",
        "barcelona", "lisbon", "brussels", "berlin", "vienna", "wien",
        "city", "italy", "italia", "france", "spain",
        # Generic noun tokens that often match wrong files on their own.
        "saint",
    }
    return {t for t in tokens if len(t) >= 4 and t not in stop}


def _commons_file_url(
    filename: str,
    *,
    lang: str = "en",
    thumb_size: int = 1200,
    timeout: float = 8.0,
) -> str | None:
    """Resolve a 'File:...jpg' title to a working upload.wikimedia.org thumb URL
    by querying the imageinfo API. Pass `lang` to query a non-English Wikipedia
    (the file lookup works regardless of language because files live on Commons,
    but querying via the same-language API avoids redirects)."""
    if not filename.lower().startswith("file:"):
        filename = "File:" + filename
    try:
        r = requests.get(
            _wikipedia_api_for(lang),
            params={
                "action": "query",
                "titles": filename,
                "prop": "imageinfo",
                "iiprop": "url",
                "iiurlwidth": thumb_size,
                "format": "json",
            },
            headers={"User-Agent": _WIKIPEDIA_UA, "Accept": "application/json"},
            timeout=timeout,
        )
        pages = (r.json().get("query") or {}).get("pages") or {}
        for p in pages.values():
            for info in p.get("imageinfo") or []:
                return info.get("thumburl") or info.get("url")
    except Exception:
        return None
    return None


def _pick_best_wikipedia_hit(hits: list[dict[str, Any]], *, venue_name: str) -> str | None:
    """Of the top N search hits, pick the page whose title shares the most
    distinctive tokens with the venue name. Handles cases like searching for
    'Florence Baptistery' returning [Florence Cathedral, Florence, Florence
    Baptistery, ...]; the Baptistery hit at #3 is what we actually want."""
    if not hits:
        return None
    venue_tokens = _venue_name_match_tokens(venue_name)
    if not venue_tokens:
        return hits[0].get("title")
    best_title: str | None = None
    best_score = -1
    for h in hits:
        title = h.get("title") or ""
        if not title:
            continue
        title_tokens = _venue_name_match_tokens(title)
        score = len(venue_tokens & title_tokens)
        if score > best_score:
            best_score = score
            best_title = title
    # If no hit shares any distinctive tokens with the venue, fall back to #1.
    if best_score <= 0:
        return hits[0].get("title")
    return best_title


def _wikipedia_lookup_photo(
    *,
    venue_name: str,
    city: str,
    name_local: str = "",
    lang: str = "en",
    thumb_size: int = 1200,
    timeout: float = 8.0,
) -> dict[str, str] | None:
    """Three-step Wikipedia REST API lookup:
      1. action=query&list=search to find the page title.
      2. action=query&prop=pageimages — fast path; accept ONLY if filename
         contains a venue-name token (avoids generic city-panorama matches
         when Wikipedia's curated pageimage is bad).
      3. Fall back to action=query&prop=images and pick the first filename
         that matches a venue-name token; resolve to a thumb URL via
         imageinfo. Skips logos/SVGs/icons.
    Returns dict {image_url, page_url, credit, page_title} or None. Sync;
    callers should wrap in asyncio.to_thread.

    `lang` selects which Wikipedia to query (en/it/es/fr/etc.). Italian
    venues in Italian cities often have richer coverage on it.wikipedia.org
    than the English one. When searching a non-English Wikipedia we use the
    local-language venue name (name_local) as the primary query term if
    available, falling back to the English name."""
    api_url = _wikipedia_api_for(lang)
    headers = {"User-Agent": _WIKIPEDIA_UA, "Accept": "application/json"}
    # For non-English Wikipedias prefer the local-language name; it'll match
    # the Italian-titled article (e.g. "Castello Alfonsino") much better than
    # an English description ("Castello Alfonsino (Castello Aragonese)").
    primary_name = (name_local.strip() if lang != "en" and name_local else venue_name).strip()
    # Skip appending city when the venue name already contains it (e.g.
    # "Florence Baptistery Florence" dilutes the search; with just
    # "Florence Baptistery" the Baptistery page hits #1).
    if city.lower() in primary_name.lower():
        query = primary_name
    else:
        query = f"{primary_name} {city}".strip()
    try:
        r = requests.get(
            api_url,
            params={
                "action": "query",
                "list": "search",
                "srsearch": query,
                "format": "json",
                "srlimit": 3,
            },
            headers=headers,
            timeout=timeout,
        )
        hits = (r.json().get("query") or {}).get("search") or []
        if not hits:
            return None
        page_title = _pick_best_wikipedia_hit(hits, venue_name=primary_name) or hits[0]["title"]
    except Exception as exc:
        logger.debug("perm.photo.wikipedia_search_err venue=%s lang=%s err=%r", venue_name, lang, exc)
        return None

    page_url = f"https://{lang}.wikipedia.org/wiki/{page_title.replace(' ', '_')}"
    credit = f"Wikipedia / Wikimedia Commons — {page_title}"
    # Token-match against the page title too — handles the
    # English→Italian filename mismatch (e.g. venue "National Archaeological
    # Museum of Florence" → tokens {'archaeological'} doesn't match the Italian
    # filename Museo_archeologico_nazionale.jpg, but the Wikipedia page title
    # is "National Archaeological Museum, Florence" so tokenizing that gives
    # back English tokens that match the page's English description).
    name_tokens = _venue_name_match_tokens(venue_name, name_local=name_local)
    page_title_tokens = _venue_name_match_tokens(page_title)
    all_tokens = name_tokens | page_title_tokens

    # Step 2: pageimages fast path. Accept if the pageimage filename matches
    # a venue/page-title token, OR if the Wikipedia page title clearly
    # corresponds to the venue (covers English↔Italian filename mismatches
    # like "Archaeological" → "archeologico"). The page_title-corresponds
    # check requires a strong overlap between venue tokens and page-title
    # tokens — that prevents the Uffizi/Caravaggio-painting false positive
    # because the search page title for "Uffizi Gallery" is "Uffizi" and
    # both share the strong `uffizi` token, but for the rare false-positive
    # case the search would return a totally different page title.
    page_title_matches_venue = bool(
        name_tokens and page_title_tokens and (name_tokens & page_title_tokens)
    )
    try:
        r = requests.get(
            api_url,
            params={
                "action": "query",
                "titles": page_title,
                "prop": "pageimages",
                "pithumbsize": thumb_size,
                "format": "json",
            },
            headers=headers,
            timeout=timeout,
        )
        pages = (r.json().get("query") or {}).get("pages") or {}
        for p in pages.values():
            thumb = (p.get("thumbnail") or {}).get("source")
            page_filename = (p.get("pageimage") or "").lower()
            if not thumb:
                continue
            filename_matches_token = (
                not all_tokens or any(t in page_filename for t in all_tokens)
            )
            if filename_matches_token or page_title_matches_venue:
                return {
                    "image_url": thumb,
                    "page_url": page_url,
                    "credit": credit,
                    "page_title": page_title,
                }
    except Exception as exc:
        logger.debug("perm.photo.wikipedia_pageimage_err venue=%s err=%r", venue_name, exc)

    # Step 3: scan all images on the page for a name-match.
    try:
        r = requests.get(
            api_url,
            params={
                "action": "query",
                "titles": page_title,
                "prop": "images",
                "imlimit": 30,
                "format": "json",
            },
            headers=headers,
            timeout=timeout,
        )
        pages = (r.json().get("query") or {}).get("pages") or {}
        for p in pages.values():
            for im in p.get("images") or []:
                title = im.get("title") or ""
                lower = title.lower()
                # Skip non-photo assets.
                if lower.endswith((".svg", ".gif")):
                    continue
                if any(s in lower for s in ("logo", "commons-logo", "icon", "wikidata", "coa", "flag")):
                    continue
                if not all_tokens or not any(t in lower for t in all_tokens):
                    continue
                url = _commons_file_url(title, lang=lang, thumb_size=thumb_size, timeout=timeout)
                if url:
                    return {
                        "image_url": url,
                        "page_url": page_url,
                        "credit": credit,
                        "page_title": page_title,
                    }
    except Exception as exc:
        logger.debug("perm.photo.wikipedia_images_err venue=%s err=%r", venue_name, exc)
    return None


async def _try_wikipedia_lang(
    *,
    venue_name: str,
    name_local: str,
    city: str,
    lang: str,
) -> dict[str, str] | None:
    """Run the Wikipedia lookup + HEAD-check for one language. Returns the
    result dict on success, None if the lookup returned nothing or the URL
    was dead. Logs why on failure."""
    wiki = await asyncio.to_thread(
        _wikipedia_lookup_photo,
        venue_name=venue_name,
        city=city,
        name_local=name_local,
        lang=lang,
    )
    if not (wiki and wiki.get("image_url")):
        return None
    ok = await asyncio.to_thread(
        _head_check_image,
        wiki["image_url"],
        timeout=settings.PERM_PHOTO_VERIFY_TIMEOUT_S,
    )
    if not ok:
        logger.info(
            "perm.photo.wikipedia_dead venue=%s lang=%s url=%s",
            venue_name, lang, wiki["image_url"][:80],
        )
        return None
    return {
        "image_url": wiki["image_url"],
        "page_url": wiki["page_url"],
        "credit": wiki["credit"],
        "page_title": wiki["page_title"],
    }


async def _lookup_perm_photo(
    *,
    client,
    venue_name: str,
    name_local: str,
    city: str,
    country: str,
    official_url: str,
) -> dict[str, str] | None:
    """Find a working photo for the venue. Strategy:
      1. Try English Wikipedia (en.wikipedia.org).
      2. If no result, detect the local language and try that Wikipedia.
         Language is detected by character script first (Cyrillic → ru,
         CJK → zh, Greek → el, etc.) and falls back to a one-shot AI
         lookup of {country → wiki lang code}, cached per country.
      3. If both fail, return None — caller substitutes the placeholder.
    Cached per (venue, city, country)."""
    cache_key = f"{venue_name.strip().lower()}|{city.strip().lower()}|{country.strip().lower()}"
    if cache_key in _PERM_PHOTO_CACHE:
        return _PERM_PHOTO_CACHE[cache_key]

    # Pass 1: English Wikipedia.
    result = await _try_wikipedia_lang(
        venue_name=venue_name, name_local=name_local, city=city, lang="en"
    )
    if result is not None:
        logger.debug(
            "perm.photo.wikipedia venue=%s lang=en title=%s",
            venue_name, result["page_title"],
        )
        out = {k: result[k] for k in ("image_url", "page_url", "credit")}
        _PERM_PHOTO_CACHE[cache_key] = out
        return out

    # Pass 2: local-language Wikipedia. Use name_local for script detection
    # (the local-language name is more likely to be in the local script).
    local_lang = _detect_script_language(name_local) or _detect_script_language(venue_name)
    if local_lang is None:
        # Latin-script venue — derive from country.
        local_lang = await _lookup_country_wiki_lang(client=client, country=country)

    if local_lang and local_lang != "en":
        result = await _try_wikipedia_lang(
            venue_name=venue_name,
            name_local=name_local,
            city=city,
            lang=local_lang,
        )
        if result is not None:
            logger.info(
                "perm.photo.wikipedia venue=%s lang=%s title=%s (en miss)",
                venue_name, local_lang, result["page_title"],
            )
            out = {k: result[k] for k in ("image_url", "page_url", "credit")}
            _PERM_PHOTO_CACHE[cache_key] = out
            return out

    logger.info("perm.photo.no_wikipedia venue=%s tried=en,%s", venue_name, local_lang)
    _PERM_PHOTO_CACHE[cache_key] = None
    return None


async def _verify_or_replace_photo(
    venue: dict[str, Any],
    *,
    client,
    city: str,
    country: str,
) -> dict[str, Any]:
    """HEAD-verify the photo_url from search. If it's bad, do a targeted
    web_search-grounded lookup for a real Wikimedia/official-site image.
    Fall through to the placeholder only if both fail."""
    url = venue.get("photo_url") or ""

    # Step 1: verify search-phase URL if it looks like an image.
    if url and _is_likely_image_url(url) and settings.PERM_PHOTO_VERIFY_ENABLED:
        ok = await asyncio.to_thread(
            _head_check_image, url, timeout=settings.PERM_PHOTO_VERIFY_TIMEOUT_S
        )
        if ok:
            return venue
        logger.info("perm.photo.dead venue=%s url=%s", venue["name"], url)

    # Step 2: targeted lookup.
    found = await _lookup_perm_photo(
        client=client,
        venue_name=venue["name"],
        name_local=venue.get("name_local", "") or "",
        city=city,
        country=country,
        official_url=venue.get("official_url", ""),
    )
    if found is not None:
        venue["photo_url"] = found["image_url"]
        if found["credit"]:
            venue["photo_credit"] = found["credit"]
        logger.info(
            "perm.photo.replaced venue=%s -> %s", venue["name"], found["image_url"][:80]
        )
        return venue

    # Step 3: fallback to placeholder.
    venue["photo_url"] = settings.PERM_PHOTO_FALLBACK_URL
    venue["photo_credit"] = ""
    return venue


_PERM_COORD_CACHE: dict[str, tuple[float, float] | None] = {}


async def _lookup_perm_coords(
    *,
    client,
    venue_name: str,
    address: str,
    city: str,
    country: str,
) -> tuple[float, float] | None:
    """Permanent-attraction coord lookup. These are well-documented landmarks
    on Wikipedia, Google Maps, tourism sites — the model should find real
    coords for nearly every one. Cached per (name+city+country)."""
    cache_key = f"{venue_name.strip().lower()}|{city.strip().lower()}|{country.strip().lower()}"
    if cache_key in _PERM_COORD_CACHE:
        return _PERM_COORD_CACHE[cache_key]

    tools = [{"type": "web_search"}] if settings.PERM_ENABLE_WEB_SEARCH else None
    prompt = (
        f"What are the geographic coordinates of {venue_name} in {city}, {country}?\n"
        f"Address: {address}\n\n"
        "This is a famous permanent attraction. Its coordinates are documented "
        "on Wikipedia (look for the infobox), on Google Maps, and on official "
        "tourism sites. Search the web if needed.\n\n"
        'Return ONLY JSON: {"latitude": <decimal>, "longitude": <decimal>}.\n'
        "Use decimal degrees (e.g. 43.7678, 11.2553). Both fields must be numbers.\n"
        'If you genuinely cannot find them after a focused search, return {"latitude": null, "longitude": null}.\n'
        "Do NOT invent coordinates. Do NOT return city-center coordinates as a fallback."
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=PERM_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=1200,
            ),
            max_attempts=3,
        )
    except Exception as exc:
        logger.warning("perm.coord.lookup_error venue=%s err=%r", venue_name, exc)
        _PERM_COORD_CACHE[cache_key] = None
        return None

    raw = resp.output_text or _extract_response_text(resp) or ""
    obj = _extract_json_object(_clean_json_content(raw))
    if not isinstance(obj, dict):
        logger.warning("perm.coord.lookup_parse_failed venue=%s head=%r", venue_name, raw[:200])
        _PERM_COORD_CACHE[cache_key] = None
        return None
    lat = _coerce_float(obj.get("latitude"))
    lon = _coerce_float(obj.get("longitude"))
    if lat is None or lon is None:
        _PERM_COORD_CACHE[cache_key] = None
        return None
    _PERM_COORD_CACHE[cache_key] = (lat, lon)
    return (lat, lon)


async def _backfill_coords(
    *,
    client,
    venue: dict[str, Any],
    city: str,
    country: str,
) -> dict[str, Any]:
    """If lat/lon is missing, call _lookup_perm_coords. Mutates and returns
    the venue dict."""
    if venue.get("latitude") is not None and venue.get("longitude") is not None:
        return venue
    coords = await _lookup_perm_coords(
        client=client,
        venue_name=venue["name"],
        address=venue.get("address", ""),
        city=city,
        country=country,
    )
    if coords is None:
        logger.info("perm.coord.unfilled venue=%s", venue["name"])
        return venue
    venue["latitude"], venue["longitude"] = coords
    logger.debug(
        "perm.coord.backfill venue=%s lat=%s lon=%s",
        venue["name"], coords[0], coords[1],
    )
    return venue


def _passes_coord_sanity(
    *,
    venue: dict[str, Any],
    city_lat: float | None,
    city_lon: float | None,
) -> bool:
    """Drop venues whose coords are clearly wrong (model hallucination or
    wrong-city). Returns True if the venue should be kept."""
    if not settings.PERM_COORD_SANITY_CHECK_ENABLED:
        return True
    if city_lat is None or city_lon is None:
        return True  # Can't check; don't reject.
    lat = venue.get("latitude")
    lon = venue.get("longitude")
    if lat is None or lon is None:
        # Missing coords are NOT a sanity-check failure — they'll get
        # an empty cell in Excel. Caller decides whether to drop missing-coord
        # venues separately.
        return True
    dist = _haversine_km(city_lat, city_lon, lat, lon)
    if dist > settings.PERM_COORD_MAX_DRIFT_KM:
        logger.warning(
            "perm.coord.sanity_fail venue=%s lat=%s lon=%s dist_km=%.1f",
            venue["name"],
            lat,
            lon,
            dist,
        )
        return False
    return True


def _backfill_hours_duration(venue: dict[str, Any]) -> dict[str, Any]:
    """Trivial fallbacks for empty hours / duration."""
    if not venue.get("opening_hours"):
        venue["opening_hours"] = settings.PERM_VENUE_HOURS_FALLBACK_VALUE
    if venue.get("duration_hours") is None:
        venue["duration_hours"] = settings.PERM_DURATION_FALLBACK_HOURS
    return venue


async def _enrich_one_venue(
    *,
    client,
    venue: dict[str, Any],
    city: str,
    country: str,
    city_lat: float | None,
    city_lon: float | None,
    coord_sem: asyncio.Semaphore,
    photo_sem: asyncio.Semaphore,
) -> dict[str, Any] | None:
    """Run all enrichment steps for one venue. Returns the venue dict (mutated)
    or None if the sanity check drops it."""
    async with coord_sem:
        venue = await _backfill_coords(client=client, venue=venue, city=city, country=country)

    if not _passes_coord_sanity(venue=venue, city_lat=city_lat, city_lon=city_lon):
        return None

    async with photo_sem:
        venue = await _verify_or_replace_photo(
            venue, client=client, city=city, country=country
        )

    venue = _backfill_hours_duration(venue)
    return venue


async def enrich_venues(
    *,
    client,
    city: str,
    country: str,
    venues: list[dict[str, Any]],
    run_store: RunStore | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Coord backfill + sanity check + photo HEAD-verify + hours/duration fallback.

    Concurrency: coord backfill calls hit OpenAI (bound by PERM_GEO_CONCURRENCY);
    photo HEAD-checks are local network calls (bound by PERM_PHOTO_VERIFY_CONCURRENCY).
    Each venue is checkpointed to run_store on success so a restart resumes
    where we left off.
    """
    if not venues:
        return []

    center = await _lookup_city_center(client=client, city=city, country=country)
    city_lat, city_lon = (center if center is not None else (None, None))

    coord_sem = asyncio.Semaphore(max(1, settings.PERM_GEO_CONCURRENCY))
    photo_sem = asyncio.Semaphore(max(1, settings.PERM_PHOTO_VERIFY_CONCURRENCY))

    async def _wrap(v: dict[str, Any]) -> dict[str, Any] | None:
        try:
            out = await _enrich_one_venue(
                client=client,
                venue=v,
                city=city,
                country=country,
                city_lat=city_lat,
                city_lon=city_lon,
                coord_sem=coord_sem,
                photo_sem=photo_sem,
            )
        except Exception as exc:
            logger.exception(
                "perm.enrich.venue_error venue=%s err=%r", v.get("name"), exc
            )
            return None
        if out is not None and run_store is not None and run_id is not None:
            await run_store.mark_venue_enriched(run_id, out["venue_id"], out)
        return out

    results = await asyncio.gather(*[_wrap(v) for v in venues])
    kept = [r for r in results if r is not None]
    logger.info(
        "perm.enrich.city done city=%s in=%d kept=%d dropped=%d",
        city,
        len(venues),
        len(kept),
        len(venues) - len(kept),
    )
    return kept


# ──────────────────────────────────────────────────────────────────────────────
# Phase 3 — English copy generation
# ──────────────────────────────────────────────────────────────────────────────


# Same category set the old Google-based scraper used; Filament UI / DB may
# depend on these exact strings.
DIVENTO_CATEGORIES = [
    "Whats_hot",
    "Performing_Arts",
    "Famous_Places",
    "Eating_and_Drinking",
    "Arts_and_Culture",
    "Hidden_Gems",
    "Family",
    "Parks_and_Gardens",
    "Historic_Houses_and_Sites",
]


# System message restored VERBATIM from the original scrape_destinations.py
# (the Google-Places-era script). Fiona asked for identical copy behaviour.
_PERM_COPY_SYSTEM = (
    "You are an expert travel writer and historian specialising in cultural attractions. "
    "You have extensive firsthand knowledge of historical sites, architecture, and art. "
    "You write factual, historically accurate descriptions based on thorough research. "
    "Follow the provided examples and guidelines exactly. Respond only with JSON."
)

# DESC_PROMPT restored VERBATIM from the original scrape_destinations.py
# lines 142-231. Do not edit without checking with the client — she asked
# for the exact pre-rebuild prompt.
_OLD_DESC_PROMPT = (
    "Write copy for a Divento permanent attraction or museum listing.\n\n"
    "GLOBAL PRIORITY\n"
    "- Base all content on verified factual sources, prioritising the official website of the venue where possible.\n"
    "- Extract concrete information (history, architecture, collections, artworks, people) from official sources.\n"
    "- Rewrite all information in Divento style; do not copy text verbatim.\n"
    "- Do not invent dates, artworks, artists, or historical context.\n\n"
    "WRITING STYLE GUIDELINES\n"
    "- Use an informal tone and address the reader directly as 'you'.\n"
    "- Write as though the author has visited the attraction.\n"
    "- Base descriptions heavily on historical fact, including dates, architectural styles, origins and development, physical and spatial detail.\n"
    "- Include people associated with the site (architects, artists, patrons, historical figures), with dates where relevant.\n"
    "- When describing museums, include specific artworks or objects, not general summaries.\n"
    "- Integrate naturally: one highlight, two don't-miss elements, and one lesser-known detail.\n"
    "- Be specific and concrete; use precise nouns and strong verbs.\n"
    "- Cut filler and favour active voice.\n"
    "- Always use British English spelling.\n\n"
    "DATE RULES (STRICT)\n"
    "- Include dates where they add clarity: construction and modification phases, historical events linked to the site, creation dates of artworks where relevant.\n"
    "- For people: format as Name (birth-death) or Name (born YEAR), and include only on first mention.\n"
    "- Do not overload with unnecessary dates.\n"
    "- Never guess or approximate.\n\n"
    "STRICTLY FORBIDDEN WORDS\n"
    "Never use: visitor(s), located, feature(d), showcase, blend, period, accessible, house(d), home(d), step into\n"
    "Avoid all brochure-style language.\n\n"
    "FORMAT REQUIREMENTS\n"
    "- Spell out numbers from one to ten; use numerals from 11 upward.\n"
    "- Ensure consistent spacing.\n"
    "- Do not begin descriptions with the attraction name.\n"
    "- Do not start with: include, explore, step into.\n"
    "- Avoid wrap-up sentences and dashes.\n"
    "- Use active voice.\n\n"
    "HTML\n"
    "- Wrap each paragraph in <p></p> tags.\n"
    "- Keep formatting clean and minimal.\n\n"
    "LONG DESCRIPTION\n"
    "- Target 300-320 words.\n"
    "- Multiple paragraphs.\n"
    "- Must read as a continuous narrative, not a checklist.\n"
    "- Avoid formulaic openings.\n\n"
    "CONTENT REQUIREMENTS\n"
    "- Include architectural and structural detail, materials and construction techniques, and changes over time.\n"
    "- Clearly explain original function vs current use.\n"
    "- Include political, cultural, or historical context and notable events tied to the site.\n"
    "- Mention specific artworks, artefacts, or architectural elements.\n"
    "- Integrate highlight, don't-miss, and lesser-known details naturally.\n\n"
    "SOURCE DISCIPLINE (CRITICAL)\n"
    "- Anchor the text in verifiable facts drawn from the venue's official description.\n"
    "- Prefer named works, artists, materials, dates, and features explicitly mentioned by the venue.\n"
    "- If detailed information is limited, remain factual and restrained rather than expanding into generic filler.\n"
    "- Do not add speculative interpretation.\n\n"
    "SHORT DESCRIPTION\n"
    "- Maximum 164 characters.\n"
    "- One sentence only.\n"
    "- Aim for 20-25 words where possible.\n\n"
    "Must:\n"
    "- Include a clear subject (building, site, collection, or theme).\n"
    "- Include a specific reason to visit.\n"
    "- Include at least one concrete detail (date, feature, artwork, or historical fact).\n"
    "- Contain a verb.\n"
    "- Read naturally.\n\n"
    "Must NOT:\n"
    "- Repeat the attraction name.\n"
    "- Be vague or promotional.\n"
    "- Focus on layout, rooms, or visitor flow.\n\n"
    "SHORT DESCRIPTION CONTENT RULES\n"
    "- Historic building/site -> include construction date or era.\n"
    "- Museum -> include type of collection and key strength.\n"
    "- Person-focused site -> include name and dates.\n"
    "- Cultural/historical theme -> include timeframe.\n"
    "- If multiple angles exist, prioritise the most distinctive one.\n\n"
    "KEY HISTORICAL ELEMENTS TO INCLUDE\n"
    "- Construction dates and phases.\n"
    "- Architectural styles and materials.\n"
    "- Dimensions and technical details where relevant.\n"
    "- Architects, artists, patrons (with dates where relevant).\n"
    "- Political and social context.\n"
    "- Specific artworks, sculptures, decorative elements.\n"
    "- Original vs current function.\n"
    "- Construction and engineering techniques.\n"
    "- Events that took place there.\n"
    "- Archaeological discoveries and findings.\n\n"
    "OUTPUT\n"
    "Return ONLY a JSON object:\n"
    "{\n"
    '  "short": "...",\n'
    '  "long": "..."\n'
    "}\n\n"
    "Both fields must be non-empty strings."
)


def _build_copy_prompt(
    *,
    venue: dict[str, Any],
    city: str,
    country: str,
) -> str:
    """User prompt restored to the original enrich_place_sync() structure.
    The old `context` was Google Places' editorial_summary (or first review)
    — that source is gone, so context is empty, a state the original code
    also produced when Google had no summary."""
    name = venue.get("name", "")
    context = ""
    return (
        f"Write historically accurate copy for: {name} in {city}.\n"
        f"Context: {context}\n\n"
        f"Available categories: {', '.join(DIVENTO_CATEGORIES)}\n"
        "If multiple categories apply, include all relevant categories as a single comma-separated string under the 'categories' key.\n\n"
        f"{_OLD_DESC_PROMPT}\n\n"
        "Write descriptions ONLY IN ENGLISH following the source discipline and historical accuracy requirements above. "
        "Provide a meta description (max 150 characters) in English. "
        "Estimate the recommended visit duration in hours (use decimals: 0.30 for 30 minutes, 1 for 1 hour, etc.) based on the site's complexity and historical significance.\n\n"
        "For this worker, return JSON keys: categories, short_en, long_en, meta_en, duration. "
        "short_en must contain the prompt's short description. long_en must contain the prompt's long description."
    )


_PERM_COPY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "categories": {"type": "string"},
        "short_en": {"type": "string"},
        "long_en": {"type": "string"},
        "meta_en": {"type": "string"},
        "duration": {"type": "number"},
    },
    "required": ["categories", "short_en", "long_en", "meta_en", "duration"],
}


def _normalise_categories(value: str) -> str:
    """Filter the model's categories to the allowed list, comma-joined."""
    if not value:
        return ""
    raw = [c.strip() for c in value.split(",") if c.strip()]
    allowed_lower = {c.lower(): c for c in DIVENTO_CATEGORIES}
    out: list[str] = []
    for c in raw:
        match = allowed_lower.get(c.lower())
        if match and match not in out:
            out.append(match)
    return ", ".join(out)


async def generate_copy(
    *,
    client,
    venue: dict[str, Any],
    city: str,
    country: str,
) -> dict[str, str]:
    """English short_en / long_en / meta_en + filtered Divento categories.
    Returns {} on terminal failure — caller decides whether to drop the venue."""
    prompt = _build_copy_prompt(venue=venue, city=city, country=country)
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=PERM_COPY_MODEL,
                input=[
                    {"role": "system", "content": _PERM_COPY_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                text={
                    "verbosity": "low",
                    "format": {
                        "type": "json_schema",
                        "name": "perm_venue_copy",
                        "strict": True,
                        "schema": _PERM_COPY_SCHEMA,
                    },
                },
                max_output_tokens=16000,  # generous: gpt-5 reasoning shares this budget; old code had no cap
            ),
            max_attempts=3,
        )
    except Exception as exc:
        # Fall back to json_object on schema rejection by the model.
        logger.warning("perm.copy.schema_failed venue=%s err=%r", venue.get("name"), exc)
        try:
            resp = await _call_with_backoff(
                lambda: client.responses.create(
                    model=PERM_COPY_MODEL,
                    input=[
                        {"role": "system", "content": _PERM_COPY_SYSTEM},
                        {"role": "user", "content": prompt},
                    ],
                    text={"verbosity": "low", "format": {"type": "json_object"}},
                    max_output_tokens=16000,  # generous: gpt-5 reasoning shares this budget; old code had no cap
                ),
                max_attempts=2,
            )
        except Exception as exc2:
            logger.error("perm.copy.error venue=%s err=%r", venue.get("name"), exc2)
            return {}

    raw = resp.output_text or _extract_response_text(resp) or ""
    obj = _extract_json_object(_clean_json_content(raw))
    if not isinstance(obj, dict):
        logger.warning(
            "perm.copy.parse_failed venue=%s head=%r", venue.get("name"), raw[:200]
        )
        return {}

    # Duration parsing restored from the original enrich_place_sync():
    # missing/zero/invalid -> 1.0 hour default.
    try:
        duration = obj.get("duration", None)
        if duration is None or duration == 0 or (
            isinstance(duration, (int, float, str)) and float(duration) <= 0
        ):
            duration = 1.0
        else:
            duration = float(duration)
    except Exception:
        duration = 1.0

    return {
        "short_en": _coerce_str(obj.get("short_en")) or _coerce_str(obj.get("short")),
        "long_en": _coerce_str(obj.get("long_en")) or _coerce_str(obj.get("long")),
        "meta_en": _coerce_str(obj.get("meta_en")),
        "categories": _normalise_categories(_coerce_str(obj.get("categories"))),
        "duration": duration,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Phase 4 — translate
# ──────────────────────────────────────────────────────────────────────────────


# System message restored VERBATIM from the original scrape_destinations.py
# translate_descriptions() call.
_PERM_TRANSLATE_SYSTEM = (
    "You are a professional translator specializing in travel content. "
    "Maintain accuracy, tone, and formatting."
)


def _build_translate_prompt(
    *,
    name: str,
    city: str,
    short_en: str,
    long_en: str,
    meta_en: str,
) -> str:
    """Prompt restored VERBATIM from the original translate_descriptions()."""
    return f"""Translate these travel descriptions for "{name}" in {city} from English to French, Spanish, Italian, Russian and Chinese.

SHORT DESCRIPTION: {short_en}

LONG DESCRIPTION: {long_en}

META DESCRIPTION: {meta_en}

Requirements:
- Maintain the same HTML formatting in long descriptions
- Keep the same tone and style
- Preserve historical accuracy and factual information
- Maintain British English spelling conventions in the original meaning
- Return JSON with this exact structure:
{{
  "fr": {{"short": "...", "long": "...", "meta": "..."}},
  "es": {{"short": "...", "long": "...", "meta": "..."}},
  "it": {{"short": "...", "long": "...", "meta": "..."}},
  "ru": {{"short": "...", "long": "...", "meta": "..."}},
  "zh": {{"short": "...", "long": "...", "meta": "..."}}
}}"""


def _build_translate_schema(languages: list[str]) -> dict[str, Any]:
    lang_obj = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "short": {"type": "string"},
            "long": {"type": "string"},
            "meta": {"type": "string"},
        },
        "required": ["short", "long", "meta"],
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {lang: lang_obj for lang in languages},
        "required": languages,
    }


def _empty_translation_bundle() -> dict[str, dict[str, str]]:
    return {
        lang: {"short": "", "long": "", "meta": ""}
        for lang in PERMANENT_LANGUAGES
    }


# City-name translation: separate once-per-city call, restored from the
# original translate_city() (which the old Excel's "Name of site city xx"
# columns were built from). Cached per city like the original.
_CITY_TRANSLATIONS_CACHE: dict[str, dict[str, str]] = {}


async def translate_city_async(*, client, city: str) -> dict[str, str]:
    """Prompt restored VERBATIM from the original translate_city().
    Returns {fr, es, it, ru, zh: translated-city}, defaulting each missing
    language to the untranslated city name (original behaviour)."""
    if city in _CITY_TRANSLATIONS_CACHE:
        return _CITY_TRANSLATIONS_CACHE[city]

    prompt = (
        f"Translate the city name '{city}' into French, Spanish, Italian, "
        "Russian and Chinese. Respond in JSON with keys fr, es, it, ru and zh."
    )
    js: dict[str, Any] = {}
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=PERM_TRANSLATION_MODEL,
                input=[
                    {"role": "system", "content": "You translate city names."},
                    {"role": "user", "content": prompt},
                ],
                max_output_tokens=400,
            ),
            max_attempts=2,
        )
        raw = resp.output_text or _extract_response_text(resp) or ""
        obj = _extract_json_object(_clean_json_content(raw))
        if isinstance(obj, dict):
            js = obj
    except Exception as exc:
        logger.warning("perm.city_translate.error city=%s err=%r", city, exc)

    out = {lang: _coerce_str(js.get(lang)) or city for lang in PERMANENT_LANGUAGES}
    _CITY_TRANSLATIONS_CACHE[city] = out
    logger.info("perm.city_translate city=%s -> %s", city, out)
    return out


async def translate_venue(
    *,
    client,
    name: str,
    city: str,
    english: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Translate short/long/meta into PERMANENT_LANGUAGES (original prompt).
    Returns {lang: {short, long, meta}}; empties on failure. City names are
    translated separately via translate_city_async (original architecture)."""
    short_en = english.get("short_en", "")
    long_en = english.get("long_en", "")
    meta_en = english.get("meta_en", "")
    if not any([short_en, long_en, meta_en]):
        return _empty_translation_bundle()

    prompt = _build_translate_prompt(
        name=name,
        city=city,
        short_en=short_en,
        long_en=long_en,
        meta_en=meta_en,
    )
    schema = _build_translate_schema(PERMANENT_LANGUAGES)

    async def _attempt(model: str) -> dict[str, Any] | None:
        try:
            resp = await _call_with_backoff(
                lambda: client.responses.create(
                    model=model,
                    input=[
                        {"role": "system", "content": _PERM_TRANSLATE_SYSTEM},
                        {"role": "user", "content": prompt},
                    ],
                    # NOTE: no text.verbosity here — gpt-4.1-mini (the
                    # restored original translation model) rejects that
                    # gpt-5-family parameter.
                    text={
                        "format": {
                            "type": "json_schema",
                            "name": "perm_translation_bundle",
                            "strict": True,
                            "schema": schema,
                        },
                    },
                    max_output_tokens=9000,
                ),
                max_attempts=3,
            )
        except Exception as exc:
            logger.warning("perm.translate.schema_failed model=%s err=%r", model, exc)
            return None
        raw = resp.output_text or _extract_response_text(resp) or ""
        obj = _extract_json_object(_clean_json_content(raw))
        return obj if isinstance(obj, dict) else None

    data = await _attempt(PERM_TRANSLATION_MODEL)
    if data is None and PERM_TRANSLATION_FALLBACK_MODEL != PERM_TRANSLATION_MODEL:
        logger.info("perm.translate.fallback name=%s", name)
        data = await _attempt(PERM_TRANSLATION_FALLBACK_MODEL)

    if data is None:
        logger.error("perm.translate.error name=%s — both models failed", name)
        return _empty_translation_bundle()

    out: dict[str, dict[str, str]] = {}
    for lang in PERMANENT_LANGUAGES:
        obj = data.get(lang) if isinstance(data.get(lang), dict) else {}
        out[lang] = {
            "short": _coerce_str((obj or {}).get("short")),
            "long": _coerce_str((obj or {}).get("long")),
            "meta": _coerce_str((obj or {}).get("meta")),
        }
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Top-level orchestrator
# ──────────────────────────────────────────────────────────────────────────────


async def _copy_and_translate_one(
    *,
    client,
    venue: dict[str, Any],
    city: str,
    country: str,
    copy_sem: asyncio.Semaphore,
    translate_sem: asyncio.Semaphore,
    run_store: RunStore | None,
    run_id: str | None,
) -> dict[str, Any] | None:
    try:
        async with copy_sem:
            english = await generate_copy(
                client=client, venue=venue, city=city, country=country
            )
        if not english:
            logger.warning("perm.copy.empty venue=%s — dropping", venue.get("name"))
            return None
        if run_store is not None and run_id is not None:
            await run_store.mark_venue_copy(run_id, venue["venue_id"], english)

        async with translate_sem:
            translations = await translate_venue(
                client=client, name=venue["name"], city=city, english=english
            )
        if run_store is not None and run_id is not None:
            await run_store.mark_venue_translated(run_id, venue["venue_id"], translations)

        # Attach copy + translations to the venue dict for the Excel writer.
        venue["copy"] = english
        venue["translations"] = translations
        # Duration comes from the copy call now (original behaviour: the
        # gpt-5 enrichment estimated visit duration; default 1.0).
        if english.get("duration") is not None:
            venue["duration_hours"] = english["duration"]
        return venue
    except Exception as exc:
        logger.exception(
            "perm.copy_translate.error venue=%s err=%r", venue.get("name"), exc
        )
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Editorial ratings — model assigns evidence tiers per venue, deterministic
# post-processing distributes 1-5 stars with batch-size-aware targets so a
# city of 16 famous landmarks doesn't end up with 16 fives.
# ──────────────────────────────────────────────────────────────────────────────


def _editorial_rating_targets(batch_size: int) -> tuple[int, int, int, int]:
    """Returns (target_fives, target_fours, max_twos, max_ones). Tuned for
    permanent batches (typically 10-25 famous sites) — a touch more generous
    than the temp scraper's helper so a curated city list gets meaningful
    differentiation rather than a flat sea of 3s. Most rows still default to 3."""
    if batch_size <= 0:
        return 0, 0, 0, 0
    # 5s: 1 starting at 10 venues, scales up at 30+
    target_fives = 0 if batch_size < 10 else (1 if batch_size < 30 else max(1, (batch_size + 19) // 20))
    # 4s: ~1 per 5 venues, capped relative to batch
    target_fours = (
        0
        if batch_size < 4
        else (1 if batch_size < 8 else max(2, (batch_size + 4) // 5))
    )
    max_twos = 0 if batch_size < 8 else (1 if batch_size < 20 else max(1, (batch_size + 14) // 15))
    max_ones = 0 if batch_size < 30 else 1
    target_fours = min(target_fours, max(0, batch_size - target_fives))
    max_twos = min(max_twos, max(0, batch_size - target_fives - target_fours))
    max_ones = min(max_ones, max(0, batch_size - target_fives - target_fours - max_twos))
    return target_fives, target_fours, max_twos, max_ones


def _strip_html_for_evidence(value: str) -> str:
    """Cheap HTML strip + whitespace collapse for the rating prompt evidence."""
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", text).strip()


async def _assign_city_editorial_ratings_perm_async(
    venues: list[dict[str, Any]],
    *,
    city: str,
) -> list[str]:
    """Model assigns each venue an evidence tier; we then distribute 1-5 star
    ratings according to PERM-batch-size targets. Returns a list of stringy
    digits, same length and order as `venues`. Defaults to "3" on any failure
    so a flaky run never leaves rating blank.

    Evidence: venue name + short_en + meta_en + first ~240 chars of long_en
    + city + Divento categories. We do NOT include reviews/ratings from
    search (they're always null anyway) — judgment is entirely on the
    written evidence."""
    if not venues:
        return []

    import json as _json

    client = _get_openai_client()
    target_fives, target_fours, max_twos, max_ones = _editorial_rating_targets(len(venues))

    evidence_rows: list[dict[str, Any]] = []
    for idx, v in enumerate(venues):
        copy = v.get("copy") or {}
        long_excerpt = _strip_html_for_evidence(copy.get("long_en", ""))
        if len(long_excerpt) > 240:
            long_excerpt = long_excerpt[:237].rsplit(" ", 1)[0].strip() + "..."
        evidence_rows.append(
            {
                "id": idx,
                "name": v.get("name", ""),
                "categories": copy.get("categories", ""),
                "short": copy.get("short_en", ""),
                "meta": copy.get("meta_en", ""),
                "long_excerpt": long_excerpt,
            }
        )

    prompt = (
        f"Rank a batch of permanent visitor attractions in {city} from strongest to weakest "
        "for Divento editorial rating.\n\n"
        "Do not assign final star ratings. Instead classify each attraction into one evidence tier:\n"
        "- exceptional = world-class landmark, the kind of place a traveller plans an entire trip around (UNESCO sites, top-3-in-country museums, iconic monuments)\n"
        "- above_average = clearly above average and worth planning for; major museum, important monument, or distinctive cultural site\n"
        "- average = competent attraction worth a visit; default when evidence is thin\n"
        "- weak = minor or specialist site of limited general interest\n"
        "- poor = clearly weak or skippable for most travellers\n\n"
        "Judge from the written evidence (name, categories, descriptions). Consider historical significance, "
        "scale of collection or site, architectural importance, and uniqueness.\n"
        "Keep most attractions in average unless there is clear reason to move them up or down.\n"
        "Famous landmarks (cathedrals, world-class galleries, UNESCO sites) typically warrant exceptional or above_average.\n"
        "Smaller specialist museums or minor sites typically warrant average or weak.\n"
        "Return a JSON array sorted from strongest attraction to weakest attraction.\n"
        "Each object must have exactly these keys: 'id' and 'tier'.\n"
        "Include every id exactly once.\n\n"
        "ATTRACTIONS JSON\n"
        f"{_json.dumps(evidence_rows, ensure_ascii=False)}"
    )

    # Responses API requires the root schema to be an object — wrap the
    # ranked array in a `rankings` field. Parser unwraps it below.
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "rankings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "id": {"type": "integer"},
                        "tier": {
                            "type": "string",
                            "enum": ["exceptional", "above_average", "average", "weak", "poor"],
                        },
                    },
                    "required": ["id", "tier"],
                },
            },
        },
        "required": ["rankings"],
    }
    # gpt-5.2 with reasoning=medium reserves a large chunk of the output
    # budget for reasoning tokens, so the ranking JSON itself only gets
    # what's left. Each ranking row is ~25 output tokens (id + tier +
    # JSON punctuation) but we need to leave ~1500 tokens for reasoning
    # plus headroom. The previous formula bottomed out at 900 which
    # truncated the output for batches >12 venues and left every venue
    # rated 3 (perm.rating.invalid_json).
    max_tokens = max(2500, min(8000, 1500 + len(venues) * 40))

    content = ""
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=PERM_COPY_MODEL,
                input=prompt,
                reasoning={"effort": "medium"},
                text={
                    "verbosity": "low",
                    "format": {
                        "type": "json_schema",
                        "name": "perm_city_editorial_ratings",
                        "strict": True,
                        "schema": schema,
                    },
                },
                max_output_tokens=max_tokens,
            )
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
    except Exception as exc:
        logger.warning("perm.rating.schema_failed city=%s err=%r", city, exc)
        try:
            resp = await _call_with_backoff(
                lambda: client.responses.create(
                    model=PERM_COPY_MODEL,
                    input=prompt,
                    reasoning={"effort": "medium"},
                    text={"verbosity": "low", "format": {"type": "json_object"}},
                    max_output_tokens=max_tokens,
                )
            )
            content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        except Exception as exc2:
            logger.error("perm.rating.error city=%s err=%r", city, exc2)
            content = ""

    data = None
    if content:
        obj = _extract_json_object(content)
        if isinstance(obj, dict):
            inner = obj.get("rankings")
            if isinstance(inner, list):
                data = inner
        if data is None:
            # Legacy / fallback: maybe the model returned a bare array anyway.
            arr = _extract_json_array(content)
            if isinstance(arr, list):
                data = arr
    if not isinstance(data, list):
        # Snip to keep the log readable — first 300 chars usually shows
        # whether we got a truncated json or some other parse failure.
        head = (content or "")[:300].replace("\n", "\\n")
        logger.warning(
            "perm.rating.invalid_json city=%s n=%d content_len=%d head=%r — defaulting all to 3",
            city, len(venues), len(content or ""), head,
        )
        return ["3"] * len(venues)

    tier_by_id: dict[int, str] = {}
    ordered_ids: list[int] = []
    valid_tiers = {"exceptional", "above_average", "average", "weak", "poor"}
    for item in data:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("id"))
            tier = str(item.get("tier") or "").strip().lower()
        except Exception:
            continue
        if idx < 0 or idx >= len(venues) or tier not in valid_tiers or idx in tier_by_id:
            continue
        tier_by_id[idx] = tier
        ordered_ids.append(idx)

    # Fill any missing rows with average, preserving original order at the tail.
    for idx in range(len(venues)):
        if idx not in tier_by_id:
            tier_by_id[idx] = "average"
            ordered_ids.append(idx)

    final_ratings = ["3"] * len(venues)
    used_ones = 0
    used_twos = 0
    used_fives = 0
    used_fours = 0

    # Bottom: distribute 1s then 2s starting from the weakest end.
    for idx in reversed(ordered_ids):
        tier = tier_by_id.get(idx, "average")
        if tier == "poor" and used_ones < max_ones:
            final_ratings[idx] = "1"
            used_ones += 1
    for idx in reversed(ordered_ids):
        tier = tier_by_id.get(idx, "average")
        if final_ratings[idx] != "3":
            continue
        if tier in {"weak", "poor"} and used_twos < max_twos:
            final_ratings[idx] = "2"
            used_twos += 1

    # Top: 5s first to exceptional, then 4s to remaining exceptional + above_average.
    for idx in ordered_ids:
        tier = tier_by_id.get(idx, "average")
        if tier == "exceptional":
            if used_fives < target_fives:
                final_ratings[idx] = "5"
                used_fives += 1
            elif used_fours < target_fours:
                final_ratings[idx] = "4"
                used_fours += 1
        elif tier == "above_average" and final_ratings[idx] == "3" and used_fours < target_fours:
            final_ratings[idx] = "4"
            used_fours += 1

    # If we haven't hit target_fours yet, promote the strongest remaining 3s.
    if used_fours < target_fours:
        for idx in ordered_ids:
            if used_fours >= target_fours:
                break
            if final_ratings[idx] != "3":
                continue
            final_ratings[idx] = "4"
            used_fours += 1

    logger.info(
        "perm.rating.city city=%s n=%d dist=1×%d 2×%d 3×%d 4×%d 5×%d",
        city,
        len(final_ratings),
        final_ratings.count("1"),
        final_ratings.count("2"),
        final_ratings.count("3"),
        final_ratings.count("4"),
        final_ratings.count("5"),
    )
    return final_ratings


async def run_permanent_scrape(
    *,
    run_id: str,
    cities: list[str],
    country: str,
    min_reviews: int,
    target_min: int,
    target_max: int,
    run_store: RunStore,
) -> dict[str, Any]:
    """Top-level entry point — search → enrich → copy → translate.

    The Excel writer is owned by the API layer (`app/ui.py`) because it
    needs to choose the output path; this function returns the fully
    enriched venue dicts and a per-city summary.
    """
    client = _get_openai_client()
    if client is None:
        await run_store.update_run(
            run_id, status="error", error_message="OPENAI_API_KEY not configured"
        )
        raise RuntimeError("OPENAI_API_KEY not configured")

    summary: dict[str, int] = {}
    all_venues: list[dict[str, Any]] = []

    copy_sem = asyncio.Semaphore(max(1, settings.PERM_COPY_CONCURRENCY))
    translate_sem = asyncio.Semaphore(max(1, settings.PERM_TRANSLATION_CONCURRENCY))

    for i, city in enumerate(cities):
        await run_store.update_run(
            run_id,
            status="searching",
            current_phase="search",
            current_city=city,
            progress_pct=round(i / max(1, len(cities)) * 100, 1),
        )
        searched = await search_city(
            client=client,
            city=city,
            country=country,
            min_reviews=min_reviews,
            target_min=target_min,
            target_max=target_max,
            run_store=run_store,
            run_id=run_id,
        )

        await run_store.update_run(run_id, status="enriching", current_phase="enrich")
        enriched = await enrich_venues(
            client=client,
            city=city,
            country=country,
            venues=searched,
            run_store=run_store,
            run_id=run_id,
        )

        await run_store.update_run(
            run_id, status="translating", current_phase="copy+translate"
        )
        # City name translated once per city (original translate_city()
        # behaviour, cached) — feeds the "Name of site city xx" columns.
        city_translations = await translate_city_async(client=client, city=city)

        tasks = [
            _copy_and_translate_one(
                client=client,
                venue=v,
                city=city,
                country=country,
                copy_sem=copy_sem,
                translate_sem=translate_sem,
                run_store=run_store,
                run_id=run_id,
            )
            for v in enriched
        ]
        results = await asyncio.gather(*tasks)
        kept = [r for r in results if r is not None]
        for v in kept:
            v["city_translations"] = city_translations

        if kept:
            await run_store.update_run(
                run_id, status="translating", current_phase="rating"
            )
            ratings = await _assign_city_editorial_ratings_perm_async(kept, city=city)
            for v, r in zip(kept, ratings):
                v["rating"] = r

        summary[city] = len(kept)
        all_venues.extend(kept)
        logger.info(
            "perm.city_done city=%s searched=%d enriched=%d final=%d",
            city,
            len(searched),
            len(enriched),
            len(kept),
        )

    await run_store.update_run(
        run_id,
        status="writing",
        current_phase="excel",
        current_city=None,
        progress_pct=95.0,
        result_summary=summary,
    )
    return {"venues": all_venues, "summary": summary}


# ──────────────────────────────────────────────────────────────────────────────
# Phase 5 — Excel writer (40-column shape matching the old save_excel())
# ──────────────────────────────────────────────────────────────────────────────

_PERM_EXCEL_COLUMNS = [
    "Name of site, City",
    "City",
    "Country",
    "Full address",
    "Type(s) of activity",
    "Divento Categories",
    "Free activity?",
    "Short description",
    "Long description",
    "Long description fr",
    "Long description es",
    "Long description it",
    "Long description ru",
    "Long description zh",
    "URL of images",
    "Legends of images",
    "Duration of visit",
    "Opening and closing time",
    "Short description fr",
    "Short description es",
    "Short description it",
    "Short description ru",
    "Short description zh",
    "Meta description",
    "Meta description fr",
    "Meta description es",
    "Meta description it",
    "Meta description ru",
    "Meta description zh",
    "Latitude",
    "Information",
    "Longitude",
    "Activity type",
    "Rating",
    "Name of site city",
    "Name of site city fr",
    "Name of site city it",
    "Name of site city ru",
    "Name of site city zh",
    "Name of site city es",
    "Real city",
]

_ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")


def _excel_sanitize(value: Any) -> Any:
    """Strip illegal control characters that openpyxl refuses to write."""
    if isinstance(value, str):
        return _ILLEGAL_EXCEL_CHARS_RE.sub("", value)
    return value


def _name_city_for_lang(name: str, city_translated: str, fallback_city: str) -> str:
    return f"{name}, {city_translated or fallback_city}"


def _venue_to_excel_row(venue: dict[str, Any]) -> list[Any]:
    """Build the 41-cell row (matches _PERM_EXCEL_COLUMNS order). Sanitizes
    each cell. Any missing field falls back to empty string."""
    name = venue.get("name", "")
    city = venue.get("city") or venue.get("real_city") or ""
    country = venue.get("country", "")
    address = venue.get("address", "")
    rating = venue.get("rating")
    rating_s = "" if rating is None else f"{rating}"
    latitude = venue.get("latitude")
    longitude = venue.get("longitude")
    lat_s = "" if latitude is None else f"{latitude}"
    lon_s = "" if longitude is None else f"{longitude}"
    information = venue.get("official_url", "")
    duration = venue.get("duration_hours")
    duration_s = "" if duration is None else f"{duration}"
    photo_url = venue.get("photo_url", "")
    photo_credit = venue.get("photo_credit", "")

    copy = venue.get("copy") or {}
    short_en = copy.get("short_en", "")
    long_en = copy.get("long_en", "")
    meta_en = copy.get("meta_en", "")
    categories = copy.get("categories", "")

    translations = venue.get("translations") or {}
    # City names come from the once-per-city translate_city call (original
    # architecture), stamped on the venue by the orchestrator.
    city_trans = venue.get("city_translations") or {}

    def t(lang: str, field: str) -> str:
        return (translations.get(lang) or {}).get(field, "") or ""

    def ct(lang: str) -> str:
        return city_trans.get(lang, "") or ""

    name_city = f"{name}, {city}" if city else name
    row = [
        name_city,                                # 0  Name of site, City
        city,                                     # 1  City
        country,                                  # 2  Country
        address,                                  # 3  Full address
        "",                                       # 4  Type(s) of activity (unused, matches old code)
        categories,                               # 5  Divento Categories
        "0",                                      # 6  Free activity? (matches old default)
        short_en,                                 # 7  Short description
        long_en,                                  # 8  Long description (English)
        t("fr", "long"),                          # 9  Long description fr
        t("es", "long"),                          # 10 Long description es
        t("it", "long"),                          # 11 Long description it
        t("ru", "long"),                          # 12 Long description ru
        t("zh", "long"),                          # 13 Long description zh
        photo_url,                                # 14 URL of images
        photo_credit,                             # 15 Legends of images
        duration_s,                               # 16 Duration of visit
        "",                                       # 17 Opening and closing time (always empty in the original Excel)
        t("fr", "short"),                         # 18 Short description fr
        t("es", "short"),                         # 19 Short description es
        t("it", "short"),                         # 20 Short description it
        t("ru", "short"),                         # 21 Short description ru
        t("zh", "short"),                         # 22 Short description zh
        meta_en,                                  # 23 Meta description
        t("fr", "meta"),                          # 24 Meta description fr
        t("es", "meta"),                          # 25 Meta description es
        t("it", "meta"),                          # 26 Meta description it
        t("ru", "meta"),                          # 27 Meta description ru
        t("zh", "meta"),                          # 28 Meta description zh
        lat_s,                                    # 29 Latitude
        information,                              # 30 Information (official URL)
        lon_s,                                    # 31 Longitude
        "",                                       # 32 Activity type (unused, matches old code)
        rating_s,                                 # 33 Rating
        name_city,                                # 34 Name of site city (English, dup of col 0)
        _name_city_for_lang(name, ct("fr"), city),   # 35 Name of site city fr
        _name_city_for_lang(name, ct("it"), city),   # 36 Name of site city it
        _name_city_for_lang(name, ct("ru"), city),   # 37 Name of site city ru
        _name_city_for_lang(name, ct("zh"), city),   # 38 Name of site city zh
        _name_city_for_lang(name, ct("es"), city),   # 39 Name of site city es
        city,                                     # 40 Real city
    ]
    return [_excel_sanitize(c) for c in row]


def write_permanent_excel(
    *,
    venues: list[dict[str, Any]],
    output_path: str | Path,
    city: str | None = None,
    country: str | None = None,
) -> str:
    """Write the 40-column permanent-attractions Excel file. `venues` must
    already have `copy` and `translations` attached (run_permanent_scrape
    does this). `city` and `country` are stamped onto venues that don't
    already carry them (the search-phase venues don't, but the orchestrator
    knows the city). Returns the path written."""
    # openpyxl is heavy; import lazily so the module stays cheap to import.
    from openpyxl import Workbook
    from openpyxl.utils.exceptions import IllegalCharacterError

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "Permanent venues"
    ws.append(_PERM_EXCEL_COLUMNS)

    rows_written = 0
    for v in venues:
        if not v.get("address"):
            # Skip rows without an address (matches old save_excel() behavior).
            continue
        # Stamp city/country if not on the venue dict.
        if city and not v.get("city"):
            v["city"] = city
        if city and not v.get("real_city"):
            v["real_city"] = city
        if country and not v.get("country"):
            v["country"] = country
        try:
            ws.append(_venue_to_excel_row(v))
            rows_written += 1
        except IllegalCharacterError:
            # Belt-and-braces: should already be handled by _excel_sanitize.
            logger.warning("perm.excel.illegal_char venue=%s — retrying", v.get("name"))
            ws.append([_excel_sanitize(str(c)) for c in _venue_to_excel_row(v)])
            rows_written += 1

    wb.save(out)
    logger.info("perm.excel.wrote path=%s rows=%d", out, rows_written)
    return str(out)
