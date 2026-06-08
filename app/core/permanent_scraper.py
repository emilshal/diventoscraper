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
    _lookup_venue_coords_async,
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


def _head_check_image(url: str, *, timeout: float) -> bool:
    """Run in a thread. True if HEAD returns 2xx and Content-Type starts with
    image/. Falls back to a small GET if HEAD is rejected (some CDNs do 405)."""
    if not url:
        return False
    headers = {"User-Agent": "Mozilla/5.0 (DiventoScraper/permanent)"}
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True, headers=headers)
        if r.status_code == 405 or r.status_code == 403:
            # Retry with a tiny GET — some hosts block HEAD.
            r = requests.get(
                url, timeout=timeout, allow_redirects=True, headers=headers, stream=True
            )
            r.close()
        if r.status_code >= 400:
            return False
        ctype = (r.headers.get("Content-Type") or "").lower()
        if ctype and not ctype.startswith("image/"):
            return False
        return True
    except Exception:
        return False


async def _verify_or_replace_photo(venue: dict[str, Any]) -> dict[str, Any]:
    """HEAD-verify the photo_url; replace with fallback on failure."""
    url = venue.get("photo_url") or ""
    if not _is_likely_image_url(url):
        if url:
            logger.debug("perm.photo.reject_shape venue=%s url=%s", venue["name"], url)
        venue["photo_url"] = settings.PERM_PHOTO_FALLBACK_URL
        venue["photo_credit"] = ""
        return venue

    if not settings.PERM_PHOTO_VERIFY_ENABLED:
        return venue

    ok = await asyncio.to_thread(
        _head_check_image, url, timeout=settings.PERM_PHOTO_VERIFY_TIMEOUT_S
    )
    if not ok:
        logger.info("perm.photo.dead venue=%s url=%s", venue["name"], url)
        venue["photo_url"] = settings.PERM_PHOTO_FALLBACK_URL
        venue["photo_credit"] = ""
    return venue


async def _backfill_coords(
    *,
    client,
    venue: dict[str, Any],
    city: str,
    country: str,
) -> dict[str, Any]:
    """If lat/lon is missing, call _lookup_venue_coords_async (reuses temp's
    cache). Mutates and returns the venue dict."""
    if venue.get("latitude") is not None and venue.get("longitude") is not None:
        return venue
    use_web = bool(settings.PERM_ENABLE_WEB_SEARCH)
    coords = await _lookup_venue_coords_async(
        client=client,
        venue=venue["name"],
        address=venue.get("address", ""),
        city=city,
        country=country,
        use_web_search_tool=use_web,
    )
    if coords is None:
        return venue
    lat_s, lon_s, _src = coords
    lat = _coerce_float(lat_s)
    lon = _coerce_float(lon_s)
    if lat is not None and lon is not None:
        venue["latitude"] = lat
        venue["longitude"] = lon
        logger.debug(
            "perm.coord.backfill venue=%s lat=%s lon=%s", venue["name"], lat, lon
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
        venue = await _verify_or_replace_photo(venue)

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


_PERM_COPY_SYSTEM = (
    "You are an expert travel writer and historian specialising in cultural "
    "attractions. You write factual, historically accurate descriptions based "
    "on thorough research. Respond only with JSON matching the provided schema."
)


def _build_copy_prompt(
    *,
    venue: dict[str, Any],
    city: str,
    country: str,
) -> str:
    name = venue.get("name", "")
    name_local = venue.get("name_local", "")
    address = venue.get("address", "")
    official = venue.get("official_url", "")
    cats_seed = ", ".join(venue.get("categories") or [])
    return (
        f"Write copy for a Divento permanent attraction or museum listing.\n\n"
        f"INPUTS\n"
        f"- Name: {name}\n"
        f"- Local-language name (if different): {name_local}\n"
        f"- City: {city}\n"
        f"- Country: {country}\n"
        f"- Address: {address}\n"
        f"- Official URL (use only as a fact-check reference, do not cite): {official}\n"
        f"- Search-phase category seeds (use to inform 'categories', refine if needed): {cats_seed}\n\n"
        f"AVAILABLE CATEGORIES (pick 1-3 most relevant, comma-separated):\n"
        f"{', '.join(DIVENTO_CATEGORIES)}\n\n"
        "GLOBAL PRIORITY\n"
        f"- Base all content on verified factual sources, prioritising the official website of {name} where possible.\n"
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
        "Never use: visitor(s), located, feature(d), showcase, blend, period, accessible, house(d), home(d), step into.\n"
        "Avoid all brochure-style language.\n\n"
        "FORMAT REQUIREMENTS\n"
        "- Spell out numbers from one to ten; use numerals from 11 upward.\n"
        "- Ensure consistent spacing.\n"
        "- Do not begin descriptions with the attraction name.\n"
        "- Do not start with: include, explore, step into.\n"
        "- Avoid wrap-up sentences and dashes.\n"
        "- Use active voice.\n"
        "- Do not use em/en dash characters (— or –); rewrite with commas or parentheses.\n\n"
        "HTML\n"
        "- Wrap each paragraph in <p></p> tags in 'long_en'.\n"
        "- Keep formatting clean and minimal.\n\n"
        "LONG DESCRIPTION (long_en)\n"
        "- Target 300-320 words.\n"
        "- Multiple paragraphs.\n"
        "- Must read as a continuous narrative, not a checklist.\n"
        "- Avoid formulaic openings.\n\n"
        "SHORT DESCRIPTION (short_en)\n"
        "- Maximum 164 characters.\n"
        "- One sentence only.\n"
        "- Aim for 20-25 words where possible.\n"
        "- Include a clear subject, a specific reason to visit, and at least one concrete detail.\n"
        "- Do not repeat the attraction name.\n"
        "- Do not be vague or promotional.\n\n"
        "META DESCRIPTION (meta_en)\n"
        "- Maximum 150 characters.\n"
        "- One sentence, SEO-style: name the attraction + the single most distinctive reason to visit.\n"
        "- Plain text only (no HTML).\n\n"
        "OUTPUT\n"
        "Return ONLY a JSON object with these keys:\n"
        "{\n"
        '  "short_en": "...",\n'
        '  "long_en":  "<p>...</p><p>...</p>",\n'
        '  "meta_en":  "...",\n'
        '  "categories": "Comma_Separated, From_Available_List"\n'
        "}"
    )


_PERM_COPY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "short_en": {"type": "string"},
        "long_en": {"type": "string"},
        "meta_en": {"type": "string"},
        "categories": {"type": "string"},
    },
    "required": ["short_en", "long_en", "meta_en", "categories"],
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
                max_output_tokens=3500,
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
                    max_output_tokens=3500,
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

    return {
        "short_en": _coerce_str(obj.get("short_en")),
        "long_en": _coerce_str(obj.get("long_en")),
        "meta_en": _coerce_str(obj.get("meta_en")),
        "categories": _normalise_categories(_coerce_str(obj.get("categories"))),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Phase 4 — translate
# ──────────────────────────────────────────────────────────────────────────────


_PERM_TRANSLATE_SYSTEM = (
    "You are a professional translator specialising in travel content. "
    "You translate faithfully, preserve historical and factual accuracy, "
    "keep HTML tags intact, and respond only with JSON matching the provided schema."
)


def _build_translate_prompt(
    *,
    name: str,
    city: str,
    short_en: str,
    long_en: str,
    meta_en: str,
) -> str:
    return (
        f'Translate these travel descriptions for "{name}" in {city} from English '
        "to French, Spanish, Italian, Russian, and Chinese (Simplified).\n\n"
        "Also translate the city name itself for each target language (e.g. "
        "Florence -> Florence/Florencia/Firenze/Флоренция/佛罗伦萨). If the city "
        "name does not have an established local translation, return it unchanged.\n\n"
        f"SHORT DESCRIPTION (English):\n{short_en}\n\n"
        f"LONG DESCRIPTION (English, HTML):\n{long_en}\n\n"
        f"META DESCRIPTION (English):\n{meta_en}\n\n"
        "Requirements:\n"
        "- Maintain the same HTML formatting in long descriptions.\n"
        "- Keep the same tone and informal style; address the reader directly where the source does.\n"
        "- Preserve historical accuracy and factual information; do not invent details.\n"
        "- Maintain British English spelling conventions in the source meaning.\n"
        "- Do not use em/en dash characters (— or –); rewrite with commas or parentheses.\n"
        "- For Chinese (zh), translate ALL prose into Simplified Chinese; Latin script is allowed ONLY for proper nouns and official artwork/series titles.\n\n"
        "Return ONLY a JSON object with this exact structure:\n"
        "{\n"
        '  "fr": {"short": "...", "long": "...", "meta": "...", "city": "..."},\n'
        '  "es": {"short": "...", "long": "...", "meta": "...", "city": "..."},\n'
        '  "it": {"short": "...", "long": "...", "meta": "...", "city": "..."},\n'
        '  "ru": {"short": "...", "long": "...", "meta": "...", "city": "..."},\n'
        '  "zh": {"short": "...", "long": "...", "meta": "...", "city": "..."}\n'
        "}"
    )


def _build_translate_schema(languages: list[str]) -> dict[str, Any]:
    lang_obj = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "short": {"type": "string"},
            "long": {"type": "string"},
            "meta": {"type": "string"},
            "city": {"type": "string"},
        },
        "required": ["short", "long", "meta", "city"],
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {lang: lang_obj for lang in languages},
        "required": languages,
    }


def _empty_translation_bundle() -> dict[str, dict[str, str]]:
    return {
        lang: {"short": "", "long": "", "meta": "", "city": ""}
        for lang in PERMANENT_LANGUAGES
    }


async def translate_venue(
    *,
    client,
    name: str,
    city: str,
    english: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Translate short/long/meta + city name into PERMANENT_LANGUAGES.
    Returns {lang: {short, long, meta, city}}; empties on failure."""
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
                    text={
                        "verbosity": "low",
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
            "city": _coerce_str((obj or {}).get("city")),
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
        return venue
    except Exception as exc:
        logger.exception(
            "perm.copy_translate.error venue=%s err=%r", venue.get("name"), exc
        )
        return None


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
