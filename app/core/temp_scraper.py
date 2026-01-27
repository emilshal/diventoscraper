import asyncio
import random
import re
import uuid
import json
import logging
from hashlib import sha256
from pathlib import Path
from datetime import datetime, timedelta
from html import unescape
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta
from openai import AsyncOpenAI

from app.config import settings
from app.core.ml import classify


# Temporary-exhibition specific constants
LANGUAGES = ["fr", "es", "it", "ru", "zh-CN"]

_TEMP_VENUE_IMAGE_CACHE: dict[str, str] = {}

_TEMP_SHORT_OPENERS = [
    "Trace",
    "Track",
    "Follow",
    "Compare",
    "Meet",
    "Study",
    "See",
    "Watch",
    "Handle",
    "Read",
    "Map",
    "Unpick",
    "Chart",
    "Listen",
]

_TEMP_LONG_OPENING_PREFIX_TEMPLATES = [
    "Rooms at {venue} map the show’s themes through objects, images, sound and text.",
    "Galleries at {venue} set out a clear route through materials, techniques and context.",
    "Wall texts and objects at {venue} pull focus to method, detail and historical context.",
    "A sequence of rooms at {venue} grounds the show in specific works and practical context.",
    "Labels at {venue} keep attention on evidence, making the subject read through objects and craft.",
    "Across the galleries at {venue}, materials and techniques carry the argument without hype.",
    "In the first rooms at {venue}, close looking matters as objects and images do the work.",
    "Inside the galleries at {venue}, the route moves from key works to supporting material and context.",
]

_CITY_VENUE_HINTS: dict[str, list[str]] = {
    # These are *search prompts* to encourage breadth. The model must still verify dates from web_search.
    "barcelona": [
        "MNAC exhibitions Barcelona",
        "Fundació Joan Miró exhibitions Barcelona",
        "CaixaForum Barcelona exhibitions",
        "Fundació Antoni Tàpies exhibitions Barcelona",
        "Museu Picasso Barcelona exhibitions",
        "CCCB exhibitions Barcelona",
        "MACBA exhibitions Barcelona",
        "Palau Robert exhibitions Barcelona",
        "Centre d'Art Santa Mònica exhibitions Barcelona",
        "Foto Colectania exhibitions Barcelona",
        "Disseny Hub Barcelona exhibitions",
        "Museu del Disseny exhibitions Barcelona",
    ],
    "london": [
        "British Museum exhibitions London",
        "Tate Modern exhibitions London",
        "National Gallery exhibitions London",
        "V&A exhibitions London",
        "British Library exhibitions London",
        "Science Museum exhibitions London",
        "Natural History Museum exhibitions London",
        "Barbican exhibitions London",
        "Hayward Gallery exhibitions London",
        "Serpentine exhibitions London",
        "Imperial War Museum exhibitions London",
        "Wellcome Collection exhibitions London",
        "Royal Academy exhibitions London",
        "Saatchi Gallery exhibitions London",
    ],
}


def _stable_index(key: str, modulo: int) -> int:
    if modulo <= 0:
        return 0
    h = sha256(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % modulo


def _pick_required_short_opener(
    *, seed: str, used: dict[str, int] | None = None
) -> str:
    base = _stable_index(seed, len(_TEMP_SHORT_OPENERS))
    if not used:
        return _TEMP_SHORT_OPENERS[base]
    for offset in range(len(_TEMP_SHORT_OPENERS)):
        cand = _TEMP_SHORT_OPENERS[(base + offset) % len(_TEMP_SHORT_OPENERS)]
        if used.get(cand, 0) == 0:
            used[cand] = 1
            return cand
    cand = _TEMP_SHORT_OPENERS[base]
    used[cand] = used.get(cand, 0) + 1
    return cand


def _pick_required_long_prefix(
    *, venue: str, seed: str, used: dict[str, int] | None = None
) -> str:
    templates = _TEMP_LONG_OPENING_PREFIX_TEMPLATES
    base = _stable_index(seed, len(templates))
    if not used:
        return templates[base].format(venue=venue).strip()
    for offset in range(len(templates)):
        cand = templates[(base + offset) % len(templates)].format(venue=venue).strip()
        if used.get(cand, 0) == 0:
            used[cand] = 1
            return cand
    cand = templates[base].format(venue=venue).strip()
    used[cand] = used.get(cand, 0) + 1
    return cand


TEMPORARY_COLUMNS_ORDER = [
    "Name of site, City",  # A
    "City",  # B
    "Country",  # C
    "Full address",  # D
    "Type(s) of activity",  # E
    "Divento Categories",  # F
    "Free activity?",  # G
    "Long description",  # H
    "Long description fr",  # I
    "Long description es",  # J
    "Long description it",  # K
    "Long description ru",  # L
    "Long description zh",  # M
    "URL of images",  # N
    "Legends of images",  # O
    "Duration of visit",  # P
    "Opening and closing time",  # Q
    "Short description",  # R
    "Short description fr",  # S
    "Short description es",  # T
    "Short description it",  # U
    "Short description ru",  # V
    "Short description zh",  # W
    "Meta description",  # X
    "Meta description fr",  # Y
    "Meta description es",  # Z
    "Meta description it",  # AA
    "Meta description ru",  # AB
    "Meta description zh",  # AC
    "Latitude",  # AD
    "Information",  # AE
    "Ticket URL",  # AE2
    "Longitude",  # AF
    "Activity type",  # AG
    "Rating",  # AH
    "Name of site city",  # AI
    "Name of site city fr",  # AJ
    "Name of site city es",  # AK
    "Name of site city it",  # AL
    "Name of site city ru",  # AM
    "Name of site city zh",  # AN
    "Real city",  # AO
    "Start date (YYYY-MM-DD)",  # AP
    "End date (YYYY-MM-DD)",  # AQ
    "Venue category path",  # AR
    "Repeat pattern",  # AS
    "Open days",  # AT
]


ua = UserAgent()
logger = logging.getLogger(__name__)

_OPENAI_CLIENTS: dict[str, AsyncOpenAI] = {}


def _get_openai_client() -> AsyncOpenAI | None:
    """
    Return a cached AsyncOpenAI client for the currently loaded API key.

    This is intentionally lazy so `.env` changes (plus restart) and any startup-time
    overrides are picked up without relying on import-time globals.
    """
    key = settings.OPENAI_API_KEY
    if not key:
        return None
    fp = sha256(key.encode("utf-8")).hexdigest()[:16]
    existing = _OPENAI_CLIENTS.get(fp)
    if existing is not None:
        return existing
    new_client = AsyncOpenAI(api_key=key)
    _OPENAI_CLIENTS[fp] = new_client
    return new_client


# Model selection (temporary exhibitions only)
TEMP_MODEL = settings.OPENAI_TEMP_MODEL
TEMP_SEARCH_MODEL = (
    settings.OPENAI_TEMP_SEARCH_MODEL or settings.OPENAI_TEMP_MODEL
).strip()
TEMP_TRANSLATION_MODEL = settings.OPENAI_TEMP_TRANSLATION_MODEL
TEMP_TRANSLATION_FALLBACK_MODEL = settings.OPENAI_TEMP_TRANSLATION_FALLBACK_MODEL
TEMP_COPY_MODEL = settings.OPENAI_TEMP_COPY_MODEL


def _ordinal_day(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _normalise_for_dedupe(value: str) -> str:
    if not value:
        return ""
    value = unescape(value)
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s{2,}", " ", value).strip()


def _contains_city(text: str, city: str) -> bool:
    t = _normalise_for_dedupe(text)
    c = _normalise_city_name(city)
    if not t or not c:
        return False
    return bool(re.search(r"\b" + re.escape(c) + r"\b", t))


def _format_date_range_label(start_date: str, end_date: str) -> str:
    def _parse(d: str):
        try:
            return datetime.fromisoformat(d).date()
        except Exception:
            return None

    start = _parse(start_date) if start_date else None
    end = _parse(end_date) if end_date else None
    if not start and not end:
        return ""
    if start and end:
        if start.year == end.year:
            # Same year: only include the year once at the end.
            start_label = f"{start.day} {start.strftime('%B')}"
            end_label = f"{end.day} {end.strftime('%B')} {end.year}"
            return f"{start_label}-{end_label}"
        start_label = f"{start.day} {start.strftime('%B')} {start.year}"
        end_label = f"{end.day} {end.strftime('%B')} {end.year}"
        return f"{start_label}-{end_label}"
    if start:
        return f"{start.day} {start.strftime('%B')} {start.year}"
    return f"{end.day} {end.strftime('%B')} {end.year}"


def _parse_date(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        try:
            return date_parser.parse(value).date()
        except Exception:
            return None


_OPENING_HOURS_DAY_ALIASES = {
    "mon": "Mon",
    "monday": "Mon",
    "tue": "Tue",
    "tues": "Tue",
    "tuesday": "Tue",
    "wed": "Wed",
    "wednesday": "Wed",
    "thu": "Thu",
    "thur": "Thu",
    "thurs": "Thu",
    "thursday": "Thu",
    "fri": "Fri",
    "friday": "Fri",
    "sat": "Sat",
    "saturday": "Sat",
    "sun": "Sun",
    "sunday": "Sun",
}


def _normalise_opening_hours(value: str) -> str:
    """
    Normalise opening hours to: Mon:HH:MM-HH:MM,Wed:HH:MM-HH:MM,...
    Allows multiple intervals per day separated by '/' (e.g. Mon:10:00-13:00/14:00-18:00).
    Returns empty string if it doesn't look valid.
    """
    if not value:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""

    # Fast path for already-normal strings (strip whitespace only).
    raw = re.sub(r"\s+", "", raw)

    parts = [p for p in raw.split(",") if p]
    if not parts:
        return ""

    day_parts: list[str] = []
    for part in parts:
        if ":" not in part:
            return ""
        day_raw, times_raw = part.split(":", 1)
        day = _OPENING_HOURS_DAY_ALIASES.get(day_raw.lower())
        if not day:
            return ""
        if not times_raw:
            return ""

        intervals = times_raw.split("/")
        norm_intervals: list[str] = []
        for interval in intervals:
            if "-" not in interval:
                return ""
            a, b = interval.split("-", 1)
            if not re.fullmatch(r"\d{2}:\d{2}", a) or not re.fullmatch(
                r"\d{2}:\d{2}", b
            ):
                return ""
            norm_intervals.append(f"{a}-{b}")
        day_parts.append(f"{day}:{'/'.join(norm_intervals)}")

    return ",".join(day_parts)


def _normalise_coord(value) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    try:
        return f"{float(raw)}"
    except Exception:
        m = re.search(r"-?\d+(?:\.\d+)?", raw.replace(",", " "))
        if m:
            try:
                return f"{float(m.group(0))}"
            except Exception:
                return ""
    return ""


def _normalise_duration_hours(value) -> str:
    if value is None:
        return ""
    s = str(value).strip().lower()
    if not s:
        return ""
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return ""
    num = float(m.group(1))
    if "min" in s:
        hours = num / 60.0
    elif "hour" in s:
        hours = num
    else:
        hours = num if num <= 10 else num / 60.0
    label = f"{hours:.2f}".rstrip("0").rstrip(".")
    return label


def _normalise_city_name(name: str) -> str:
    if not name:
        return ""
    parts = [p.strip() for p in str(name).split(",") if p.strip()]
    if not parts:
        return ""
    if len(parts) >= 2:
        a = parts[0].lower()
        # Handle common user input reversal: "Country, City"
        known_countries = {
            "spain",
            "italy",
            "france",
            "germany",
            "portugal",
            "netherlands",
            "belgium",
            "switzerland",
            "austria",
            "united kingdom",
            "uk",
            "england",
            "scotland",
            "wales",
            "ireland",
            "united states",
            "usa",
        }
        if a in known_countries:
            token = parts[1]
        else:
            token = parts[0]
    else:
        token = parts[0]
    return re.sub(r"\s+", " ", token).strip().lower()


def _clean_json_content(text: str) -> str:
    """Strip common wrappers like markdown code fences."""
    if not text:
        return text
    t = text.strip()
    if t.startswith("```"):
        t = t.lstrip("`")
        if "\n" in t:
            t = t.split("\n", 1)[1]
        t = t.strip("`").strip()
    return t


def _clean_json_content(text: str) -> str:
    if not text:
        return text
    t = text.strip()
    if t.startswith("```"):
        t = t.lstrip("`")
        if "\n" in t:
            t = t.split("\n", 1)[1]
        t = t.strip("`").strip()
    return t


def _extract_response_text(resp) -> str:
    """
    Best-effort extraction of plain text from an OpenAI Responses API result.
    """
    if resp is None:
        return ""
    try:
        output = getattr(resp, "output", None)
        parts: list[str] = []
        for item in output or []:
            content = getattr(item, "content", None) or []
            for part in content:
                text = getattr(part, "text", None)
                if isinstance(text, str) and text:
                    parts.append(text)
        return "\n".join(parts).strip()
    except Exception:
        return ""


def _abbrev_country(name: str) -> str:
    mapping = {
        "France": "FR",
        "United Kingdom": "UK",
        "UK": "UK",
        "England": "UK",
    }
    name_clean = (name or "").strip()
    return mapping.get(name_clean, name_clean)


def _abbrev_country_in_address(address: str, country: str) -> str:
    if not address:
        return address
    abbrev = _abbrev_country(country)
    if not abbrev or not country:
        return address
    return re.sub(rf"\b{re.escape(country)}\b", abbrev, address)


def _extract_json_array(payload: str):
    payload = payload.strip()
    try:
        data = json.loads(payload)
        return data if isinstance(data, list) else None
    except Exception:
        pass
    start = payload.find("[")
    end = payload.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            data = json.loads(payload[start : end + 1])
            return data if isinstance(data, list) else None
        except Exception:
            return None
    return None


def _extract_json_object(payload: str):
    payload = payload.strip()
    try:
        data = json.loads(payload)
        return data if isinstance(data, dict) else None
    except Exception:
        pass
    start = payload.find("{")
    end = payload.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            data = json.loads(payload[start : end + 1])
            return data if isinstance(data, dict) else None
        except Exception:
            return None
    return None


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html or "")
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _normalise_html_spacing(html: str) -> str:
    """
    Normalise whitespace in HTML strings so Excel cells don't render large blank gaps.
    Keeps tags/content intact but removes extra whitespace between tags and stray newlines.
    """
    if not html:
        return ""
    cleaned = html.replace("\r\n", "\n").replace("\r", "\n").strip()
    # Remove whitespace between tags (e.g. </p>\n\n<p> -> </p><p>)
    cleaned = re.sub(r">\s+<", "><", cleaned)
    # Remove remaining newlines (often inserted inside <p>...</p> by translation),
    # then collapse repeated whitespace.
    cleaned = cleaned.replace("\n", " ")
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned


def _word_count_html(html: str) -> int:
    text = _strip_html(html)
    return len([w for w in text.split(" ") if w])


def _contains_source_citations(text: str) -> bool:
    if not text:
        return False
    if "http://" in text or "https://" in text:
        return True
    if re.search(r"\(\[[^\]]+\]\([^)]+\)\)", text):
        return True
    return False


def _normalise_for_match(text: str) -> str:
    if not text:
        return ""
    t = unescape(str(text)).lower()
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _validate_temp_copy(
    title: str, short: str, long_html: str, address: str = ""
) -> list[str]:
    violations: list[str] = []

    short_clean = (short or "").strip()
    long_clean = (long_html or "").strip()
    combined_text = " ".join([short_clean, _strip_html(long_clean)]).strip()
    combined_low = combined_text.lower()

    if not short_clean:
        violations.append("short is empty")
    elif len(short_clean) > 164:
        violations.append(f"short exceeds 164 chars ({len(short_clean)})")
    elif short_clean.lower().startswith("explore "):
        violations.append("short starts with 'Explore' (vary the opening)")

    if not long_clean:
        violations.append("long is empty")
        return violations

    if not long_clean.startswith("<p>") or "</p>" not in long_clean:
        violations.append("long must be HTML paragraphs wrapped in <p> tags")

    # Avoid em/en dashes; hyphens are allowed for compounds like "16th-century".
    if "—" in long_clean or "–" in long_clean:
        violations.append("long contains dash characters (—/–)")

    if _contains_source_citations(long_clean):
        violations.append("long contains source citations/URLs")

    wc = _word_count_html(long_clean)
    if wc < 350 or wc > 400:
        violations.append(f"long word count must be 350–400 (got {wc})")

    lead = _strip_html(long_clean)[:80].lower()
    if lead.startswith("explore "):
        violations.append("long starts with 'Explore' (vary the opening)")
    title_head = (title or "").split(":", 1)[0].strip().lower()
    if title_head and lead.startswith(title_head):
        violations.append("long starts with exhibition name")
    if lead.startswith("this exhibition"):
        violations.append("long starts with 'this exhibition'")

    banned = [
        "you",
        "visitor",
        "visitors",
        "located",
        "feature",
        "featured",
        "showcase",
        "blend",
        "period",
        "accessible",
    ]
    low = _strip_html(long_clean).lower()
    for w in banned:
        if re.search(r"\b" + re.escape(w) + r"\b", low):
            violations.append(f"long uses banned word '{w}'")
            break

    # Do not mention exhibition dates in copy (dates are metadata only).
    months = (
        r"january|february|march|april|may|june|july|august|september|october|november|december|"
        r"jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
    )
    date_patterns = [
        r"\b20\d{2}-\d{2}-\d{2}\b",  # ISO
        rf"\b\d{{1,2}}\s+(?:{months})\s+20\d{{2}}\b",  # 7 January 2026
        rf"\b(?:{months})\s+\d{{1,2}},?\s+20\d{{2}}\b",  # January 7, 2026
        rf"\b\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{months})\b",  # 7th January
        rf"\b(?:{months})\s+\d{{1,2}}(?:st|nd|rd|th)?\b",  # January 7th
    ]
    if any(re.search(p, combined_low) for p in date_patterns):
        violations.append("copy mentions exhibition dates (remove dates from short/long)")
    if re.search(r"\bruns?\s+(from|until|to)\b", combined_low) or "running from" in combined_low:
        violations.append("copy mentions exhibition date range phrasing (remove date range wording)")

    # Do not mention visit duration / time-to-spend in copy (duration is metadata only).
    duration_patterns = [
        r"\b\d{1,3}\s*(?:minutes?|mins?|hours?|hrs?)\b",
        r"\b(?:one|two|three|four|five|six|seven|eight|nine|ten)\s+(?:minutes?|hours?)\b",
        r"\bhalf\s+an?\s+hour\b",
        r"\ban?\s+hour\b",
        r"\b(?:allow|plan|budget)\s+(?:about\s+)?an?\s+hour\b",
        r"\b(?:allow|plan|budget)\s+\d{1,3}\s*(?:minutes?|hours?)\b",
        r"\b(?:visit|route|circuit)\s+(?:takes|lasts)\b",
        r"\b(?:suggested|recommended)\s+(?:visit\s+)?duration\b",
    ]
    if any(re.search(p, combined_low) for p in duration_patterns):
        violations.append("copy mentions visit duration/time-to-spend (remove duration from short/long)")

    # Spell out numbers 1–10 in prose (no digits).
    # Note: this also discourages "1 hour" style phrasing, but we keep a separate duration rule above.
    if re.search(r"\b(?:10|[1-9])\b", combined_text):
        violations.append("copy uses digits 1–10; spell them out")

    # Do not include venue postal address in copy (no "address is ..." or street/postcode).
    if (
        re.search(r"\baddress\b", low)
        or "venue’s address" in low
        or "venue's address" in low
    ):
        violations.append("long includes address wording")

    addr_norm = _normalise_for_match(address)
    if addr_norm and len(addr_norm) >= 10:
        combined_norm = _normalise_for_match(" ".join([short_clean, long_clean]))
        if addr_norm in combined_norm:
            violations.append("copy includes the venue postal address")
        else:
            # Also flag partial leaks (street name / postcode / distinctive chunk).
            pieces: list[str] = []
            for chunk in re.split(r"[,\n]+", address):
                c = chunk.strip()
                if not c:
                    continue
                c_norm = _normalise_for_match(c)
                if len(c_norm) >= 8:
                    pieces.append(c_norm)
            # Common numeric postal codes (e.g. ES 08001, FR 75001, etc.).
            for m in re.finditer(r"\b\d{4,6}\b", address):
                pieces.append(m.group(0))
            for piece in sorted(set(pieces), key=len, reverse=True)[:8]:
                if piece and piece in combined_norm:
                    violations.append("copy includes part of the venue postal address")
                    break

    return violations


def _maybe_prefix_the_venue(venue: str, country: str) -> str:
    if not venue:
        return ""
    v = venue.strip()
    if not v:
        return ""
    c = (country or "").strip().lower()
    if not c:
        return v
    is_france = c == "fr" or c == "fra" or "france" in c
    if not is_france:
        return v
    if v.lower().startswith("the "):
        return v
    # Only apply to venue types where English convention typically uses "the".
    if re.search(r"\b(museum|foundation|fondation|gallery|centre|center)\b", v, flags=re.I):
        return f"the {v}"
    return v


def _extract_meta_image_url(html: str) -> str:
    """
    Extract an image URL from HTML meta tags.
    Prefer og:image, then twitter:image.
    """
    if not html:
        return ""
    # Keep this lightweight (regex) and tolerant of attribute ordering/quotes.
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
    return ""


def _fetch_url_text(url: str, *, timeout_s: int = 20) -> str:
    if not url:
        return ""
    try:
        headers = {"User-Agent": ua.random if "ua" in globals() else UserAgent().random}
    except Exception:
        headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout_s, allow_redirects=True)
        if resp.status_code >= 400:
            return ""
        # Don't let extremely large pages explode memory; meta tags are near top.
        text = resp.text
        return text[:750_000]
    except Exception:
        return ""


def _venue_homepage_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        p = urlparse(url)
        if not p.scheme or not p.netloc:
            return ""
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return ""


async def _get_venue_image_url_async(
    *, venue: str, city: str, country: str, source_url: str
) -> str:
    """
    Best-effort venue image URL suitable for the Excel `URL of images` column.
    Strategy:
    1) Try og:image/twitter:image from the exhibition `source_url`.
    2) Try og:image/twitter:image from the venue homepage (same domain).
    3) If still empty, ask OpenAI (web_search) for a public venue photo URL (fallback).
    """
    homepage = _venue_homepage_from_url(source_url) or ""
    cache_key = _normalise_for_dedupe("|".join([venue, city, country, homepage or source_url]))
    if cache_key and cache_key in _TEMP_VENUE_IMAGE_CACHE:
        return _TEMP_VENUE_IMAGE_CACHE.get(cache_key, "")

    # 1) Exhibition page
    html = await asyncio.to_thread(_fetch_url_text, source_url)
    img = _extract_meta_image_url(html)
    if not img and homepage:
        # 2) Venue homepage
        html_home = await asyncio.to_thread(_fetch_url_text, homepage)
        img = _extract_meta_image_url(html_home)

    # 3) OpenAI fallback (optional; only if we have a client).
    if not img:
        client = _get_openai_client()
        if client is not None:
            use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
            tools = [{"type": "web_search"}] if use_web_search_tool else None
            prompt = (
                "Find a public, non-press venue photo image URL for this museum/gallery.\n"
                "Return ONLY JSON with keys: image_url, page_url.\n"
                "- Prefer the official venue website.\n"
                "- Do not use press login/press kit pages.\n"
                "- image_url must be a direct image URL (jpg/png/webp).\n"
                "- If you cannot find a suitable image, return empty strings.\n\n"
                f"Venue: {venue}\nCity: {city}\nCountry: {country}\n"
            )
            try:
                resp = await _call_with_backoff(
                    lambda: client.responses.create(
                        model=TEMP_SEARCH_MODEL,
                        input=prompt,
                        tools=tools,
                        reasoning={"effort": "medium"},
                        text={"format": {"type": "json_object"}},
                        max_output_tokens=500,
                    ),
                    max_attempts=3,
                )
                content = _clean_json_content(
                    resp.output_text or _extract_response_text(resp) or ""
                )
                data = _extract_json_object(content) if content else None
                if isinstance(data, dict):
                    img = str(data.get("image_url") or "").strip()
            except Exception:
                img = ""

    if cache_key:
        _TEMP_VENUE_IMAGE_CACHE[cache_key] = img or ""
    return img or ""


def _generate_temp_copy(
    *,
    title: str,
    venue: str,
    city: str,
    country: str,
    address: str,
    start_date: str,
    end_date: str,
    duration: str,
    avoid_short_openers: list[str] | None = None,
    avoid_long_openers: list[str] | None = None,
) -> dict[str, str]:
    """
    Pass 2: generate English short/long copy for a single temporary exhibition.
    Returns dict: {"short": "...", "long": "<p>...</p>..."}.
    """
    client = _get_openai_client()
    if client is None:
        return {"short": "", "long": ""}

    avoid_short_openers = [
        w.strip() for w in (avoid_short_openers or []) if w and w.strip()
    ]
    avoid_long_openers = [
        w.strip() for w in (avoid_long_openers or []) if w and w.strip()
    ]
    avoid_short_clause = (
        "- Do not start the short description with any of these opening words: "
        + ", ".join(sorted(set(avoid_short_openers))[:12])
        + ".\n"
        if avoid_short_openers
        else ""
    )
    avoid_long_clause = (
        "- Do not start the long description with any of these opening words: "
        + ", ".join(sorted(set(avoid_long_openers))[:12])
        + ".\n"
        if avoid_long_openers
        else ""
    )

    base_prompt = (
        "Write copy for a Divento temporary exhibition listing.\n\n"
        "INPUTS\n"
        f"- Title: {title}\n"
        f"- Venue: {venue}\n"
        f"- City: {city}\n"
        f"- Country: {country}\n"
        "\n"
        "HARD CONSTRAINTS (must pass)\n"
        "- Write as though the exhibition is already running; do not use future tense.\n"
        "- British English spelling.\n"
        "- Never use the first person.\n"
        "- Do not use these words anywhere: You, visitor, visitors, located, feature, featured, showcase, blend, period, accessible.\n"
        "- Do not use em/en dash characters: — or –. If you want that pause, rewrite with commas or parentheses.\n"
        "- Do not include citations, links, or URLs.\n\n"
        "- Do not mention the exhibition dates anywhere.\n"
        "- Do not mention the visit duration/time-to-spend anywhere.\n"
        "- Spell out numbers one to ten in words (no digits 1–10).\n"
        "- Do not include the venue’s postal address (no street, postcode, or 'address is …').\n\n"
        "LONG (HTML)\n"
        "- 350 to 400 words. Target 380–395 words.\n"
        "- If you are under 350 words, add one more factual sentence to reach the minimum.\n"
        "- Multiple paragraphs; wrap each paragraph in <p> tags.\n"
        "- Do not begin with the exhibition name or 'this exhibition'.\n"
        f"- Start the long description (immediately after <p>) with this exact sentence: {required_long_prefix}\n"
        f"{avoid_long_clause}"
        "- Avoid concluding sentences.\n"
        "- Include naturally: one highlight, two don't-miss elements, and one hidden gem.\n"
        "- Mention at least three specific works, artists, or items when possible.\n"
        "- Use strong verbs, concrete nouns, active voice; cut filler.\n"
        "- No brochure-style language, clichés, or exaggerated adjectives.\n\n"
        "SHORT\n"
        "- Maximum 164 characters.\n"
        "- Must include a verb.\n"
        "- Must not repeat the exhibition name.\n"
        "- Must not repeat the phrasing or start of the long description.\n\n"
        "- Do not start the short description with 'Explore'.\n"
        f"{avoid_short_clause}"
        "OUTPUT\n"
        "- Return ONLY a JSON object with exactly the keys 'short' and 'long'.\n"
        "- Both 'short' and 'long' must be non-empty strings.\n"
    )

    last_violations: list[str] = []
    last_json: str = ""
    best: dict[str, str] | None = None
    best_violations: list[str] = []
    raise RuntimeError(
        "_generate_temp_copy is sync-only; use _generate_temp_copy_async"
    )


def _is_insufficient_quota_error(exc: Exception) -> bool:
    msg = repr(exc)
    return "insufficient_quota" in msg or "exceeded your current quota" in msg


async def _sleep_with_jitter(seconds: float) -> None:
    await asyncio.sleep(seconds + random.random() * 0.25)


async def _call_with_backoff(awaitable_factory, *, max_attempts: int = 5):
    """
    Run an async OpenAI call with basic backoff for 429/rate-limit style errors.
    Stops immediately for insufficient quota errors.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await awaitable_factory()
        except Exception as exc:  # noqa: BLE001
            if _is_insufficient_quota_error(exc):
                raise
            last_exc = exc
            # Heuristic: retry on rate limits and transient transport errors.
            msg = repr(exc)
            retryable = (
                ("429" in msg)
                or ("Too Many Requests" in msg)
                or ("RateLimit" in msg)
                or ("TCPTransport closed" in msg)
                or ("handler is closed" in msg)
                or ("ConnectError" in msg)
                or ("ReadTimeout" in msg)
                or ("ConnectTimeout" in msg)
                or ("RemoteProtocolError" in msg)
            )
            if not retryable or attempt == max_attempts:
                raise
            await _sleep_with_jitter(min(20.0, 0.75 * (2 ** (attempt - 1))))
    raise last_exc or RuntimeError("OpenAI call failed")


async def _generate_temp_copy_async(
    *,
    title: str,
    venue: str,
    city: str,
    country: str,
    address: str,
    start_date: str,
    end_date: str,
    duration: str,
    avoid_short_openers: list[str] | None = None,
    avoid_long_openers: list[str] | None = None,
    required_short_opener: str | None = None,
    required_long_prefix: str | None = None,
) -> dict[str, str]:
    """
    Pass 2: generate English short/long copy for a single temporary exhibition.
    Returns dict: {"short": "...", "long": "<p>...</p>..."}.
    """
    client = _get_openai_client()
    if client is None:
        return {"short": "", "long": ""}

    avoid_short_openers = [
        w.strip() for w in (avoid_short_openers or []) if w and w.strip()
    ]
    avoid_long_openers = [
        w.strip() for w in (avoid_long_openers or []) if w and w.strip()
    ]
    avoid_short_clause = (
        "- Do not start the short description with any of these opening words: "
        + ", ".join(sorted(set(avoid_short_openers))[:12])
        + ".\n"
        if avoid_short_openers
        else ""
    )
    avoid_long_clause = (
        "- Do not start the long description with any of these opening words: "
        + ", ".join(sorted(set(avoid_long_openers))[:12])
        + ".\n"
        if avoid_long_openers
        else ""
    )

    base_prompt = (
        "Write copy for a Divento temporary exhibition listing.\n\n"
        "INPUTS\n"
        f"- Title: {title}\n"
        f"- Venue: {venue}\n"
        f"- City: {city}\n"
        f"- Country: {country}\n"
        "\n"
        "HARD CONSTRAINTS (must pass)\n"
        "- Write as though the exhibition is already running; do not use future tense.\n"
        "- British English spelling.\n"
        "- Keep it casual, like talking to a friend, while staying factual and concrete.\n"
        "- Use natural rhythm and occasional contractions (it's, there's, don't) to avoid robotic phrasing.\n"
        "- Make the writing flow: vary sentence length, use smooth transitions between ideas, and avoid a rigid checklist feel.\n"
        "- Never use the first person.\n"
        "- Do not address the reader directly (no second person).\n"
        "- Do not use these words anywhere: You, visitor, visitors, located, feature, featured, showcase, blend, period, accessible.\n"
        "- Do not use em/en dash characters: — or –. If you want that pause, rewrite with commas or parentheses.\n"
        "- Do not include citations, links, or URLs.\n"
        "- Do not mention the exhibition dates anywhere.\n"
        "- Do not mention the visit duration/time-to-spend anywhere.\n"
        "- Spell out numbers one to ten in words (no digits 1–10).\n"
        "- Do not include the venue’s postal address (no street, postcode, or 'address is …').\n\n"
        "LONG (HTML)\n"
        "- 350 to 400 words. Target 380–395 words.\n"
        "- If you are under 350 words, add one more factual sentence to reach the minimum.\n"
        "- Multiple paragraphs; wrap each paragraph in <p> tags.\n"
        "- Do not begin with the exhibition name or 'this exhibition'.\n"
        f"- Start the long description (immediately after <p>) with this exact sentence: {required_long_prefix}\n"
        f"{avoid_long_clause}"
        "- Avoid concluding sentences.\n"
        "- Include naturally: one highlight, two don't-miss elements, and one hidden gem.\n"
        "- Mention at least three specific works, artists, or items when possible.\n"
        "- Use strong verbs, concrete nouns, active voice; cut filler.\n"
        "- No brochure-style language, clichés, or exaggerated adjectives.\n\n"
        "SHORT\n"
        "- Maximum 164 characters.\n"
        "- Must include a verb.\n"
        "- Must not repeat the exhibition name.\n"
        "- Must not repeat the phrasing or start of the long description.\n\n"
        f"- Start the short description with the exact first word '{required_short_opener}'.\n"
        "- Do not start the short description with 'Explore'.\n"
        f"{avoid_short_clause}"
        "OUTPUT\n"
        "- Return ONLY a JSON object with exactly the keys 'short' and 'long'.\n"
        "- Both 'short' and 'long' must be non-empty strings.\n"
    )

    last_violations: list[str] = []
    last_json: str = ""
    best: dict[str, str] | None = None
    best_violations: list[str] = []
    for _attempt in range(5):
        prompt = base_prompt
        if last_violations:
            prompt += (
                "\nPREVIOUS OUTPUT (JSON)\n"
                f"{last_json}\n\n"
                "Fix these issues:\n- "
                + "\n- ".join(last_violations)
                + "\nReturn ONLY the corrected JSON object.\n"
            )

        content = ""
        try:
            resp = await _call_with_backoff(
                lambda: client.responses.create(
                    model=TEMP_COPY_MODEL,
                    input=prompt,
                    reasoning={"effort": "medium"},
                    text={
                        "verbosity": "low",
                        "format": {
                            "type": "json_schema",
                            "name": "temp_exhibition_copy",
                            "strict": True,
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "short": {"type": "string"},
                                    "long": {"type": "string"},
                                },
                                "required": ["short", "long"],
                            },
                        },
                    },
                    max_output_tokens=2500,
                )
            )
            content = _clean_json_content(
                resp.output_text or _extract_response_text(resp) or ""
            )
        except Exception as exc_schema:  # noqa: BLE001
            print(
                "DEBUG _generate_temp_copy_async Responses json_schema error:",
                repr(exc_schema),
            )
            try:
                resp = await _call_with_backoff(
                    lambda: client.responses.create(
                        model=TEMP_COPY_MODEL,
                        input=prompt,
                        reasoning={"effort": "medium"},
                        text={"verbosity": "low", "format": {"type": "json_object"}},
                        max_output_tokens=2500,
                    )
                )
                content = _clean_json_content(
                    resp.output_text or _extract_response_text(resp) or ""
                )
            except Exception as exc_obj:  # noqa: BLE001
                print(
                    "DEBUG _generate_temp_copy_async Responses json_object error:",
                    repr(exc_obj),
                )
                content = ""

        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            last_violations = ["output was not valid JSON object"]
            continue

        short = str(data.get("short") or "").strip()
        long_html = str(data.get("long") or "").strip()
        try:
            last_json = json.dumps(
                {"short": short, "long": long_html}, ensure_ascii=False
            )
        except Exception:
            last_json = content[:2000]

        last_violations = _validate_temp_copy(title, short, long_html, address)
        if required_short_opener:
            first = (short.split(" ", 1)[0] if short else "").strip()
            if first != required_short_opener:
                last_violations.append(
                    f"short must start with '{required_short_opener}'"
                )
        if required_long_prefix and long_html:
            body = long_html
            if body.startswith("<p>"):
                body = body[3:]
            if not body.startswith(required_long_prefix):
                last_violations.append(f"long must start with '{required_long_prefix}'")
        if long_html and (not best or len(last_violations) < len(best_violations)):
            best = {"short": short, "long": long_html}
            best_violations = last_violations[:]

        if not last_violations:
            return {"short": short, "long": long_html}

    if best:
        if best_violations:
            print(
                "DEBUG _generate_temp_copy_async returning best-effort output with violations:",
                best_violations,
            )
        return best
    return {"short": "", "long": ""}


def _zh_latin_leaks(text: str) -> list[str]:
    stripped = _strip_html(text)
    leaks: list[str] = []
    for m in re.finditer(r"[A-Za-z][A-Za-z'\\-]{2,}", stripped):
        token = m.group(0)
        if any("A" <= ch <= "Z" for ch in token):
            continue
        leaks.append(token)
    return leaks


def _validate_translation_bundle(bundle: dict, *, languages: list[str]) -> list[str]:
    violations: list[str] = []
    for lang in languages:
        obj = bundle.get(lang)
        if not isinstance(obj, dict):
            violations.append(f"{lang} missing object")
            continue
        name = str(obj.get("name") or "").strip()
        short = str(obj.get("short") or "").strip()
        long_html = str(obj.get("long") or "").strip()
        if not name:
            violations.append(f"{lang}.name empty")
        if not short:
            violations.append(f"{lang}.short empty")
        elif len(short) > 164:
            violations.append(f"{lang}.short > 164 chars ({len(short)})")
        if not long_html:
            violations.append(f"{lang}.long empty")
        else:
            if not long_html.startswith("<p>") or "</p>" not in long_html:
                violations.append(f"{lang}.long missing <p> wrapper")
            if "—" in long_html or "–" in long_html:
                violations.append(f"{lang}.long contains dash characters (—/–)")
            if _contains_source_citations(long_html):
                violations.append(f"{lang}.long contains URLs/citations")
        if lang.lower() in {"zh-cn", "zh"}:
            leaks = _zh_latin_leaks(" ".join([name, short, long_html]))
            if leaks:
                violations.append(
                    f"{lang} contains leftover English prose: {', '.join(sorted(set(leaks))[:8])}"
                )
    return violations


async def _translate_bundle_async(
    *,
    title_en: str,
    short_en: str,
    long_en_html: str,
    languages: list[str],
) -> dict[str, dict[str, str]]:
    """
    Translate name/short/long for all target languages in one call per exhibition.
    Returns: {lang: {"name": str, "short": str, "long": str}, ...}
    """
    client = _get_openai_client()
    if client is None:
        return {lang: {"name": "", "short": "", "long": ""} for lang in languages}

    lang_obj_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "name": {"type": "string"},
            "short": {"type": "string"},
            "long": {"type": "string"},
        },
        "required": ["name", "short", "long"],
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {lang: lang_obj_schema for lang in languages},
        "required": languages,
    }

    base_prompt = (
        "Translate the following Divento temporary exhibition content.\n\n"
        "Rules (follow exactly):\n"
        "- Return ONLY a JSON object matching the provided schema.\n"
        "- Do not add citations, links, or URLs.\n"
        "- Avoid em/en dash characters (— or –); rewrite with commas or parentheses.\n"
        "- Keep all HTML tags intact in 'long' and return valid HTML.\n"
        "- Do NOT add extra blank lines/newlines.\n"
        "- For 'short': maximum 164 characters in the target language, keep natural phrasing.\n"
        "- For 'name': translate only what should be translated; keep proper nouns/titles as appropriate.\n"
        "- For zh-CN: translate ALL prose into Simplified Chinese; Latin script is allowed ONLY for proper nouns and official artwork/series titles.\n\n"
        "English inputs:\n"
        f"- name_en: {title_en}\n"
        f"- short_en: {short_en}\n"
        "long_en_html:\n"
        f"{long_en_html}\n"
    )

    last_json: str = ""
    last_violations: list[str] = []
    for _attempt in range(3):
        prompt = base_prompt
        if last_violations and last_json:
            prompt += (
                "\nPREVIOUS OUTPUT (JSON)\n"
                f"{last_json}\n\n"
                "Fix these issues:\n- "
                + "\n- ".join(last_violations)
                + "\nReturn ONLY the corrected JSON.\n"
            )

        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_TRANSLATION_MODEL,
                input=prompt,
                reasoning={"effort": "medium"},
                text={
                    "verbosity": "low",
                    "format": {
                        "type": "json_schema",
                        "name": "temp_translation_bundle",
                        "strict": True,
                        "schema": schema,
                    },
                },
                max_output_tokens=9000,
            )
        )
        content = _clean_json_content(
            resp.output_text or _extract_response_text(resp) or ""
        )
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            last_violations = ["output was not valid JSON object"]
            last_json = content[:2000]
            continue

        cleaned: dict[str, dict[str, str]] = {}
        for lang in languages:
            obj = data.get(lang) if isinstance(data.get(lang), dict) else {}
            name = str((obj or {}).get("name") or "").strip()
            short = str((obj or {}).get("short") or "").strip()
            long_html = _normalise_html_spacing(str((obj or {}).get("long") or ""))
            cleaned[lang] = {"name": name, "short": short, "long": long_html}

        last_violations = _validate_translation_bundle(cleaned, languages=languages)
        try:
            last_json = json.dumps(cleaned, ensure_ascii=False)
        except Exception:
            last_json = content[:2000]
        if not last_violations:
            return cleaned

    if last_json:
        fallback = _extract_json_object(last_json) or {}
        if isinstance(fallback, dict):
            return {
                lang: {
                    "name": str((fallback.get(lang) or {}).get("name") or "").strip(),
                    "short": str((fallback.get(lang) or {}).get("short") or "").strip(),
                    "long": _normalise_html_spacing(
                        str((fallback.get(lang) or {}).get("long") or "")
                    ),
                }
                for lang in languages
            }
    return {lang: {"name": "", "short": "", "long": ""} for lang in languages}


def _build_combinations_sheet(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in df.iterrows():
        name = str(row.get("Name of site, City", "")).strip()
        start_s = str(row.get("Start date (YYYY-MM-DD)", "")).strip()
        end_s = str(row.get("End date (YYYY-MM-DD)", "")).strip()
        if not name or not start_s:
            continue
        if not end_s:
            end_s = start_s
        try:
            start = datetime.fromisoformat(start_s).date()
            end = datetime.fromisoformat(end_s).date()
        except Exception:
            continue
        if end < start:
            start, end = end, start
        cur = start
        while cur <= end:
            rows.append(
                {
                    "Name of site, City": name,
                    "Date": cur.isoformat(),
                    "Quantity": 1,
                }
            )
            cur = cur.fromordinal(cur.toordinal() + 1)
    return pd.DataFrame(rows, columns=["Name of site, City", "Date", "Quantity"])


def _fetch_temporary_exhibitions_window(
    city: str,
    window_start,
    window_end,
    *,
    target_min: int = 0,
    target_max: int = 20,
) -> list[dict]:
    raise RuntimeError(
        "_fetch_temporary_exhibitions_window is sync-only; use _fetch_temporary_exhibitions_window_async"
    )


async def _fetch_temporary_exhibitions_window_async(
    city: str,
    window_start,
    window_end,
    *,
    target_min: int = 0,
    target_max: int = 20,
) -> list[dict]:
    client = _get_openai_client()
    if client is None:
        return []

    today_iso = window_start.isoformat()
    end_iso = window_end.isoformat()
    soon_cutoff = window_start + timedelta(days=30)
    soon_cutoff_iso = soon_cutoff.isoformat()
    use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
    city_norm = _normalise_city_name(city)
    venue_hints = _CITY_VENUE_HINTS.get(city_norm, [])
    venue_hint_block = ""
    if venue_hints:
        venue_hint_block = (
            "Venue coverage checklist (use these as search queries; verify dates from the pages you find):\n- "
            + "\n- ".join(venue_hints[:16])
            + "\n\n"
        )
    if target_max <= 0:
        select_clause = (
            "- Return as many distinct exhibitions as you can find for this window (no duplicates). "
            "Search deeply across multiple venues/sources.\n\n"
        )
    elif target_max == 1:
        select_clause = "- Return exactly 1 distinct exhibition (no duplicates).\n\n"
    else:
        select_clause = (
            "- Return as many exhibitions as you can find for this window: it is VERY IMPORTANT to reach 15 to 20 "
            "distinct exhibitions. Search deeply across multiple museums and venues/sources, keep count, and keep adding until you "
            "hit the maximum or truly run out. No duplicates.\n\n"
        )
    tool_line = "- Use the web_search tool.\n" if use_web_search_tool else ""
    evidence_lines = (
        "- Use web_search to find real pages.\n"
        "- Do not invent exhibitions, dates, venues, addresses, or opening hours.\n"
        "- If you cannot reliably find a specific field, return an empty string for that field (except dates).\n"
        "- Dates (start_date/end_date) should match what you find on the web; do not guess.\n"
        "- Opening hours should come from the venue’s official hours page when possible; if not verified, return opening_hours as an empty string.\n"
    )
    if not use_web_search_tool:
        evidence_lines = (
            "- Use web sources (the model’s built-in browsing/search) to verify every field.\n"
            "- Dates (start_date/end_date) MUST match what you find on the web; do not guess.\n"
            "- Opening hours MUST be taken from the venue’s official hours page or a reliable source; if not verified, return opening_hours as an empty string.\n"
            "- If you cannot reliably find a specific field, return an empty string for that field.\n"
        )
    prompt = (
        "I need to create new exhibitions for Divento.\n"
        "Please follow these steps exactly.\n\n"
        "IMPORTANT (anti-hallucination):\n"
        f"{tool_line}"
        f"{evidence_lines}"
        "- Prefer official venue/museum pages.\n\n"
        "Naming policy:\n"
        "- Prefer English-language official pages when available.\n"
        "- Return the exhibition title exactly as written on the source_url page you used; do not translate it.\n\n"
        "Venue diversity requirement:\n"
        "- Do NOT return results from only 2–4 venues.\n"
        "- Aim to cover at least 8 distinct venues when possible.\n"
        "- Until you have at least 8 venues, cap at 3 exhibitions per venue (keep searching other venues).\n"
        "- If the city truly has fewer venues with temporary exhibitions in this window, return what you can verify.\n\n"
        f"{venue_hint_block}"
        "1. Select exhibitions\n"
        f"- Assume today's date is {today_iso}.\n"
        f"- Do not include exhibitions that end before {soon_cutoff_iso} (ending within the next 30 days).\n"
        f"- The end of the search window is {end_iso}.\n"
        f"- Search for all temporary exhibitions happening in {city} between {today_iso} "
        f"and {end_iso} (inclusive).\n"
        f"- Include exhibitions that started before {today_iso} but are still running during this window (any date overlap counts).\n"
        "- Only include temporary exhibitions, not permanent collections or long-term displays.\n"
        f"{select_clause}"
        "2. Return structured data\n"
        "For each exhibition, you must provide the following fields:\n"
        "- name: exhibition label formatted as "
        '"Exhibition name, Exhibition, Venue, City: Dates" unless the word '
        '"exhibition" is already in the exhibition name (then do not repeat it).\n'
        "- city: the city where the exhibition takes place.\n"
        "- country: the country for that city.\n"
        "- address: the full postal address of the venue.\n"
        "- duration: suggested visit duration as free text (for example '1 hour' or '90 minutes').\n"
        "- start_date: exhibition start date in ISO format YYYY-MM-DD.\n"
        "- end_date: exhibition end date in ISO format YYYY-MM-DD.\n"
        "- venue: the name of the hosting museum or gallery.\n"
        "- source_url: a URL for verification (official venue page strongly preferred). If unavailable, return an empty string.\n"
        "- Do not include citations or markdown links in any string fields (URLs are allowed ONLY in 'source_url').\n\n"
        "3. Pricing and ticketing\n"
        "- is_free: explicitly check ticketing; set to 0 ONLY if the exhibition is clearly free (explicitly free entry). If there is any ticket price or it is unclear, set to 1. Do not guess 0. Return this as the string '0' or '1'.\n"
        "- ticket_url: ALWAYS return an empty string.\n\n"
        "4. Additional info\n"
        "- information: one or two sentences of additional factual context or practical "
        "information about the exhibition, without marketing language.\n"
        "- latitude and longitude: REQUIRED. Provide decimal coordinates for the venue based on reliable sources; do not leave blank. Use decimals (e.g. 48.8606, 2.3376).\n\n"
        "5. Opening pattern\n"
        "- repeat_pattern: 'daily' or 'weekly' based on how the exhibition runs.\n"
        "- open_days: comma-separated weekdays when open (e.g. Tue,Wed,Thu,Fri,Sat,Sun). If open daily, return Mon,Tue,Wed,Thu,Fri,Sat,Sun.\n\n"
        "6. Venue opening hours (required)\n"
        "- opening_hours: venue opening hours in this exact format (no spaces):\n"
        "  Mon:12:00-14:00,Wed:08:00-12:00,Fri:14:00-18:00\n"
        "  Use 24-hour time with leading zeros; omit days the venue is closed.\n"
        "  If the venue has multiple intervals in a day, use '/' inside that day:\n"
        "  Mon:10:00-13:00/14:00-18:00\n"
        "  If hours cannot be verified reliably, return an empty string.\n\n"
        "Do not include image URLs or legends.\n\n"
        "Return ONLY a raw JSON array and nothing else (no code fences, no prose). If you find no exhibitions, return an empty JSON array [].\n"
        "The top-level value must be a JSON array. Each element must "
        "be an object with exactly the keys: "
        "'name', 'city', 'country', 'address', 'duration', 'start_date', 'end_date', "
        "'is_free', 'ticket_url', 'information', 'venue', 'latitude', 'longitude', 'repeat_pattern', 'open_days', 'opening_hours', 'source_url'."
    )

    try:
        content = ""
        try:
            tools_arg = [{"type": "web_search"}] if use_web_search_tool else None
            resp = await _call_with_backoff(
                lambda: client.responses.create(
                    model=TEMP_SEARCH_MODEL,
                    input=prompt,
                    tools=tools_arg,
                    max_output_tokens=12000,
                )
            )
            content = _clean_json_content(
                resp.output_text or _extract_response_text(resp) or ""
            )
        except Exception as exc_resp:  # noqa: BLE001
            print("DEBUG Responses API error:", repr(exc_resp))
            logger.debug("temp_search_responses_error city=%s err=%r", city, exc_resp)
            content = ""

        async def _responses_retry() -> str:
            try:
                tools_arg = [{"type": "web_search"}] if use_web_search_tool else None
                resp_retry = await _call_with_backoff(
                    lambda: client.responses.create(
                        model=TEMP_SEARCH_MODEL,
                        input=prompt,
                        tools=tools_arg,
                        max_output_tokens=12000,
                    )
                )
                return _clean_json_content(
                    resp_retry.output_text or _extract_response_text(resp_retry) or ""
                )
            except Exception as exc_retry:  # noqa: BLE001
                print("DEBUG Responses API retry error:", repr(exc_retry))
                logger.debug(
                    "temp_search_responses_retry_error city=%s err=%r", city, exc_retry
                )
                return ""

        if not content or content.strip() in ("[]", ""):
            content = await _responses_retry()

        if content.strip() in ("[]", ""):
            content = await _responses_retry()

        print("DEBUG _fetch_temporary_exhibitions_window raw content:", content[:500])
        logger.debug(
            "temp_search_raw city=%s chars=%s snippet=%r",
            city,
            len(content or ""),
            (content or "")[:1200],
        )
        if not content or content.strip() in ("[]", ""):
            print("DEBUG _fetch_temporary_exhibitions_window parsed items: 0 (empty)")
            logger.debug(
                "temp_search_empty city=%s raw=%r", city, (content or "")[:2000]
            )
            return []

        data = _extract_json_array(content)
        if data is None:
            logger.debug(
                "temp_search_parse_failed city=%s raw=%r", city, (content or "")[:2000]
            )
            content_retry = await _responses_retry()
            if content_retry and content_retry.strip() not in ("[]", ""):
                logger.debug(
                    "temp_search_retry_raw city=%s chars=%s snippet=%r",
                    city,
                    len(content_retry or ""),
                    (content_retry or "")[:1200],
                )
                data = _extract_json_array(content_retry)
            if data is None:
                logger.debug("temp_search_parse_failed_after_retry city=%s", city)
                return []

        logger.debug("temp_search_parsed city=%s items=%s", city, len(data))
        filtered: list[dict] = []
        for item in data:
            if not isinstance(item, dict):
                continue

            # Prompt should keep results grounded; we avoid hard drops here and instead normalise.
            source_url = (item.get("source_url") or "").strip()

            s_raw = (item.get("start_date") or "").strip()
            e_raw = (item.get("end_date") or "").strip()
            s_date = _parse_date(s_raw)
            e_date = _parse_date(e_raw) if e_raw else None
            if s_date:
                item["start_date"] = s_date.isoformat()
            if e_date:
                item["end_date"] = e_date.isoformat()
            elif s_date and not e_raw:
                item["end_date"] = s_date.isoformat()

            # Skip exhibitions ending soon (< 30 days from window_start).
            if e_date and e_date < soon_cutoff:
                continue

            item["latitude"] = _normalise_coord(item.get("latitude"))
            item["longitude"] = _normalise_coord(item.get("longitude"))
            item["venue"] = (item.get("venue") or "").strip()
            item["city"] = (item.get("city") or city).strip()
            item["country"] = _abbrev_country((item.get("country") or "").strip())
            item["address"] = _abbrev_country_in_address(
                (item.get("address") or "").strip(), item["country"]
            )
            item["opening_hours"] = _normalise_opening_hours(
                (item.get("opening_hours") or "").strip()
            )
            item["repeat_pattern"] = (item.get("repeat_pattern") or "").strip()
            item["open_days"] = (item.get("open_days") or "").strip()
            item["information"] = (item.get("information") or "").strip()
            item["ticket_url"] = (item.get("ticket_url") or "").strip()
            item["duration"] = (item.get("duration") or "").strip()
            item["source_url"] = source_url
            filtered.append(item)

        logger.debug("temp_search_filtered city=%s kept=%s", city, len(filtered))
        if target_max > 0 and len(filtered) > target_max:
            filtered = filtered[:target_max]
        return filtered
    except Exception as exc:  # noqa: BLE001
        print("DEBUG _fetch_temporary_exhibitions_window OpenAI error:", repr(exc))
        logger.debug("temp_search_error city=%s err=%r", city, exc)
        raise


def _choose_temp_category(description: str) -> str:
    allowed = ["Whats_hot", "Top_Exhibitions", "Arts_and_Culture"]
    display_map = {"Whats_hot": "Whats_Hot"}
    try:
        scores = classify(description or "")
    except Exception:
        scores = None
    if not isinstance(scores, list):
        return display_map.get("Top_Exhibitions", "Top_Exhibitions")
    best_cat = "Top_Exhibitions"
    best_val = -1
    for cat in allowed:
        try:
            idx = allowed.index(cat)
        except Exception:
            continue
        val = scores[idx] if idx < len(scores) else 0
        if val > best_val:
            best_val = val
            best_cat = cat
    return display_map.get(best_cat, best_cat)


def scrape_temporary_exhibitions(
    city: str,
    *,
    months: int = 24,
    start_date=None,
    end_date=None,
    languages: list[str] | None = None,
) -> pd.DataFrame:
    # `asyncio.run()` cannot be called from inside an existing event loop (e.g. notebooks).
    # This function is called from a FastAPI background thread in `app/ui.py`, so it is safe.
    return asyncio.run(
        scrape_temporary_exhibitions_async(
            city, months=months, start_date=start_date, end_date=end_date, languages=languages
        )
    )


async def scrape_temporary_exhibitions_async(
    city: str,
    *,
    months: int = 24,
    start_date=None,
    end_date=None,
    languages: list[str] | None = None,
) -> pd.DataFrame:
    if languages is None:
        languages = LANGUAGES

    target_max = int(getattr(settings, "TEMP_MAX_EXHIBITIONS", 20) or 0)
    if start_date is not None and end_date is not None:
        try:
            window_start = (
                start_date
                if hasattr(start_date, "isoformat")
                else datetime.fromisoformat(str(start_date)).date()
            )
            window_end = (
                end_date
                if hasattr(end_date, "isoformat")
                else datetime.fromisoformat(str(end_date)).date()
            )
        except Exception as exc:  # noqa: BLE001
            raise ValueError("Invalid start_date/end_date; expected YYYY-MM-DD") from exc
    else:
        window_start = datetime.utcnow().date()
        window_end = window_start + relativedelta(months=months)

    if window_end < window_start:
        window_start, window_end = window_end, window_start

    exhibitions = await _fetch_temporary_exhibitions_window_async(
        city,
        window_start,
        window_end,
        target_min=1,
        target_max=target_max,
    )

    rows: list[dict] = []
    copy_sem = asyncio.Semaphore(
        max(1, int(getattr(settings, "TEMP_COPY_CONCURRENCY", 2) or 2))
    )
    translation_sem = asyncio.Semaphore(
        max(1, int(getattr(settings, "TEMP_TRANSLATION_CONCURRENCY", 4) or 4))
    )

    used_short_openers: dict[str, int] = {}
    used_long_prefixes: dict[str, int] = {}
    per_ex_openers: dict[int, tuple[str, str]] = {}
    for idx, ex in enumerate(exhibitions):
        venue = (ex.get("venue") or "").strip()
        seed = "|".join(
            [
                (ex.get("name") or "").strip(),
                venue,
                (ex.get("start_date") or "").strip(),
                (ex.get("end_date") or "").strip(),
                city,
            ]
        )
        required_short = _pick_required_short_opener(seed=seed, used=used_short_openers)
        required_long = _pick_required_long_prefix(
            venue=venue or "the venue", seed=seed, used=used_long_prefixes
        )
        per_ex_openers[idx] = (required_short, required_long)

    async def _process_exhibition(ex: dict, *, idx: int) -> dict:
        name = (ex.get("name") or "").strip()
        venue = (ex.get("venue") or "").strip()
        address = (ex.get("address") or "").strip()
        duration_raw = (ex.get("duration") or "").strip()
        ticket_url = (ex.get("ticket_url") or "").strip()
        source_url = (ex.get("source_url") or "").strip()
        start_date = (ex.get("start_date") or "").strip()
        end_date = (ex.get("end_date") or "").strip()
        information = (ex.get("information") or "").strip()
        is_free = ex.get("is_free")
        ex_city = (ex.get("city") or city).strip()
        country = (ex.get("country") or "").strip()
        latitude = _normalise_coord(ex.get("latitude"))
        longitude = _normalise_coord(ex.get("longitude"))
        repeat_pattern = (ex.get("repeat_pattern") or "").strip()
        open_days = (ex.get("open_days") or "").strip()
        opening_hours = _normalise_opening_hours(
            (ex.get("opening_hours") or "").strip()
        )

        venue = _maybe_prefix_the_venue(venue, country)

        # Best-effort venue image URL (cached). Store as a single URL in the existing column.
        image_url = await _get_venue_image_url_async(
            venue=venue, city=ex_city, country=country, source_url=source_url
        )

        title_for_copy = (
            name.split(":", 1)[0].strip()
            if name
            else ", ".join([p for p in [venue, ex_city] if p])
        )

        required_short_opener, required_long_prefix = per_ex_openers.get(
            idx,
            (
                "Trace",
                f"Rooms at {venue} map the show’s themes through objects, images, sound and text.",
            ),
        )

        async with copy_sem:
            copy = await _generate_temp_copy_async(
                title=title_for_copy,
                venue=venue,
                city=ex_city,
                country=country,
                address=address,
                start_date=start_date,
                end_date=end_date or start_date,
                duration=duration_raw or "1.5 hours",
                avoid_short_openers=None,
                avoid_long_openers=None,
                required_short_opener=required_short_opener,
                required_long_prefix=required_long_prefix,
            )

        short_desc = (copy.get("short") or "").strip()
        long_desc = _normalise_html_spacing(copy.get("long") or "")
        if not short_desc or not long_desc:
            print(
                "DEBUG scrape_temporary_exhibitions copy generation failed:",
                title_for_copy,
            )
            short_desc = (
                short_desc
                or "See key works and objects that map the show’s themes and context."
            )
            long_desc = long_desc or ""

        if isinstance(is_free, str):
            is_free_clean = is_free.strip()
        else:
            is_free_clean = str(is_free) if is_free is not None else ""
        free_flag = "0" if is_free_clean in ("0", "free", "FREE") else "1"

        pretty_range = _format_date_range_label(start_date, end_date)

        if not venue and name:
            head = name.split(":", 1)[0]
            parts = [p.strip() for p in head.split(",") if p.strip()]
            if len(parts) >= 3:
                venue = parts[-2]
                ex_city = parts[-1] or ex_city
        venue = _maybe_prefix_the_venue(venue, country)

        city_label = ex_city or city
        cats = f"Top Exhibitions in {city_label}".strip()
        name_head = (name.split(":", 1)[0] if name else "").strip()
        name_head = re.sub(r"\(.*?\)", "", name_head).strip()
        title_seed = name_head or venue or city_label
        parts = (
            [p.strip() for p in title_seed.split(",") if p.strip()]
            if title_seed
            else []
        )
        base_core = parts[0] if parts else title_seed
        base_segments = [base_core] if base_core else []
        if base_core and "exhibition" not in base_core.lower():
            base_segments[0] = f"{base_core}, Exhibition"

        def _seg_contains(val: str) -> bool:
            if not val:
                return False
            val_norm = _normalise_for_dedupe(val)
            for seg in base_segments:
                if val_norm and val_norm in _normalise_for_dedupe(seg):
                    return True
            return False

        if venue and not _seg_contains(venue):
            base_segments.append(venue)
        if city_label:
            city_norm = _normalise_city_name(city_label)
            # Do not repeat the city if it's already in any segment (e.g. venue name contains it).
            already_has_city = any(
                _contains_city(seg, city_label) for seg in base_segments
            )
            if city_norm and not already_has_city:
                base_segments.append(city_label)
        base_joined = ", ".join(base_segments) if base_segments else (city_label or "")
        title = f"{base_joined}: {pretty_range}" if pretty_range else base_joined

        venue_category_path = ""
        if country and ex_city and venue:
            venue_category_path = f"{country}, {ex_city}, {venue}"
        elif country and ex_city:
            venue_category_path = f"{country}, {ex_city}"

        duration_val = _normalise_duration_hours(duration_raw) or "1.5"

        row = {
            "Name of site, City": title,
            "City": ex_city,
            "Country": country,
            "Full address": address,
            "Type(s) of activity": "",
            "Divento Categories": cats,
            "Free activity?": free_flag,
            "Long description": long_desc,
            "Long description fr": "",
            "Long description es": "",
            "Long description it": "",
            "Long description ru": "",
            "Long description zh": "",
            "URL of images": "",
            "Legends of images": "",
            "Duration of visit": duration_val,
            "Opening and closing time": opening_hours,
            "Short description": short_desc,
            "Short description fr": "",
            "Short description es": "",
            "Short description it": "",
            "Short description ru": "",
            "Short description zh": "",
            "Meta description": short_desc or long_desc,
            "Meta description fr": "",
            "Meta description es": "",
            "Meta description it": "",
            "Meta description ru": "",
            "Meta description zh": "",
            "Latitude": latitude,
            "Information": information,
            "Ticket URL": ticket_url,
            "Longitude": longitude,
            "Activity type": "",
            "Rating": "4",
            "Name of site city": title,
            "Name of site city fr": "",
            "Name of site city es": "",
            "Name of site city it": "",
            "Name of site city ru": "",
            "Name of site city zh": "",
            "Real city": ex_city,
            "Start date (YYYY-MM-DD)": start_date,
            "End date (YYYY-MM-DD)": end_date or start_date,
            "Venue category path": venue_category_path,
            "Repeat pattern": repeat_pattern,
            "Open days": open_days,
        }
        row["URL of images"] = image_url or ""
        row["Legends of images"] = ""

        try:
            async with translation_sem:
                bundle = await _translate_bundle_async(
                    title_en=title,
                    short_en=short_desc,
                    long_en_html=long_desc,
                    languages=languages,
                )
        except Exception as exc:  # noqa: BLE001
            if _is_insufficient_quota_error(exc):
                raise
            logger.debug("temp_translation_error city=%s err=%r", city, exc)
            bundle = {lang: {"name": "", "short": "", "long": ""} for lang in languages}

        row["Name of site city fr"] = (
            bundle.get("fr", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city es"] = (
            bundle.get("es", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city it"] = (
            bundle.get("it", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city ru"] = (
            bundle.get("ru", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city zh"] = (
            bundle.get("zh-CN", {}).get("name") or row["Name of site, City"]
        )

        for lang in languages:
            code = lang.split("-")[0]
            row[f"Short description {code}"] = bundle.get(lang, {}).get("short") or ""
            row[f"Long description {code}"] = bundle.get(lang, {}).get("long") or ""
            meta_key = {
                "fr": "Meta description fr",
                "es": "Meta description es",
                "it": "Meta description it",
                "ru": "Meta description ru",
                "zh": "Meta description zh",
            }.get(code)
            if meta_key:
                row[meta_key] = (
                    row[f"Short description {code}"] or row[f"Long description {code}"]
                )

        return row

    tasks = [
        asyncio.create_task(_process_exhibition(ex, idx=i))
        for i, ex in enumerate(exhibitions)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for res in results:
        if isinstance(res, Exception):
            if _is_insufficient_quota_error(res):
                raise res
            logger.debug("temp_exhibition_task_error city=%s err=%r", city, res)
            continue
        if isinstance(res, dict) and res:
            rows.append(res)

    df = pd.DataFrame(rows)
    for col in TEMPORARY_COLUMNS_ORDER:
        if col not in df:
            df[col] = ""
    return df[TEMPORARY_COLUMNS_ORDER]


def scrape_destinations_temp(cities: list[str], months: int = 24) -> str:
    frames = [scrape_temporary_exhibitions(c, months=months) for c in cities]
    df = pd.concat(frames, ignore_index=True)
    Path(settings.RESULT_DIR).mkdir(parents=True, exist_ok=True)
    out_path = Path(settings.RESULT_DIR) / f"{uuid.uuid4()}_places.xlsx"

    combinations = _build_combinations_sheet(df)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Exhibitions", index=False)
        combinations.to_excel(writer, sheet_name="Combinations", index=False)
    return str(out_path)
