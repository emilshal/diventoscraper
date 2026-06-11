"""Licensed image sourcing for permanent attractions.

Implements Fiona's "Attraction Image Sourcing" spec (2026-06-09): for each
attraction fill FOUR image slots (hero / secondary / interior / detail) by
walking a fixed source-priority chain and stopping at the first image that
suits the role and carries a verified-reusable licence. Every stored image
keeps its licence and a ready-to-render attribution string.

Source priority chain (per slot):
    1   Official site (og:image + JSON-LD)  -> "official-unconfirmed",
        blocked until PERM_IMG_CONFIRM_OFFICIAL_TERMS is set
    2   Other web sources                    -> pluggable, empty by default
    3a  Wikimedia Commons                    -> per-file licence via extmetadata
    3b  Europeana (needs EUROPEANA_API_KEY)  -> reusability=open only
    3c  Museum open access (Met, Art Inst.)  -> CC0 public-domain subset
    3d  Gov/heritage bodies                  -> pluggable registry, empty
    3e  Unsplash (needs UNSPLASH_ACCESS_KEY) -> Unsplash Licence
    3f  OpenAI web-search                    -> rights resolved by domain,
        unverifiable rejected (or set aside if PERM_IMG_SURFACE_UNVERIFIED)
    4   OpenAI generation (gpt-image-1)      -> LAST RESORT, generic type
        photo only, never the named place; off by default

Licence gate (Divento is commercial and crops/resizes):
    accepted:  CC0, Public Domain, CC-BY, CC-BY-SA, Unsplash, Pexels,
               confirmed official media
    rejected:  -NC, -ND, unknown/unconfirmed

OpenAI is used in TWO distinct roles here (per spec): web SEARCH finds real
existing photos whose rights still need domain-resolution; GENERATION makes
a generic placeholder of the attraction TYPE and must never depict the real
place.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import random
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

import requests

from app.config import settings

logger = logging.getLogger(__name__)

_UA = "DiventoScraper/1.0 (https://divento.com; contact@divento.com)"
_TIMEOUT = 10.0


# ──────────────────────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class Attraction:
    name: str
    city: str = ""
    country: str = ""
    website: str = ""
    kind: str = ""              # museum / monument / church / park / ...
    name_local: str = ""
    commons_category: str = ""
    wikidata_id: str = ""
    # Pre-fetched candidate from the search phase (OpenAI web_search found
    # it already) — fed to the openai-search adapter at zero extra cost.
    prefetched_search_url: str = ""
    prefetched_search_page: str = ""


@dataclass
class ImageCandidate:
    url: str
    source: str                 # adapter id, e.g. "wikimedia_commons"
    license_id: str             # normalised, e.g. "cc-by-sa-4.0", "cc0"
    license_url: str = ""
    author: str = ""
    title: str = ""
    source_page: str = ""
    width: int = 0
    height: int = 0
    status: str = "ok"          # ok | blocked-official | unverified | ai-generated

    def credit_line(self) -> str:
        """Ready-to-render attribution, e.g.
        'John Smith, "West facade" / CC BY-SA 4.0 — via wikimedia_commons (https://...)'"""
        parts = []
        if self.author:
            parts.append(self.author)
        if self.title:
            parts.append(f'"{self.title}"')
        head = ", ".join(parts) if parts else "Unknown"
        licence = _license_display(self.license_id)
        page = f" ({self.source_page})" if self.source_page else ""
        return f"{head} / {licence} — via {self.source}{page}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "source": self.source,
            "license_id": self.license_id,
            "license_url": self.license_url,
            "author": self.author,
            "title": self.title,
            "source_page": self.source_page,
            "width": self.width,
            "height": self.height,
            "status": self.status,
            "credit": self.credit_line(),
        }


@dataclass
class AttractionImageSet:
    slots: dict[str, ImageCandidate | None] = field(default_factory=dict)
    # Candidates that looked right but had unverifiable rights — kept for
    # manual licensing review, never auto-published.
    unverified: list[ImageCandidate] = field(default_factory=list)

    @property
    def missing_roles(self) -> list[str]:
        return [r for r, c in self.slots.items() if c is None]

    @property
    def complete(self) -> bool:
        return not self.missing_roles

    def urls(self) -> list[str]:
        return [c.url for c in self.slots.values() if c is not None]

    def credits(self) -> list[str]:
        return [c.credit_line() for c in self.slots.values() if c is not None]

    def authors(self) -> list[str]:
        """Short author names, one per filled slot, parallel to urls().
        Commas are stripped from within each name because the legacy Excel
        importer splits the legends cell on commas (old Google files held
        exactly 'Name, Name, Name'). Falls back to the source label when a
        file has no author recorded."""
        out: list[str] = []
        for c in self.slots.values():
            if c is None:
                continue
            name = (c.author or "").replace(",", " ").strip()
            name = re.sub(r"\s+", " ", name)
            if not name:
                name = "Wikimedia Commons" if c.source == "wikimedia_commons" else c.source.replace("_", " ")
            # Keep names short like the old Google attributions.
            out.append(name[:60])
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "slots": {r: (c.to_dict() if c else None) for r, c in self.slots.items()},
            "missing_roles": self.missing_roles,
            "complete": self.complete,
            "unverified": [c.to_dict() for c in self.unverified],
        }


# The four slots, fixed order. (role, keyword chain after the name, orientation)
ROLES: list[tuple[str, list[str], str]] = [
    ("hero", ["", "exterior", "facade", "building"], "landscape"),
    ("secondary", ["aerial view", "panorama", "at night", "side view", "from above"], "landscape"),
    ("interior", ["interior", "main hall", "gallery", "collection", "nave", "inside"], "landscape"),
    # detail prefers landscape too (the Divento photo box renders portrait
    # badly until the redesign) — fill_image_set retries it with "any"
    # orientation when no landscape close-up exists.
    ("detail", ["detail", "architectural detail", "ornament", "sculpture", "relief", "close-up"], "landscape"),
]


# ──────────────────────────────────────────────────────────────────────────────
# Licence gate
# ──────────────────────────────────────────────────────────────────────────────

_LICENSE_DISPLAY = {
    "cc0": "CC0",
    "public-domain": "Public Domain",
    "unsplash": "Unsplash Licence",
    "pexels": "Pexels Licence",
    "official-confirmed": "Official media",
    "official-unconfirmed": "Official media (terms unconfirmed)",
    "ai-generated": "AI-generated illustration",
}


def _license_display(license_id: str) -> str:
    if license_id in _LICENSE_DISPLAY:
        return _LICENSE_DISPLAY[license_id]
    # cc-by-sa-4.0 -> CC BY-SA 4.0
    if license_id.startswith("cc-"):
        return license_id.upper().replace("CC-", "CC ").replace("-", "-", 1).replace("BY-SA", "BY-SA")
    return license_id


def normalise_license(raw: str) -> str:
    """Map a raw licence string (Commons LicenseShortName, Europeana rights
    URL, etc.) to a normalised id the gate understands. Unknown -> 'unknown'."""
    s = (raw or "").strip().lower()
    if not s:
        return "unknown"
    if "creativecommons.org/publicdomain/zero" in s or s in ("cc0", "cc0 1.0", "cc-zero"):
        return "cc0"
    if "publicdomain/mark" in s or "public domain" in s or s in ("pd", "pdm", "pd-old", "pd-self", "pd-us"):
        return "public-domain"
    # CC licence URL form (what Europeana's `rights` field returns):
    # http://creativecommons.org/licenses/by-sa/3.0/
    m_url = re.search(r"creativecommons\.org/licenses/(by(?:-[a-z]+)*)/(\d\.\d)", s)
    if m_url:
        code, ver = m_url.group(1), m_url.group(2)
        if "nc" in code or "nd" in code:
            return "rejected-nc-nd"
        base = "cc-by-sa" if "sa" in code else "cc-by"
        return f"{base}-{ver}"
    # CC families — keep version when present. Reject NC/ND here already.
    m = re.search(r"cc[ -]?by(?P<mods>(?:[ -](?:sa|nc|nd))*)[ -]?(?P<ver>\d\.\d)?", s)
    if m:
        mods = m.group("mods") or ""
        if "nc" in mods or "nd" in mods:
            return "rejected-nc-nd"
        ver = m.group("ver") or ""
        base = "cc-by-sa" if "sa" in mods else "cc-by"
        return f"{base}-{ver}" if ver else base
    if "unsplash" in s:
        return "unsplash"
    if "pexels" in s:
        return "pexels"
    if s.startswith("official"):
        return s
    if s == "ai-generated":
        return s
    return "unknown"


def license_accepted(license_id: str) -> bool:
    """The safety gate. Strict: commercial use + derivatives must be OK."""
    if license_id in ("cc0", "public-domain", "unsplash", "pexels", "official-confirmed"):
        return True
    if license_id.startswith("cc-by-sa") or license_id.startswith("cc-by"):
        # normalise_license already filtered NC/ND out of cc-by* ids.
        return True
    return False


# Domain resolver for OpenAI-search hits: known-open hosts only. This is a
# heuristic (per spec) — structured APIs stay the gold standard.
_KNOWN_OPEN_DOMAINS: dict[str, str] = {
    "upload.wikimedia.org": "cc-by-sa",     # per-file licence varies; Commons adapter preferred
    "images.metmuseum.org": "cc0",
    "www.artic.edu": "cc0",
    "artic.edu": "cc0",
    "ids.si.edu": "cc0",
    "images.unsplash.com": "unsplash",
    "unsplash.com": "unsplash",
    "images.pexels.com": "pexels",
    "www.pexels.com": "pexels",
}


def resolve_domain_license(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return "unknown"
    for domain, lic in _KNOWN_OPEN_DOMAINS.items():
        if host == domain or host.endswith("." + domain):
            return lic
    return "unknown"


# ──────────────────────────────────────────────────────────────────────────────
# Small shared helpers
# ──────────────────────────────────────────────────────────────────────────────


def _http_get_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    try:
        r = requests.get(
            url, params=params,
            headers={"User-Agent": _UA, "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def _head_ok(url: str) -> bool:
    """HEAD-verify with one retry; same throttle-tolerant logic as the
    permanent scraper's checker (kept local to avoid an import cycle)."""
    headers = {"User-Agent": _UA}

    def _attempt() -> bool | None:
        try:
            r = requests.head(url, timeout=8, allow_redirects=True, headers=headers)
            if r.status_code in (405, 403):
                r = requests.get(url, timeout=8, allow_redirects=True, headers=headers, stream=True)
                r.close()
            if r.status_code == 429 or (400 <= r.status_code < 500 and r.status_code != 404):
                return None
            if 500 <= r.status_code < 600:
                return None
            if r.status_code >= 400:
                return False
            ctype = (r.headers.get("Content-Type") or "").lower()
            if ctype and not ctype.startswith("image/"):
                return False
            return True
        except requests.exceptions.RequestException:
            return None

    for i in range(2):
        out = _attempt()
        if out is not None:
            return out
        time.sleep(0.5 * (i + 1) + random.random() * 0.5)
    return False


def _orientation_ok(width: int, height: int, want: str) -> bool:
    if want == "any" or not width or not height:
        return True
    if want == "landscape":
        return width >= height
    return True


def _size_ok(width: int, height: int) -> bool:
    """Minimum-dimension gate. The Divento photo box renders small images
    badly (no site-side cropping until the redesign), so reject anything
    under the configured floor. Unknown dimensions (0) pass — only the
    structured sources report size."""
    if not width or not height:
        return True
    return width >= settings.PERM_IMG_MIN_WIDTH and height >= settings.PERM_IMG_MIN_HEIGHT


def _strip_html(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s or "")).strip()


def _clean_commons_title(s: str) -> str:
    """Commons ObjectName sometimes embeds wikidata QuickStatements markup,
    e.g. 'Italian: Trittico title QS:P1476,it:"Trittico" label QS:Lit,...'.
    Cut at the first QS fragment and drop a leading language prefix."""
    s = re.split(r"\s+(?:title|label)\s+QS:", s or "", 1)[0]
    s = re.sub(r"^[A-Z][a-z]+:\s+", "", s)  # "Italian: " etc.
    return s.strip()


# ──────────────────────────────────────────────────────────────────────────────
# Source adapters
# Each: async (attraction, role, keywords, orientation, seen_urls) -> candidate
# A dead source returns None and the chain moves on — never crashes the scrape.
# ──────────────────────────────────────────────────────────────────────────────


async def _src_official_site(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    """og:image + JSON-LD image from the attraction's own website."""
    if not a.website:
        return None

    def _fetch() -> ImageCandidate | None:
        try:
            r = requests.get(a.website, headers={"User-Agent": _UA}, timeout=_TIMEOUT)
            if r.status_code != 200:
                return None
            html = r.text
        except Exception:
            return None
        urls: list[str] = []
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html)
        if m:
            urls.append(m.group(1))
        for ld in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.DOTALL):
            try:
                data = json.loads(ld)
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for item in items:
                img = item.get("image") if isinstance(item, dict) else None
                if isinstance(img, str):
                    urls.append(img)
                elif isinstance(img, list):
                    urls.extend(u for u in img if isinstance(u, str))
                elif isinstance(img, dict) and img.get("url"):
                    urls.append(img["url"])
        for u in urls:
            if u and u.startswith("http") and u not in seen:
                status = (
                    "ok" if settings.PERM_IMG_CONFIRM_OFFICIAL_TERMS else "blocked-official"
                )
                lic = (
                    "official-confirmed"
                    if settings.PERM_IMG_CONFIRM_OFFICIAL_TERMS
                    else "official-unconfirmed"
                )
                return ImageCandidate(
                    url=u, source="official_site", license_id=lic,
                    source_page=a.website, title=a.name, status=status,
                )
        return None

    return await asyncio.to_thread(_fetch)


# Pluggable: listing/aggregator pages with per-domain known licences.
# Empty by default — register (page_url, license_id) pairs as needed.
OTHER_WEB_SOURCES: list[tuple[str, str]] = []


async def _src_other_web(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    if not OTHER_WEB_SOURCES:
        return None
    # Reference behaviour: og:image of each registered page, licence as registered.
    for page_url, lic in OTHER_WEB_SOURCES:
        def _fetch() -> str | None:
            try:
                r = requests.get(page_url, headers={"User-Agent": _UA}, timeout=_TIMEOUT)
                m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', r.text)
                return m.group(1) if m else None
            except Exception:
                return None
        u = await asyncio.to_thread(_fetch)
        if u and u not in seen:
            return ImageCandidate(url=u, source="other_web", license_id=normalise_license(lic), source_page=page_url)
    return None


_COMMONS_API = "https://commons.wikimedia.org/w/api.php"


def _commons_candidate_from_page(page: dict[str, Any]) -> ImageCandidate | None:
    infos = page.get("imageinfo") or []
    if not infos:
        return None
    info = infos[0]
    meta = info.get("extmetadata") or {}

    def emv(key: str) -> str:
        v = meta.get(key) or {}
        return _strip_html(str(v.get("value") or ""))

    lic = normalise_license(emv("LicenseShortName") or emv("License"))
    url = info.get("thumburl") or info.get("url") or ""
    if not url:
        return None
    return ImageCandidate(
        url=url,
        source="wikimedia_commons",
        license_id=lic,
        license_url=emv("LicenseUrl"),
        author=emv("Artist"),
        title=_clean_commons_title(emv("ObjectName")) or (page.get("title") or "").removeprefix("File:"),
        source_page=info.get("descriptionurl") or "",
        width=int(info.get("thumbwidth") or info.get("width") or 0),
        height=int(info.get("thumbheight") or info.get("height") or 0),
    )


def _commons_relevance_text(page: dict[str, Any]) -> str:
    """Title + description + categories of a Commons file, lowercased — used
    for the wrong-place guard."""
    parts = [page.get("title") or ""]
    infos = page.get("imageinfo") or []
    if infos:
        meta = infos[0].get("extmetadata") or {}
        for key in ("ImageDescription", "Categories", "ObjectName"):
            v = meta.get(key) or {}
            parts.append(_strip_html(str(v.get("value") or "")))
    return " ".join(parts).lower()


async def _src_wikimedia_commons(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    """Workhorse. Per-file licence + author via imageinfo extmetadata —
    the gold standard. Category lookup first (if known), then filetext
    search with each keyword, English name then local name.

    Wrong-place guard: venue names are often shared globally ("Basilica of
    Saint Nicholas" exists in Bari, Amsterdam, Trnava, ...). Tier 1 searches
    WITH the city in the query (Commons indexes file description pages, which
    mention the location). Tier 2 retries without the city but only accepts
    files whose title/description/categories actually mention the city."""

    def _query(params: dict[str, Any]) -> list[dict[str, Any]]:
        data = _http_get_json(_COMMONS_API, params)
        if not data:
            return []
        pages = (data.get("query") or {}).get("pages") or {}
        return list(pages.values())

    iiprops = {
        "prop": "imageinfo",
        "iiprop": "url|extmetadata|size",
        "iiurlwidth": settings.PERM_IMG_THUMB_WIDTH,
        "format": "json",
    }

    city_l = (a.city or "").strip().lower()

    def _walk(pages: list[dict[str, Any]], *, require_city: bool) -> ImageCandidate | None:
        # Two passes: first prefer files whose URL needs no percent-encoding.
        # The legacy importer was built against Google URLs (plain base64ish
        # tokens, no '%' anywhere); Wikimedia URLs with %2C (encoded comma)
        # can shred a comma-split importer after URL-decoding.
        for clean_only in (True, False):
            for p in pages:
                title = (p.get("title") or "").lower()
                if title.endswith((".svg", ".gif", ".tif", ".tiff", ".pdf", ".ogg", ".webm")):
                    continue
                if require_city and city_l and city_l not in _commons_relevance_text(p):
                    continue
                cand = _commons_candidate_from_page(p)
                if cand is None or cand.url in seen:
                    continue
                if clean_only and "%" in cand.url:
                    continue
                if not license_accepted(cand.license_id):
                    continue
                if not _orientation_ok(cand.width, cand.height, orientation):
                    continue
                if not _size_ok(cand.width, cand.height):
                    continue
                return cand
        return None

    def _search(term: str) -> list[dict[str, Any]]:
        return _query({
            "action": "query",
            "generator": "search",
            "gsrsearch": f"filetype:bitmap {term}",
            "gsrnamespace": 6,
            "gsrlimit": 10,
            **iiprops,
        })

    # 1. Category members, if a category is known (already venue-scoped, no
    # city guard needed).
    if a.commons_category:
        pages = await asyncio.to_thread(_query, {
            "action": "query",
            "generator": "categorymembers",
            "gcmtitle": f"Category:{a.commons_category}",
            "gcmtype": "file",
            "gcmlimit": 25,
            **iiprops,
        })
        # Keyword filter inside the category for non-hero roles.
        kw_terms = [k for k in keywords if k]
        if kw_terms:
            filtered = [p for p in pages if any(k in (p.get("title") or "").lower() for k in kw_terms)]
            cand = _walk(filtered, require_city=False)
        else:
            cand = _walk(pages, require_city=False)
        if cand:
            return cand

    names = [a.name] + ([a.name_local] if a.name_local and a.name_local != a.name else [])

    # 2. Tier 1: filetext search WITH the city in the query.
    for kw in keywords:
        for nm in names:
            city_term = "" if (city_l and city_l in nm.lower()) else a.city
            term = " ".join(t for t in (nm, city_term, kw) if t).strip()
            pages = await asyncio.to_thread(_search, term)
            cand = _walk(pages, require_city=False)
            if cand:
                return cand

    # 3. Tier 2: name-only search, but require the city to appear in the
    # file's title/description/categories before accepting.
    for kw in keywords:
        for nm in names:
            term = f"{nm} {kw}".strip()
            pages = await asyncio.to_thread(_search, term)
            cand = _walk(pages, require_city=True)
            if cand:
                return cand
    return None


async def _src_europeana(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    key = settings.EUROPEANA_API_KEY
    if not key:
        return None

    def _search(term: str) -> ImageCandidate | None:
        data = _http_get_json("https://api.europeana.eu/record/v2/search.json", {
            "wskey": key,
            "query": term,
            "reusability": "open",
            "media": "true",
            "qf": "TYPE:IMAGE",
            "rows": 10,
        })
        for item in (data or {}).get("items") or []:
            url = (item.get("edmIsShownBy") or [None])[0]
            if not url or url in seen:
                continue
            lic = normalise_license((item.get("rights") or [""])[0])
            if not license_accepted(lic):
                continue
            return ImageCandidate(
                url=url, source="europeana", license_id=lic,
                license_url=(item.get("rights") or [""])[0],
                author=", ".join(item.get("dcCreator") or []),
                title=", ".join(item.get("title") or []),
                source_page=item.get("guid") or "",
            )
        return None

    for kw in keywords:
        cand = await asyncio.to_thread(_search, f"{a.name} {kw}".strip())
        if cand:
            return cand
    return None


async def _src_museum_open_access(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    """Met + Art Institute open-access. Best for interior/collection shots
    of museums; skipped for other roles/kinds."""
    if role not in ("interior", "detail"):
        return None
    if a.kind and a.kind.lower() not in ("museum", "gallery"):
        return None

    def _met(term: str) -> ImageCandidate | None:
        data = _http_get_json(
            "https://collectionapi.metmuseum.org/public/collection/v1/search",
            {"q": term, "hasImages": "true"},
        )
        for oid in ((data or {}).get("objectIDs") or [])[:5]:
            obj = _http_get_json(
                f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{oid}"
            )
            if not obj or not obj.get("isPublicDomain"):
                continue
            url = obj.get("primaryImage") or ""
            if not url or url in seen:
                continue
            return ImageCandidate(
                url=url, source="met_open_access", license_id="cc0",
                author=obj.get("artistDisplayName") or "",
                title=obj.get("title") or "",
                source_page=obj.get("objectURL") or "",
            )
        return None

    def _artic(term: str) -> ImageCandidate | None:
        data = _http_get_json("https://api.artic.edu/api/v1/artworks/search", {
            "q": term,
            "fields": "id,title,image_id,is_public_domain,artist_display",
            "limit": 5,
        })
        for item in (data or {}).get("data") or []:
            if not item.get("is_public_domain") or not item.get("image_id"):
                continue
            url = f"https://www.artic.edu/iiif/2/{item['image_id']}/full/1686,/0/default.jpg"
            if url in seen:
                continue
            return ImageCandidate(
                url=url, source="artic_open_access", license_id="cc0",
                author=_strip_html(item.get("artist_display") or ""),
                title=item.get("title") or "",
                source_page=f"https://www.artic.edu/artworks/{item.get('id')}",
            )
        return None

    # These APIs index artworks, not buildings — query by attraction name so
    # we only get works actually associated with the place.
    cand = await asyncio.to_thread(_met, a.name)
    if cand:
        return cand
    return await asyncio.to_thread(_artic, a.name)


# Pluggable per-country gov/heritage media libraries. Register adapters as
# (country_lower, async adapter) pairs.
GOV_HERITAGE_SOURCES: list[tuple[str, Callable[..., Awaitable[ImageCandidate | None]]]] = []


async def _src_gov_heritage(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    for country, adapter in GOV_HERITAGE_SOURCES:
        if country == (a.country or "").lower():
            try:
                cand = await adapter(a, role, keywords, orientation, seen)
            except Exception:
                cand = None
            if cand:
                return cand
    return None


# Unsplash download-endpoint ping: the Unsplash API guidelines ask that the
# photo's download_location be hit when the image is actually USED (i.e. when
# Divento publishes the page) — that's a site-side concern. The location is
# stored on the candidate via source_page metadata; call this from the
# publishing pipeline.
def unsplash_track_download(download_location: str) -> None:
    key = settings.UNSPLASH_ACCESS_KEY
    if not (key and download_location):
        return
    try:
        requests.get(
            download_location,
            headers={"Authorization": f"Client-ID {key}", "User-Agent": _UA},
            timeout=_TIMEOUT,
        )
    except Exception:
        pass


_UNSPLASH_DOWNLOAD_LOCATIONS: dict[str, str] = {}  # image url -> download_location


async def _src_unsplash(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    key = settings.UNSPLASH_ACCESS_KEY
    if not key:
        return None
    name_tokens = {t for t in re.findall(r"[a-z]+", a.name.lower()) if len(t) >= 4}

    def _search(term: str) -> ImageCandidate | None:
        try:
            r = requests.get("https://api.unsplash.com/search/photos", params={
                "query": term,
                "per_page": 10,
                **({"orientation": "landscape"} if orientation == "landscape" else {}),
            }, headers={"Authorization": f"Client-ID {key}", "User-Agent": _UA}, timeout=_TIMEOUT)
            if r.status_code != 200:
                return None
            data = r.json()
        except Exception:
            return None
        for photo in data.get("results") or []:
            url = (photo.get("urls") or {}).get("regular") or ""
            if not url or url in seen:
                continue
            # Relevance guard: Unsplash ranks loosely — require a venue-name
            # token in the description/alt text to avoid wrong-place images.
            text = f"{photo.get('description') or ''} {photo.get('alt_description') or ''}".lower()
            if name_tokens and not any(t in text for t in name_tokens):
                continue
            if not _size_ok(int(photo.get("width") or 0), int(photo.get("height") or 0)):
                continue
            user = photo.get("user") or {}
            dl = (photo.get("links") or {}).get("download_location") or ""
            if dl:
                _UNSPLASH_DOWNLOAD_LOCATIONS[url] = dl
            return ImageCandidate(
                url=url, source="unsplash", license_id="unsplash",
                license_url="https://unsplash.com/license",
                author=user.get("name") or "",
                title=photo.get("description") or photo.get("alt_description") or "",
                source_page=(photo.get("links") or {}).get("html") or "",
                width=int(photo.get("width") or 0),
                height=int(photo.get("height") or 0),
            )
        return None

    for kw in keywords:
        cand = await asyncio.to_thread(_search, f"{a.name} {kw}".strip())
        if cand:
            return cand
    return None


# search_fn hook: plug an existing OpenAI web-search call in. Must return
# {"url": ..., "source_page": ..., "title": ...} or None. The permanent
# scraper wires its own client-backed implementation at runtime.
SearchFn = Callable[[str], Awaitable[dict[str, str] | None]]
_OPENAI_SEARCH_FN: SearchFn | None = None


def set_openai_search_fn(fn: SearchFn | None) -> None:
    global _OPENAI_SEARCH_FN
    _OPENAI_SEARCH_FN = fn


async def _src_openai_search(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str],
    unverified_sink: list[ImageCandidate] | None = None,
) -> ImageCandidate | None:
    if not settings.PERM_IMG_ALLOW_OPENAI_SEARCH:
        return None

    def _resolve(url: str, page: str, title: str) -> ImageCandidate | None:
        if not url or url in seen:
            return None
        lic = resolve_domain_license(url)
        cand = ImageCandidate(
            url=url, source="openai_search", license_id=lic,
            title=title, source_page=page,
        )
        if lic == "unknown":
            cand.status = "unverified"
            if settings.PERM_IMG_SURFACE_UNVERIFIED and unverified_sink is not None:
                unverified_sink.append(cand)
            return None  # never auto-published
        if not license_accepted(lic):
            return None
        return cand

    # Zero-cost first: the search phase may already have found a photo.
    if role == "hero" and a.prefetched_search_url:
        cand = _resolve(a.prefetched_search_url, a.prefetched_search_page, a.name)
        if cand:
            return cand

    if _OPENAI_SEARCH_FN is None:
        return None
    for kw in keywords:
        try:
            hit = await _OPENAI_SEARCH_FN(f"{a.name} {kw}".strip() + (f", {a.city}" if a.city else ""))
        except Exception:
            hit = None
        if not hit:
            continue
        cand = _resolve(hit.get("url", ""), hit.get("source_page", ""), hit.get("title", ""))
        if cand:
            return cand
    return None


_KIND_GENERIC = {
    "museum": "a museum",
    "gallery": "an art gallery",
    "monument": "a historic monument",
    "church": "a historic church",
    "cathedral": "a cathedral",
    "palace": "a historic palace",
    "castle": "a castle",
    "park": "a city park",
    "garden": "a formal garden",
}

_ROLE_FRAMING = {
    "hero": "wide exterior framing",
    "secondary": "alternative exterior framing",
    "interior": "interior framing showing a gallery space",
    "detail": "close-up framing of an architectural detail",
}


async def _src_openai_generation(
    a: Attraction, role: str, keywords: list[str], orientation: str, seen: set[str]
) -> ImageCandidate | None:
    """LAST RESORT. Generates a GENERIC photo of the attraction TYPE — the
    model is never asked for the named landmark (it would produce a
    plausible-but-wrong building). Result must be labelled as an AI
    illustration in the UI."""
    if not settings.PERM_IMG_ALLOW_OPENAI_GENERATION:
        return None
    client = _get_sync_openai_client()
    if client is None:
        return None
    kind_phrase = _KIND_GENERIC.get((a.kind or "").lower(), "a tourist attraction")
    prompt = (
        f"A high-quality, realistic photograph of {kind_phrase}. "
        f"Natural daylight, eye-level {_ROLE_FRAMING.get(role, 'framing')}, "
        "no people, no text or watermarks, no recognizable real-world or "
        "branded landmarks. Editorial travel photography style."
    )

    def _generate() -> str | None:
        try:
            resp = client.images.generate(
                model="gpt-image-1",
                prompt=prompt,
                size="1536x1024" if orientation == "landscape" else "1024x1024",
                n=1,
            )
            b64 = resp.data[0].b64_json
            out_dir = Path(settings.RESULT_DIR) / "generated_images"
            out_dir.mkdir(parents=True, exist_ok=True)
            safe = re.sub(r"[^\w]+", "_", a.name.lower())[:40]
            out_path = out_dir / f"{safe}_{role}.png"
            out_path.write_bytes(base64.b64decode(b64))
            return str(out_path)
        except Exception as exc:
            logger.warning("img.generate.error name=%s err=%r", a.name, exc)
            return None

    path = await asyncio.to_thread(_generate)
    if not path:
        return None
    return ImageCandidate(
        url=path, source="openai_generation", license_id="ai-generated",
        title=f"AI illustration ({kind_phrase})", status="ai-generated",
    )


def _get_sync_openai_client():
    try:
        from openai import OpenAI
        if not settings.OPENAI_API_KEY:
            return None
        return OpenAI(api_key=settings.OPENAI_API_KEY)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

# Chain order per spec §3.
_SOURCE_CHAIN = [
    ("official_site", _src_official_site),
    ("other_web", _src_other_web),
    ("wikimedia_commons", _src_wikimedia_commons),
    ("europeana", _src_europeana),
    ("museum_open_access", _src_museum_open_access),
    ("gov_heritage", _src_gov_heritage),
    ("unsplash", _src_unsplash),
    ("openai_search", _src_openai_search),
    ("openai_generation", _src_openai_generation),
]


async def fill_image_set(attraction: Attraction) -> AttractionImageSet:
    """Fill the four slots in fixed role order. Per slot, walk the source
    chain and take the first licence-clean, role-suitable image; dedup by
    URL across slots; HEAD-verify before accepting. Resilient: a dead
    source contributes nothing and the chain moves on."""
    image_set = AttractionImageSet(slots={r: None for r, _, _ in ROLES})
    seen: set[str] = set()

    async def _fill_role(role: str, keywords: list[str], orientation: str) -> bool:
        for source_id, adapter in _SOURCE_CHAIN:
            try:
                if adapter is _src_openai_search:
                    cand = await adapter(
                        attraction, role, keywords, orientation, seen,
                        unverified_sink=image_set.unverified,
                    )
                else:
                    cand = await adapter(attraction, role, keywords, orientation, seen)
            except Exception as exc:
                logger.debug("img.source.error source=%s name=%s err=%r", source_id, attraction.name, exc)
                cand = None
            if cand is None:
                continue
            # Official-site images are surfaced but blocked until terms confirmed.
            if cand.status == "blocked-official":
                if not any(u.url == cand.url for u in image_set.unverified):
                    image_set.unverified.append(cand)
                continue
            if cand.status != "ai-generated":
                if not license_accepted(cand.license_id):
                    continue
                ok = await asyncio.to_thread(_head_ok, cand.url)
                if not ok:
                    logger.debug("img.head_dead source=%s url=%s", source_id, cand.url[:80])
                    continue
            image_set.slots[role] = cand
            seen.add(cand.url)
            logger.info(
                "img.slot_filled name=%s role=%s source=%s licence=%s",
                attraction.name, role, source_id, cand.license_id,
            )
            return True
        return False

    for role, keywords, orientation in ROLES:
        await _fill_role(role, keywords, orientation)

    # Detail fallback: a landscape close-up often doesn't exist on Commons;
    # allow portrait/square rather than leaving the slot empty.
    if image_set.slots.get("detail") is None:
        detail_keywords = next(kws for r, kws, _ in ROLES if r == "detail")
        await _fill_role("detail", detail_keywords, "any")

    if image_set.missing_roles:
        logger.info(
            "img.set_incomplete name=%s missing=%s",
            attraction.name, ",".join(image_set.missing_roles),
        )
    return image_set
