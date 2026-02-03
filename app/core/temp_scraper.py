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
from urllib.parse import urljoin

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
_TEMP_VENUE_COORD_CACHE: dict[str, tuple[str, str, str]] = {}
_TEMP_VENUE_DISCOVERY_CACHE: dict[str, list[dict[str, str]]] = {}
_TEMP_VENUE_HOURS_CACHE: dict[str, dict[str, str]] = {}

_VENUE_OPENING_INFO_FALLBACK = (
    getattr(settings, "TEMP_VENUE_HOURS_FALLBACK_VALUE", "See venue website")
    or "See venue website"
)

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

_CITY_CURATED_VENUES: dict[str, list[str]] = {
    # User-provided curated venue list (Paris, FR).
    "paris": [
        "Grand Palais",
        "Musée de la Vie romantique",
        "Jacquemart-André",
        "Musée Cernuschi",
        "Musée Bourdelle",
        "Musée de Cluny – Musée national du Moyen Âge",
        "Musée des Arts Décoratifs",
        "Musée de l’Armée (Les Invalides)",
        "Maison Européenne de la Photographie",
        "Bourdelle Museum",
        "Jeu de Paume",
        "Institut du Monde Arabe",
        "Louvre",
        "Musée d’Orsay",
        "Musée Carnavalet",
        "Centre Pompidou",
        "Musée de l’Homme",
        "L'Orangerie",
        "Musée d’Art Moderne de Paris",
        "Musée de l’Orangerie",
        "Musée de la Chasse et de la Nature",
        "Monnaie de Paris",
        "Fondation Louis Vuitton",
        "Cité des Sciences et de l’Industrie",
        "Musée Cognacq-Jay",
        "Musée des Arts et Métiers",
        "Musée du Luxembourg",
        "Musée du Quai Branly – Jacques Chirac",
        "Musée Guimet",
        "Musée Jacquemart-André",
        "Musée Marmottan Monet",
        "Musée national de la Marine",
        "Musée Nissim de Camondo",
        "Musée Picasso Paris",
        "Musée Rodin",
        "Muséum national d’Histoire naturelle",
        "Palais de Tokyo",
        "Petit Palais",
        "Zadkine Museum",
    ],
    # User-provided curated venue list (London, UK).
    "london": [
        "National Army Museum",
        "Tate Modern",
        "Courtauld Gallery",
        "National Portrait Gallery",
        "Wallace Collection",
        "Whitechapel Gallery",
        "Sir John Soane’s Museum",
        "Victoria and Albert Museum",
        "Leighton House",
        "British Museum",
        "National Gallery",
        "Serpentine Galleries",
        "Imperial War Museum",
        "Design Museum",
        "Royal Botanic Gardens, Kew",
        "Wellcome Collection",
        "Dulwich Picture Gallery",
        "Tate Britain",
        "Science Museum",
        "Horniman Museum and Gardens",
        "Royal Academy of Arts",
        "Queen’s House, Greenwich",
        "Photographers’ Gallery",
        "London Transport Museum",
        "London Museum (formerly Museum of London)",
        "Royal Observatory Greenwich",
        "Natural History Museum",
        "National Maritime Museum, Greenwich",
    ],
    # User-provided curated venue list (Barcelona, ES).
    "barcelona": [
        "Fundació Antoni Tàpies",
        "Museu d’Història de Barcelona (MUHBA)",
        "Museu Europeu d’Art Modern (MEAM)",
        "Fundació Joan Miró",
        "Museu Nacional d’Art de Catalunya (MNAC)",
        "Disseny Hub Barcelona (DHub)",
        "Museu Marítim de Barcelona",
        "Museu Egipci de Barcelona",
        "Museu del Monestir de Pedralbes",
        "CosmoCaixa",
        "Museu de les Cultures del Món",
        "CaixaForum Barcelona",
        "Museu Olímpic i de l’Esport Joan Antoni Samaranch",
        "Museu Picasso",
        "Museu Frederic Marès",
        "Museu del Modernisme Català",
        "Museu de la Xocolata",
        "MACBA – Museu d’Art Contemporani de Barcelona",
        "CCCB – Centre de Cultura Contemporània de Barcelona",
        "Museu de la Música",
        "Palau Martorell",
        "KBr Fundación MAPFRE",
        "Fabra i Coats, Centre d’Art Contemporani",
        "Casa Milà La Pedrera – Fundació Catalunya La Pedrera",
        "Museu Diocesà de Barcelona",
        "Museu de Ciències Naturals de Barcelona",
        "Palau Robert",
    ],
    # User-provided curated venue list (Madrid, ES).
    "madrid": [
        "Museo Casa de Cervantes (Alcalá de Henares)",
        "Museo Cerralbo",
        "Museo del Traje",
        "Museo Arqueológico Nacional",
        "Museo del Ferrocarril de Madrid",
        "Museo del Prado",
        "Museo Casa de Lope de Vega",
        "Museo de Historia de Madrid",
        "Museo del Ejército",
        "Museo del Romanticismo",
        "Museo de América",
        "Museo de Arte Contemporáneo de Madrid",
        "Museo Geominero",
        "Museo Lázaro Galdiano",
        "Museo Nacional Centro de Arte Reina Sofía",
        "Museo Nacional de Antropología",
        "Museo Nacional de Artes Decorativas",
        "Museo Naval",
        "Museo Sorolla",
        "Museo Thyssen-Bornemisza",
        "Real Academia de Bellas Artes de San Fernando",
        "Matadero",
        "CaixaForum Madrid",
        "Sala Canal de Isabel II",
    ],
    # User-provided curated venue list (Rome, IT).
    "rome": [
        "MAXXI – Museo nazionale delle arti del XXI secolo",
        "Museo dell’Ara Pacis",
        "Museo Nazionale di Castel Sant’Angelo",
        "Palazzo Spada",
        "Museo delle Civiltà",
        "Vatican Museums",
        "Museo di Palazzo Venezia",
        "Palazzo Altemps",
        "Palazzo Barberini",
        "Musei Vaticani – Pinacoteca",
        "Galleria Nazionale d’Arte Moderna e Contemporanea",
        "Museo di San Clemente",
        "Centrale Montemartini",
        "Galleria Corsini",
        "Musei Capitolini",
        "Museo Nazionale degli Strumenti Musicali",
        "Museo Nazionale Etrusco di Villa Giulia",
        "Museo di Roma in Trastevere",
        "Museo Napoleonico",
        "Museo delle Mura",
        "Palazzo Massimo alle Terme",
        "Museo Nazionale Romano",
        "Galleria Borghese",
        "Museo di Roma",
    ],
    # User-provided curated venue list (Florence, IT).
    "florence": [
        "Museo Nazionale del Bargello",
        "Museo dell’Opera del Duomo",
        "Galleria dell’Accademia",
        "Museo di Santa Maria Novella",
        "Museo degli Innocenti",
        "Museo Gucci",
        "Museo Galileo",
        "Galleria degli Uffizi",
        "Museo di San Marco",
        "Museo di Palazzo Davanzati",
        "Museo Horne",
        "Museo Stibbert",
        "Cappelle Medicee",
        "Museo Archeologico Nazionale di Firenze",
        "Museo Salvatore Ferragamo",
        "Casa Buonarroti",
        "Museo di Palazzo Vecchio",
        "Museo Stefano Bardini",
        "Boboli Gardens",
        "Palazzo Pitti",
    ],
    # User-provided curated venue list (Amsterdam, NL).
    "amsterdam": [
        "Hermitage Amsterdam",
        "Foam – Fotografiemuseum Amsterdam",
        "Rijksmuseum",
        "Amsterdam Museum",
        "H’ART Museum",
        "Willet-Holthuysen Museum",
        "Cobra Museum of Modern Art",
        "Rembrandt House Museum",
        "National Maritime Museum (Het Scheepvaartmuseum)",
        "Tropenmuseum",
        "Museum Van Loon",
        "Moco Museum",
        "Mauritshuis (The Hague)",
        "Museum of Bags and Purses (Tassenmuseum)",
        "Jewish Museum",
        "Eye Filmmuseum",
        "Stedelijk Museum",
        "Van Gogh Museum",
        "Our Lord in the Attic Museum (Ons’ Lieve Heer op Solder)",
        "Stadsarchief Amsterdam",
    ],
    # User-provided curated venue list (Berlin, DE).
    "berlin": [
        "Hamburger Bahnhof – Museum für Gegenwart",
        "Museum für Naturkunde",
        "Museum für Fotografie",
        "Museum für Asiatische Kunst (Humboldt Forum)",
        "Pergamonmuseum",
        "Technikmuseum",
        "Kunstgewerbemuseum",
        "Museum Europäischer Kulturen",
        "Jüdisches Museum Berlin",
        "Berlinische Galerie",
        "Sammlung Boros",
        "Neues Museum",
        "Ethnologisches Museum (Humboldt Forum)",
        "Museumsinsel (Pergamonmuseum, Neues Museum, Alte Nationalgalerie, Altes Museum, Bode Museum)",
        "KW Institute for Contemporary Art",
        "Bode Museum",
        "Gropius Bau",
        "Haus der Kulturen der Welt",
        "Kupferstichkabinett",
        "Neue Nationalgalerie",
        "Gemäldegalerie",
        "Alte Nationalgalerie",
    ],
    # User-provided curated venue list (Vienna, AT).
    "vienna": [
        "Albertina",
        "Belvedere (Upper & Lower)",
        "Technisches Museum Wien",
        "Kunsthistorisches Museum",
        "Kunst Haus Wien",
        "Jewish Museum Vienna",
        "Albertina Modern",
        "mumok – Museum moderner Kunst Stiftung Ludwig Wien",
        "Wien Museum",
        "Museum für angewandte Kunst (MAK)",
        "Leopold Museum",
        "MAK – Museum of Applied Arts",
        "Museum of Military History (Heeresgeschichtliches Museum)",
        "Liechtenstein Museum",
        "Sigmund Freud Museum",
        "Mozart Museum (Mozarthaus Vienna)",
        "Theatermuseum",
        "Papyrus Museum",
        "Imperial Treasury (Schatzkammer)",
    ],
    # User-provided curated venue list (Venice, IT).
    "venice": [
        "Gallerie dell’Accademia",
        "Ca’ Rezzonico",
        "Museo Fortuny",
        "Palazzo Ducale",
        "Scuola Grande di San Rocco",
        "Arsenale",
        "Peggy Guggenheim Collection",
        "Palazzo Grassi",
        "Museo Correr",
        "Fondazione Prada Venezia (Ca’ Corner della Regina)",
        "Ca’ Pesaro – Galleria Internazionale d’Arte Moderna",
        "Punta della Dogana",
        "Scuola Grande di San Giovanni Evangelista",
        "Palazzo Mocenigo",
        "Museo di Palazzo Grimani",
        "Museo Archeologico Nazionale di Venezia",
    ],
    # User-provided curated venue list (Brussels, BE).
    "brussels": [
        "Royal Museums of Fine Arts of Belgium",
        "Magritte Museum",
        "BELvue Museum",
        "Bozar (Centre for Fine Arts)",
        "Museum of Natural Sciences",
        "Autoworld",
        "Oldmasters Museum",
        "KANAL – Centre Pompidou",
        "La Monnaie / De Munt",
        "Musical Instruments Museum (MIM)",
        "Fin-de-Siècle Museum",
        "WIELS Contemporary Art Centre",
        "Fashion & Lace Museum",
        "Art & History Museum (Cinquantenaire)",
        "La Loge",
        "CENTRALE for contemporary art",
    ],
    # User-provided curated venue list (Lisbon, PT).
    "lisbon": [
        "MAAT – Museu de Arte, Arquitetura e Tecnologia",
        "Museu Bordalo II",
        "Museu Calouste Gulbenkian",
        "Museu de Lisboa (Palácio Pimenta and other sites)",
        "Museu do Aljube – Resistência e Liberdade",
        "Museu do Chiado – Museu Nacional de Arte Contemporânea",
        "Museu do Design (MUDE)",
        "Museu do Oriente",
        "Museu do Teatro e da Dança",
        "Museu Nacional de Arte Antiga",
        "Museu Nacional do Azulejo",
        # Kept as provided, but note: "MIM" is also used for Brussels; verify the intended Lisbon venue name if needed.
        "Musical Instruments Museum (MIM)",
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
    # Always render venue as "The <Venue>" in the forced first sentence of long English copy.
    templates = _TEMP_LONG_OPENING_PREFIX_TEMPLATES
    venue_for_copy = _with_title_the_for_copy(venue)
    base = _stable_index(seed, len(templates))
    if not used:
        return templates[base].format(venue=venue_for_copy).strip()
    for offset in range(len(templates)):
        cand = (
            templates[(base + offset) % len(templates)]
            .format(venue=venue_for_copy)
            .strip()
        )
        if used.get(cand, 0) == 0:
            used[cand] = 1
            return cand
    cand = templates[base].format(venue=venue_for_copy).strip()
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
    if raw.lower() == _VENUE_OPENING_INFO_FALLBACK.lower():
        return _VENUE_OPENING_INFO_FALLBACK

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


def _parse_coord_pair(lat_value, lon_value) -> tuple[str, str] | None:
    lat = _normalise_coord(lat_value)
    lon = _normalise_coord(lon_value)
    if not lat or not lon:
        return None
    return lat, lon


def _dedupe_exhibition_key(item: dict) -> str:
    """
    Stable identity key for deduping the same exhibition found across multiple searches/passes.

    IMPORTANT: Do NOT include `source_url` or `address` in the key since the same exhibition can
    appear with different URLs (EN/FR pages) or address formatting. Including those leads to
    duplicates leaking through into the final output.
    """
    venue = str(item.get("venue") or "").strip()
    label = str(item.get("name") or "").strip()
    title = label
    low = label.lower()
    marker = ", exhibition,"
    if marker in low:
        idx = low.find(marker)
        title = label[:idx].strip()
    start_raw = str(item.get("start_date") or "").strip()
    end_raw = str(item.get("end_date") or "").strip()
    s = _parse_date(start_raw)
    e = _parse_date(end_raw) if end_raw else None
    start_iso = s.isoformat() if s else start_raw
    end_iso = (e.isoformat() if e else end_raw) or start_iso
    return _normalise_for_dedupe("|".join([venue, title, start_iso, end_iso]))


def _score_exhibition_item(item: dict) -> int:
    score = 0
    if (item.get("source_url") or "").strip():
        score += 5
    addr = (item.get("address") or "").strip()
    if len(addr) >= 20:
        score += 2
    if _parse_coord_pair(item.get("latitude"), item.get("longitude")):
        score += 2
    if (item.get("information") or "").strip():
        score += 1
    open_days = (item.get("open_days") or "").strip()
    opening_hours = (item.get("opening_hours") or "").strip()
    if open_days and open_days != _VENUE_OPENING_INFO_FALLBACK:
        score += 1
    if opening_hours and opening_hours != _VENUE_OPENING_INFO_FALLBACK:
        score += 1
    return score


def _merge_exhibition_items_keep_best(existing: dict, candidate: dict) -> dict:
    """
    Prefer the higher-quality record and fill missing fields from the other.
    """
    a = existing or {}
    b = candidate or {}
    sa = _score_exhibition_item(a)
    sb = _score_exhibition_item(b)
    best, other = (b, a) if sb > sa else (a, b)
    out = dict(best)
    for k, v in other.items():
        if k not in out or out.get(k) in ("", None):
            out[k] = v
    # Prefer longer, more specific address if both are present.
    try:
        if isinstance(a.get("address"), str) and isinstance(b.get("address"), str):
            if len(b.get("address", "")) > len(out.get("address", "")):
                out["address"] = b.get("address", "")
    except Exception:
        pass
    return out


async def _lookup_venue_coords_async(
    *,
    client: AsyncOpenAI,
    venue: str,
    address: str,
    city: str,
    country: str,
    use_web_search_tool: bool,
) -> tuple[str, str, str] | None:
    """
    Best-effort coordinate lookup for a venue when the main search returns missing/invalid coords.
    Returns: (latitude, longitude, source_url).
    """
    raw_key = "|".join([venue or "", address or "", city or "", country or ""])
    key = _normalise_for_dedupe(raw_key)
    if key and key in _TEMP_VENUE_COORD_CACHE:
        return _TEMP_VENUE_COORD_CACHE.get(key)

    tools = [{"type": "web_search"}] if use_web_search_tool else None
    prompt = (
        "Find the latitude/longitude for this venue (museum/gallery) in the given city.\n"
        "Return ONLY JSON with keys: latitude, longitude, source_url.\n"
        "- latitude/longitude must be decimals.\n"
        "- source_url must be the page you used to verify the coordinates (official site or a reliable map listing).\n"
        "- If you cannot verify reliably, return empty strings.\n\n"
        f"Venue: {venue}\n"
        f"Address: {address}\n"
        f"City: {city}\n"
        f"Country: {country}\n"
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=600,
            ),
            max_attempts=3,
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            return None
        lat_lon = _parse_coord_pair(data.get("latitude"), data.get("longitude"))
        if not lat_lon:
            return None
        src = str(data.get("source_url") or "").strip()
        out = (lat_lon[0], lat_lon[1], src)
        if key:
            _TEMP_VENUE_COORD_CACHE[key] = out
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_venue_coord_lookup_error venue=%s city=%s country=%s err=%r",
            venue,
            city,
            country,
            exc,
        )
        return None


async def _lookup_venue_opening_info_async(
    *,
    client: AsyncOpenAI,
    venue: str,
    address: str,
    city: str,
    country: str,
    website_url: str,
    use_web_search_tool: bool,
) -> dict[str, str] | None:
    """
    Best-effort opening hours/open days lookup for a venue.
    Returns: {opening_hours, open_days, source_url}
    Values may be the fallback string if not verifiable.
    """
    raw_key = "|".join(
        [venue or "", address or "", city or "", country or "", website_url or ""]
    )
    key = _normalise_for_dedupe(raw_key)
    if key and key in _TEMP_VENUE_HOURS_CACHE:
        return _TEMP_VENUE_HOURS_CACHE.get(key)

    tools = [{"type": "web_search"}] if use_web_search_tool else None
    prompt = (
        "Find the venue opening hours and open days for this museum/gallery.\n"
        "Return ONLY JSON with keys: opening_hours, open_days, source_url.\n"
        "- opening_hours must be in this exact format (no spaces):\n"
        "  Mon:12:00-14:00,Wed:08:00-12:00,Fri:14:00-18:00\n"
        "  Use 24-hour time with leading zeros; omit closed days.\n"
        "  If multiple intervals per day, use '/': Mon:10:00-13:00/14:00-18:00\n"
        "- open_days must be comma-separated weekdays when open (e.g. Tue,Wed,Thu,Fri,Sat,Sun).\n"
        "- Use the venue’s official hours/practical-information page when possible.\n"
        f"- If you cannot verify reliably, set opening_hours to '{_VENUE_OPENING_INFO_FALLBACK}' and open_days to '{_VENUE_OPENING_INFO_FALLBACK}'. Do not guess.\n\n"
        f"Venue: {venue}\n"
        f"Address: {address}\n"
        f"City: {city}\n"
        f"Country: {country}\n"
        f"Official website (if known): {website_url}\n"
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=900,
            ),
            max_attempts=3,
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            return None
        opening_raw = str(data.get("opening_hours") or "").strip()
        open_days_raw = str(data.get("open_days") or "").strip()
        src = str(data.get("source_url") or "").strip()

        opening_norm = _normalise_opening_hours(opening_raw) if opening_raw else ""
        opening_out = opening_norm or opening_raw
        open_days_out = open_days_raw

        if not opening_out:
            opening_out = _VENUE_OPENING_INFO_FALLBACK
        if not open_days_out:
            open_days_out = _VENUE_OPENING_INFO_FALLBACK

        out = {
            "opening_hours": opening_out,
            "open_days": open_days_out,
            "source_url": src,
        }
        if key:
            _TEMP_VENUE_HOURS_CACHE[key] = out
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_venue_hours_lookup_error venue=%s city=%s country=%s err=%r",
            venue,
            city,
            country,
            exc,
        )
        return None


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


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        key = _normalise_for_dedupe(v or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _with_title_the_for_copy(name: str) -> str:
    """
    For English copy only: render venues as "The <Name>" (capital T).
    Avoid double-articles like "The the ...", and strip common non-English articles.
    """
    raw = (name or "").strip()
    if not raw:
        return "The venue"
    lowered = raw.lower()
    for prefix in ("the ", "le ", "la ", "les ", "l'", "l’"):
        if lowered.startswith(prefix):
            raw = raw[len(prefix) :].lstrip()
            break
    if raw.lower().startswith("the "):
        raw = raw[4:].lstrip()
    return f"The {raw}".strip()


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
    # Avoid awkward double-articles for venues that already include a leading French article.
    if re.match(r"^(le|la|les)\s+", v, flags=re.I) or v.lower().startswith(("l'", "l’")):
        return v
    # Only apply to venue types where English convention typically uses "the".
    if re.search(
        r"\b(museum|mus[eé]e|foundation|fondation|gallery|centre|center|palais|palace|institute|institut|collection)\b",
        v,
        flags=re.I,
    ):
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
        r'<meta[^>]+property=["\']og:image:secure_url["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image:secure_url["\']',
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        r'<meta[^>]+name=["\']twitter:image:src["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image:src["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
    return ""


def _normalise_http_url(url: str, *, base_url: str = "") -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("data:"):
        return ""
    if u.startswith("//"):
        # scheme-relative
        try:
            scheme = urlparse(base_url).scheme or "https"
        except Exception:
            scheme = "https"
        u = f"{scheme}:{u}"
    if base_url:
        try:
            u = urljoin(base_url, u)
        except Exception:
            pass
    try:
        p = urlparse(u)
        if p.scheme not in ("http", "https") or not p.netloc:
            return ""
    except Exception:
        return ""
    return u


def _extract_icon_url(html: str) -> str:
    if not html:
        return ""
    patterns = [
        r'<link[^>]+rel=["\'](?:shortcut\s+icon|icon)["\'][^>]+href=["\']([^"\']+)["\']',
        r'<link[^>]+href=["\']([^"\']+)["\'][^>]+rel=["\'](?:shortcut\s+icon|icon)["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
    return ""


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc or ""
    except Exception:
        return ""


def _google_favicon_url(domain: str, *, size: int) -> str:
    d = (domain or "").strip()
    if not d:
        return ""
    # Returns an image for most domains; used as last-resort to guarantee non-empty URLs.
    return f"https://www.google.com/s2/favicons?domain={d}&sz={size}"


async def _discover_venues_async(
    *,
    client: AsyncOpenAI,
    city: str,
    country: str,
    use_web_search_tool: bool,
    max_venues: int,
) -> list[dict[str, str]]:
    """
    Return a list of candidate venues for a city to seed deeper exhibition discovery.
    Each item: {"venue": str, "website_url": str}.
    """
    key = _normalise_for_dedupe("|".join([city, country, str(max_venues)]))
    if key and key in _TEMP_VENUE_DISCOVERY_CACHE:
        return _TEMP_VENUE_DISCOVERY_CACHE.get(key, [])

    tools = [{"type": "web_search"}] if use_web_search_tool else None
    prompt = (
        "List museums/galleries/venues in the given city that host temporary exhibitions.\n"
        "Return ONLY a raw JSON array (no prose, no code fences).\n"
        "Each element must be an object with keys: venue, website_url.\n"
        "- Prefer major museums, contemporary art centres, photography spaces, foundations, and university museums.\n"
        "- website_url should be the official venue website homepage when possible; otherwise empty string.\n"
        f"- Return up to {max_venues} venues.\n"
        "- Do not include duplicate venues.\n\n"
        f"City: {city}\n"
        f"Country: {country}\n"
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=6000,
            ),
            max_attempts=3,
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        arr = _extract_json_array(content) if content else None
        if not isinstance(arr, list):
            return []
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in arr:
            if not isinstance(item, dict):
                continue
            v = str(item.get("venue") or "").strip()
            u = str(item.get("website_url") or "").strip()
            if not v:
                continue
            u = _normalise_http_url(u)
            dedupe_key = _normalise_for_dedupe(v) or _domain_from_url(u)
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            out.append({"venue": v, "website_url": u})
            if len(out) >= max_venues:
                break
        if key:
            _TEMP_VENUE_DISCOVERY_CACHE[key] = out
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug("temp_venue_discovery_error city=%s country=%s err=%r", city, country, exc)
        return []


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
    img = _normalise_http_url(_extract_meta_image_url(html), base_url=source_url)
    if not img:
        icon = _normalise_http_url(_extract_icon_url(html), base_url=source_url)
        if icon:
            img = icon
    if not img and homepage:
        # 2) Venue homepage
        html_home = await asyncio.to_thread(_fetch_url_text, homepage)
        img = _normalise_http_url(_extract_meta_image_url(html_home), base_url=homepage)
        if not img:
            icon = _normalise_http_url(_extract_icon_url(html_home), base_url=homepage)
            if icon:
                img = icon

    # 3) Domain favicon fallback (cheap, usually available).
    if not img:
        domain = _domain_from_url(homepage or source_url)
        if domain:
            img = _google_favicon_url(
                domain, size=int(getattr(settings, "TEMP_IMAGE_FAVICON_SIZE", 256) or 256)
            )

    # 4) OpenAI fallback (optional; only if we have a client).
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
                        max_output_tokens=500,
                    ),
                    max_attempts=3,
                )
                content = _clean_json_content(
                    resp.output_text or _extract_response_text(resp) or ""
                )
                data = _extract_json_object(content) if content else None
                if isinstance(data, dict):
                    img = _normalise_http_url(str(data.get("image_url") or ""), base_url="")
                    page = _normalise_http_url(str(data.get("page_url") or ""), base_url="")
                    if not img and page:
                        html_page = await asyncio.to_thread(_fetch_url_text, page)
                        img = _normalise_http_url(_extract_meta_image_url(html_page), base_url=page)
                        if not img:
                            icon = _normalise_http_url(_extract_icon_url(html_page), base_url=page)
                            if icon:
                                img = icon
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "temp_venue_image_search_error venue=%s city=%s country=%s err=%r",
                    venue,
                    city,
                    country,
                    exc,
                )
                img = ""

    # 5) Final fallback: always return a non-empty URL.
    if not img:
        img = str(getattr(settings, "TEMP_IMAGE_FALLBACK_URL", "") or "").strip()

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

    venue_for_copy = _with_title_the_for_copy(venue)
    base_prompt = (
        "Write copy for a Divento temporary exhibition listing.\n\n"
        "INPUTS\n"
        f"- Title: {title}\n"
        f"- Venue: {venue_for_copy}\n"
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
        "- Do not include the venue’s postal address (no street, postcode, or 'address is …').\n"
        f"- When mentioning the venue name in the copy, always write it with a leading 'The' (capital T), matching: {venue_for_copy}.\n\n"
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

    # We always push hard for a full result set. If no explicit cap is provided (target_max<=0),
    # we still enforce a hard safety cap.
    hard_max = max(1, int(getattr(settings, "TEMP_HARD_MAX_EXHIBITIONS", 200) or 200))
    pass_max = max(5, int(getattr(settings, "TEMP_SEARCH_PASS_MAX_ITEMS", 60) or 60))
    desired_max = int(target_max) if int(target_max) > 0 else hard_max
    desired_min = min(int(getattr(settings, "TEMP_TARGET_MIN_EXHIBITIONS", 15) or 15), desired_max)
    passes = max(1, int(getattr(settings, "TEMP_SEARCH_PASSES", 3) or 3))
    venue_deepen_passes = max(
        0, int(getattr(settings, "TEMP_VENUE_DEEPEN_PASSES", 1) or 1)
    )
    venue_deepen_max_venues = int(getattr(settings, "TEMP_VENUE_DEEPEN_MAX_VENUES", 12) or 0)
    venue_deepen_max_per_venue = max(
        1, int(getattr(settings, "TEMP_VENUE_DEEPEN_MAX_PER_VENUE", 3) or 3)
    )
    geo_conc = max(1, int(getattr(settings, "TEMP_GEO_CONCURRENCY", 4) or 4))
    venue_discovery_enabled = int(getattr(settings, "TEMP_VENUE_DISCOVERY_ENABLED", 1) or 0) != 0
    venue_discovery_max = int(getattr(settings, "TEMP_VENUE_DISCOVERY_MAX", 50) or 0)

    venue_discovery_block = ""
    discovered_venues: list[dict[str, str]] = []
    if venue_discovery_enabled and venue_discovery_max > 0:
        discovered_venues = await _discover_venues_async(
            client=client,
            city=city,
            country="",
            use_web_search_tool=use_web_search_tool,
            max_venues=venue_discovery_max,
        )
        if discovered_venues:
            lines: list[str] = []
            for it in discovered_venues[: min(venue_discovery_max, 60)]:
                v = (it.get("venue") or "").strip()
                u = (it.get("website_url") or "").strip()
                if not v:
                    continue
                if u:
                    lines.append(f"{v} ({u})")
                else:
                    lines.append(v)
            if lines:
                venue_discovery_block = (
                    "Venue discovery seed list (use these to broaden coverage and then find temporary exhibitions at each venue):\n- "
                    + "\n- ".join(lines)
                    + "\n\n"
                )

    curated_enabled = int(getattr(settings, "TEMP_CURATED_VENUES_ENABLED", 1) or 0) != 0
    curated_max_venues = int(getattr(settings, "TEMP_CURATED_VENUES_MAX_VENUES", 0) or 0)
    curated_max_items_per_venue = max(
        1, int(getattr(settings, "TEMP_CURATED_VENUES_MAX_ITEMS_PER_VENUE", 8) or 8)
    )
    curated_venues = _dedupe_preserve_order(_CITY_CURATED_VENUES.get(city_norm, []))
    if curated_max_venues > 0:
        curated_venues = curated_venues[:curated_max_venues]

    curated_block = ""
    if curated_enabled and curated_venues:
        curated_block = (
            "Priority venue list (MUST attempt to find temporary exhibitions at each venue below, then expand beyond this list):\n- "
            + "\n- ".join(curated_venues)
            + "\n\n"
        )

    if desired_max == 1:
        select_clause = "- Return exactly 1 distinct exhibition (no duplicates).\n\n"
    elif int(target_max) <= 0:
        select_clause = (
            f"- Return as many distinct exhibitions as you can find (aim for a lot), up to {pass_max} items in this response.\n"
            "- No duplicates.\n\n"
        )
    else:
        select_clause = (
            f"- Return as many exhibitions as you can find for this window: it is VERY IMPORTANT to reach {desired_min} to {desired_max} "
            "distinct exhibitions. Search deeply, keep count, and keep adding until you hit the maximum or truly run out. "
            "No duplicates.\n\n"
        )
    tool_line = "- Use the web_search tool.\n" if use_web_search_tool else ""
    evidence_lines = (
        "- Use web_search to find real pages.\n"
        "- Do not invent exhibitions, dates, venues, addresses, or opening hours.\n"
        "- If you cannot reliably find a specific field, return an empty string for that field (except dates and venue opening info).\n"
        "- Dates (start_date/end_date) should match what you find on the web; do not guess.\n"
        f"- Opening info should come from the venue’s official hours page when possible; if not verified, set opening_hours and open_days to '{_VENUE_OPENING_INFO_FALLBACK}'.\n"
    )
    if not use_web_search_tool:
        evidence_lines = (
            "- Use web sources (the model’s built-in browsing/search) to verify every field.\n"
            "- Dates (start_date/end_date) MUST match what you find on the web; do not guess.\n"
            f"- Opening hours MUST be taken from the venue’s official hours page or a reliable source; if not verified, set opening_hours and open_days to '{_VENUE_OPENING_INFO_FALLBACK}'.\n"
            "- If you cannot reliably find a specific field, return an empty string for that field.\n"
        )
    base_prompt = (
        "I need to create new exhibitions for Divento.\n"
        "Please follow these steps exactly.\n\n"
        "IMPORTANT (anti-hallucination):\n"
        f"{tool_line}"
        f"{evidence_lines}"
        "- Prefer official venue/museum pages.\n\n"
        "Naming policy:\n"
        "- Prefer English-language official pages when available.\n"
        "- Return the exhibition title exactly as written on the source_url page you used; do not translate it.\n\n"
        "Venue strategy:\n"
        "- It is OK to return many exhibitions from the same venue if they are distinct and verifiable.\n"
        "- Do not artificially cap the number of exhibitions per venue.\n\n"
        f"{venue_hint_block}"
        f"{curated_block}"
        f"{venue_discovery_block}"
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
        "- duration: suggested visit duration as free text (for example '1 hour' or '90 minutes'). If you cannot explicitly find/verify a duration, return exactly '1 hour' (do not guess smaller values).\n"
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
        "- latitude and longitude: REQUIRED. Provide decimal coordinates for the venue based on reliable sources; do not leave blank.\n"
        "- If coordinates are not on the venue page, use a reliable map listing to verify them.\n\n"
        "5. Opening pattern\n"
        "- repeat_pattern: 'daily' or 'weekly' based on how the exhibition runs.\n"
        "- open_days: comma-separated weekdays when open (e.g. Tue,Wed,Thu,Fri,Sat,Sun). If open daily, return Mon,Tue,Wed,Thu,Fri,Sat,Sun.\n"
        f"  If open days cannot be verified reliably, return '{_VENUE_OPENING_INFO_FALLBACK}'.\n\n"
        "6. Venue opening hours (required)\n"
        "- opening_hours: venue opening hours in this exact format (no spaces):\n"
        "  Mon:12:00-14:00,Wed:08:00-12:00,Fri:14:00-18:00\n"
        "  Use 24-hour time with leading zeros; omit days the venue is closed.\n"
        "  If the venue has multiple intervals in a day, use '/' inside that day:\n"
        "  Mon:10:00-13:00/14:00-18:00\n"
        f"  If hours cannot be verified reliably, return '{_VENUE_OPENING_INFO_FALLBACK}'.\n\n"
        "Do not include image URLs or legends.\n\n"
        "Return ONLY a raw JSON array and nothing else (no code fences, no prose). If you find no exhibitions, return an empty JSON array [].\n"
        "The top-level value must be a JSON array. Each element must "
        "be an object with exactly the keys: "
        "'name', 'city', 'country', 'address', 'duration', 'start_date', 'end_date', "
        "'is_free', 'ticket_url', 'information', 'venue', 'latitude', 'longitude', 'repeat_pattern', 'open_days', 'opening_hours', 'source_url'."
    )

    try:
        tools_arg = [{"type": "web_search"}] if use_web_search_tool else None

        def _kept_estimate_count(items: list[dict]) -> int:
            """
            Estimate how many items will survive the hard filters, so we don't stop searching too early.
            Rules:
            - Exclude exhibitions that close before window_start OR open after window_end.
            - Exclude exhibitions that end within 30 days of window_start.
            """
            n = 0
            for it in items:
                if not isinstance(it, dict):
                    continue
                s_raw = (it.get("start_date") or "").strip()
                e_raw = (it.get("end_date") or "").strip()
                s_date = _parse_date(s_raw)
                e_date = _parse_date(e_raw) if e_raw else None
                if s_date and s_date > window_end:
                    continue
                if e_date and e_date < window_start:
                    continue
                if e_date and e_date < soon_cutoff:
                    continue
                n += 1
            return n

        async def _run_search(input_prompt: str) -> str:
            try:
                resp = await _call_with_backoff(
                    lambda: client.responses.create(
                        model=TEMP_SEARCH_MODEL,
                        input=input_prompt,
                        tools=tools_arg,
                        max_output_tokens=12000,
                    )
                )
                return _clean_json_content(
                    resp.output_text or _extract_response_text(resp) or ""
                )
            except Exception as exc_resp:  # noqa: BLE001
                logger.debug("temp_search_responses_error city=%s err=%r", city, exc_resp)
                return ""

        async def _run_search_with_retries(input_prompt: str) -> str:
            content = await _run_search(input_prompt)
            if not content or content.strip() in ("[]", ""):
                content = await _run_search(input_prompt)
            if content.strip() in ("[]", ""):
                content = await _run_search(input_prompt)
            return content

        combined: list[dict] = []
        seen: set[str] = set()
        last_raw = ""
        # Allow a larger pool than the final desired count because we may drop some items
        # (e.g. exhibitions ending within the next 30 days).
        if int(target_max) <= 0:
            pool_max = min(hard_max, (passes * pass_max) + 20)
        else:
            pool_max = max(desired_max * 2, desired_max + 10)

        logger.info(
            "temp_search_plan city=%s passes=%s target_min=%s target_max=%s pool_max=%s venue_deepen_passes=%s venue_deepen_max_venues=%s venue_deepen_max_per_venue=%s venue_discovery_enabled=%s",
            city,
            passes,
            desired_min,
            desired_max,
            pool_max,
            venue_deepen_passes,
            venue_deepen_max_venues,
            venue_deepen_max_per_venue,
            venue_discovery_enabled,
        )

        # Phase 0: curated venue list. Query each venue directly to ensure coverage.
        if curated_enabled and curated_venues and len(combined) < pool_max:
            logger.info(
                "temp_search_curated_plan city=%s venues=%s max_items_per_venue=%s",
                city,
                len(curated_venues),
                curated_max_items_per_venue,
            )
            for venue_name in curated_venues:
                if len(combined) >= pool_max:
                    break
                before_total = len(combined)
                before_kept = _kept_estimate_count(combined)
                existing_for_venue = [
                    f"{(it.get('name') or '').strip()} | {(it.get('start_date') or '').strip()} | {(it.get('end_date') or '').strip()}"
                    for it in combined
                    if (it.get("venue") or "").strip().lower() == venue_name.lower()
                ][:30]
                curated_prompt = (
                    base_prompt
                    + "\n\nPriority step: curated venue coverage.\n"
                    + f"- Focus ONLY on venue: {venue_name} in {city}.\n"
                    + f"- Find as many distinct temporary exhibitions at this venue within the same date window as you can, up to {curated_max_items_per_venue} items.\n"
                    + "- Do not repeat any exhibitions listed below.\n"
                    + "\nAlready found for this venue (exclude these):\n- "
                    + "\n- ".join(existing_for_venue or ["(none)"])
                    + "\n\nReturn ONLY a raw JSON array.\n"
                )
                content = await _run_search_with_retries(curated_prompt)
                if not content or content.strip() in ("[]", ""):
                    logger.info(
                        "temp_search_curated city=%s venue=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                        city,
                        venue_name,
                        0,
                        0,
                        len(combined),
                        _kept_estimate_count(combined),
                        "empty",
                    )
                    continue
                data = _extract_json_array(content)
                if data is None:
                    logger.info(
                        "temp_search_curated city=%s venue=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                        city,
                        venue_name,
                        0,
                        0,
                        len(combined),
                        _kept_estimate_count(combined),
                        "parse_failed",
                    )
                    continue
                parsed_n = len(data) if isinstance(data, list) else 0
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    k = _dedupe_exhibition_key(item)
                    if not k or k in seen:
                        continue
                    seen.add(k)
                    combined.append(item)
                    if len(combined) >= pool_max:
                        break
                after_total = len(combined)
                new_added = max(0, after_total - before_total)
                logger.info(
                    "temp_search_curated city=%s venue=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                    city,
                    venue_name,
                    parsed_n,
                    new_added,
                    after_total,
                    _kept_estimate_count(combined),
                    f"kept_before={before_kept}",
                )

        for pass_idx in range(passes):
            before_total = len(combined)
            before_kept = _kept_estimate_count(combined)
            if pass_idx == 0 or not combined:
                input_prompt = base_prompt
            else:
                exclude = []
                for it in combined[:40]:
                    exclude.append(
                        f"{(it.get('venue') or '').strip()} | {(it.get('name') or '').strip()} | {(it.get('start_date') or '').strip()} | {(it.get('end_date') or '').strip()}"
                    )
                input_prompt = (
                    base_prompt
                    + "\n\nNow find MORE distinct exhibitions under the exact same rules.\n"
                    + f"- You must add new exhibitions until you reach {desired_max} total, or truly run out.\n"
                    + "- Do not repeat any exhibitions listed below.\n"
                    + "\nAlready found (exclude these):\n- "
                    + "\n- ".join(exclude)
                    + "\n"
                )

            content = await _run_search_with_retries(input_prompt)
            last_raw = content or ""
            logger.debug(
                "temp_search_raw city=%s pass=%s chars=%s snippet=%r",
                city,
                pass_idx + 1,
                len(content or ""),
                (content or "")[:1200],
            )
            if not content or content.strip() in ("[]", ""):
                logger.info(
                    "temp_search_pass city=%s pass=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                    city,
                    pass_idx + 1,
                    0,
                    0,
                    len(combined),
                    _kept_estimate_count(combined),
                    "empty",
                )
                continue

            data = _extract_json_array(content)
            if data is None:
                logger.debug(
                    "temp_search_parse_failed city=%s pass=%s raw=%r",
                    city,
                    pass_idx + 1,
                    (content or "")[:2000],
                )
                logger.info(
                    "temp_search_pass city=%s pass=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                    city,
                    pass_idx + 1,
                    0,
                    0,
                    len(combined),
                    _kept_estimate_count(combined),
                    "parse_failed",
                )
                continue

            logger.debug("temp_search_parsed city=%s pass=%s items=%s", city, pass_idx + 1, len(data))
            for item in data:
                if not isinstance(item, dict):
                    continue
                k = _dedupe_exhibition_key(item)
                if not k or k in seen:
                    continue
                seen.add(k)
                combined.append(item)
                if len(combined) >= pool_max:
                    break

            kept_est = _kept_estimate_count(combined)
            after_total = len(combined)
            new_added = max(0, after_total - before_total)
            logger.info(
                "temp_search_pass city=%s pass=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                city,
                pass_idx + 1,
                len(data),
                new_added,
                after_total,
                kept_est,
                f"kept_before={before_kept}",
            )
            # If we're uncapped, always run all base passes and just respect the safety pool limit.
            if int(target_max) > 0 and kept_est >= desired_max:
                break
            if len(combined) >= pool_max:
                break

        if not combined:
            logger.debug("temp_search_empty city=%s raw=%r", city, last_raw[:2000])
            return []

        # Per-venue deepening: query specific venues to extract additional temporary exhibitions.
        if (
            venue_deepen_passes > 0
            and len(combined) < pool_max
            and (int(target_max) > 0 and _kept_estimate_count(combined) < desired_max)
        ):
            venue_counts: dict[str, int] = {}
            for it in combined:
                v = (it.get("venue") or "").strip()
                if not v:
                    continue
                venue_counts[v] = venue_counts.get(v, 0) + 1

            venues_sorted = sorted(venue_counts.items(), key=lambda x: (-x[1], x[0].lower()))
            venues = [v for v, _cnt in venues_sorted]

            # Also deepen on discovered venues (even if not present in the first pass results).
            for it in (discovered_venues or [])[: max(0, venue_discovery_max)]:
                v = (it.get("venue") or "").strip()
                if v and v not in venues:
                    venues.append(v)
            if venue_deepen_max_venues > 0:
                venues = venues[:venue_deepen_max_venues]

            for _pass in range(venue_deepen_passes):
                if _kept_estimate_count(combined) >= desired_max:
                    break
                logger.info(
                    "temp_search_deepen_pass city=%s pass=%s venues=%s kept_est=%s total=%s",
                    city,
                    _pass + 1,
                    len(venues),
                    _kept_estimate_count(combined),
                    len(combined),
                )
                for venue_name in venues:
                    if _kept_estimate_count(combined) >= desired_max:
                        break
                    before_total = len(combined)
                    existing_for_venue = [
                        f"{(it.get('name') or '').strip()} | {(it.get('start_date') or '').strip()} | {(it.get('end_date') or '').strip()}"
                        for it in combined
                        if (it.get("venue") or "").strip() == venue_name
                    ][:20]
                    deepen_prompt = (
                        base_prompt
                        + "\n\nExtra step: go deep for a single venue.\n"
                        + f"- Focus ONLY on venue: {venue_name} in {city}.\n"
                        + f"- Find up to {venue_deepen_max_per_venue} additional distinct temporary exhibitions at this venue within the same date window.\n"
                        + "- Do not repeat any exhibitions listed below.\n"
                        + "\nAlready found for this venue (exclude these):\n- "
                        + "\n- ".join(existing_for_venue or ["(none)"])
                        + "\n\nReturn ONLY a raw JSON array.\n"
                    )
                    content = await _run_search_with_retries(deepen_prompt)
                    logger.debug(
                        "temp_search_venue_raw city=%s venue=%s chars=%s snippet=%r",
                        city,
                        venue_name,
                        len(content or ""),
                        (content or "")[:600],
                    )
                    if not content or content.strip() in ("[]", ""):
                        continue
                    data = _extract_json_array(content)
                    if data is None:
                        logger.debug(
                            "temp_search_venue_parse_failed city=%s venue=%s raw=%r",
                            city,
                            venue_name,
                            (content or "")[:2000],
                        )
                        logger.info(
                            "temp_search_deepen city=%s pass=%s venue=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                            city,
                            _pass + 1,
                            venue_name,
                            0,
                            0,
                            len(combined),
                            _kept_estimate_count(combined),
                            "parse_failed",
                        )
                        continue
                    parsed_n = len(data) if isinstance(data, list) else 0
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        k = _dedupe_exhibition_key(item)
                        if not k or k in seen:
                            continue
                        seen.add(k)
                        combined.append(item)
                        if len(combined) >= pool_max:
                            break
                    after_total = len(combined)
                    new_added = max(0, after_total - before_total)
                    logger.info(
                        "temp_search_deepen city=%s pass=%s venue=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                        city,
                        _pass + 1,
                        venue_name,
                        parsed_n,
                        new_added,
                        after_total,
                        _kept_estimate_count(combined),
                        "",
                    )

        # Fill missing venue coordinates (best-effort) so missing coords don't reduce usable output.
        missing = [
            it
            for it in combined
            if not _parse_coord_pair(it.get("latitude"), it.get("longitude"))
        ]
        if missing:
            sem = asyncio.Semaphore(geo_conc)

            async def _fill_one(it: dict) -> None:
                async with sem:
                    looked = await _lookup_venue_coords_async(
                        client=client,
                        venue=(it.get("venue") or "").strip(),
                        address=(it.get("address") or "").strip(),
                        city=(it.get("city") or city).strip(),
                        country=(it.get("country") or "").strip(),
                        use_web_search_tool=use_web_search_tool,
                    )
                    if looked:
                        it["latitude"] = looked[0]
                        it["longitude"] = looked[1]

            await asyncio.gather(*[_fill_one(it) for it in missing[: desired_max * 2]])

        logger.debug("temp_search_combined city=%s items=%s", city, len(combined))

        filtered: list[dict] = []
        # Best-effort backfill of venue opening info after the main search. We'll apply per-venue
        # results across all exhibitions, and guarantee a non-empty string fallback at export time.
        hours_backfill_enabled = (
            int(getattr(settings, "TEMP_VENUE_HOURS_BACKFILL_ENABLED", 1) or 0) != 0
        )
        hours_backfill_conc = max(
            1, int(getattr(settings, "TEMP_VENUE_HOURS_BACKFILL_CONCURRENCY", 4) or 4)
        )
        for item in combined:
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

            # Exclude only: closes before window_start OR opens after window_end.
            if s_date and s_date > window_end:
                continue
            if e_date and e_date < window_start:
                continue
            # Also exclude: ends within 30 days from window_start.
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
            opening_hours_raw = (item.get("opening_hours") or "").strip()
            opening_norm = _normalise_opening_hours(opening_hours_raw)
            item["opening_hours"] = opening_norm or opening_hours_raw
            item["repeat_pattern"] = (item.get("repeat_pattern") or "").strip()
            item["open_days"] = (item.get("open_days") or "").strip()
            item["information"] = (item.get("information") or "").strip()
            item["ticket_url"] = (item.get("ticket_url") or "").strip()
            item["duration"] = (item.get("duration") or "").strip()
            item["source_url"] = source_url
            filtered.append(item)

        # Final dedupe after normalisation (important for multi-pass/curated phases).
        # This removes cases where the same exhibition is returned with different `source_url`
        # or slightly different address formatting.
        if filtered:
            by_key: dict[str, dict] = {}
            for it in filtered:
                k = _dedupe_exhibition_key(it)
                if not k:
                    continue
                if k not in by_key:
                    by_key[k] = it
                else:
                    by_key[k] = _merge_exhibition_items_keep_best(by_key[k], it)
            if len(by_key) != len(filtered):
                logger.info(
                    "temp_search_deduped city=%s before=%s after=%s",
                    city,
                    len(filtered),
                    len(by_key),
                )
            filtered = list(by_key.values())

        if hours_backfill_enabled and filtered:
            # Only look up venues that have missing opening info. Do this once per venue,
            # then apply across all its exhibitions.
            per_venue: dict[str, dict[str, str]] = {}
            for it in filtered:
                v = (it.get("venue") or "").strip()
                if not v:
                    continue
                if v in per_venue:
                    continue
                od = (it.get("open_days") or "").strip()
                oh = (it.get("opening_hours") or "").strip()
                if od and oh:
                    continue
                per_venue[v] = {
                    "venue": v,
                    "address": (it.get("address") or "").strip(),
                    "city": (it.get("city") or "").strip(),
                    "country": (it.get("country") or "").strip(),
                    "website_url": (it.get("source_url") or "").strip(),
                }

            if per_venue:
                sem = asyncio.Semaphore(hours_backfill_conc)

                async def _fill_venue(vinfo: dict[str, str]) -> tuple[str, dict[str, str]] | None:
                    async with sem:
                        got = await _lookup_venue_opening_info_async(
                            client=client,
                            venue=vinfo.get("venue") or "",
                            address=vinfo.get("address") or "",
                            city=vinfo.get("city") or "",
                            country=vinfo.get("country") or "",
                            website_url=vinfo.get("website_url") or "",
                            use_web_search_tool=use_web_search_tool,
                        )
                        if not got:
                            return None
                        return (vinfo.get("venue") or "", got)

                filled = await asyncio.gather(*[_fill_venue(v) for v in per_venue.values()])
                filled_map: dict[str, dict[str, str]] = {}
                for pair in filled:
                    if not pair:
                        continue
                    name_key, got = pair
                    if name_key:
                        filled_map[name_key] = got

                if filled_map:
                    for it in filtered:
                        v = (it.get("venue") or "").strip()
                        got = filled_map.get(v)
                        if not got:
                            continue
                        if not (it.get("opening_hours") or "").strip():
                            it["opening_hours"] = got.get("opening_hours") or ""
                        if not (it.get("open_days") or "").strip():
                            it["open_days"] = got.get("open_days") or ""

        # Derive open_days from opening_hours when possible (and not already set).
        for it in filtered:
            if (it.get("open_days") or "").strip():
                continue
            oh = (it.get("opening_hours") or "").strip()
            oh_norm = _normalise_opening_hours(oh)
            if not oh_norm or oh_norm == _VENUE_OPENING_INFO_FALLBACK:
                continue
            days = []
            for part in oh_norm.split(","):
                if ":" in part:
                    days.append(part.split(":", 1)[0])
            if days:
                it["open_days"] = ",".join(days)

        logger.debug("temp_search_filtered city=%s kept=%s", city, len(filtered))
        if desired_max > 0 and len(filtered) > desired_max:
            filtered = filtered[:desired_max]
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
        opening_hours_raw = (ex.get("opening_hours") or "").strip()
        opening_hours = _normalise_opening_hours(opening_hours_raw) or opening_hours_raw
        if not open_days:
            open_days = _VENUE_OPENING_INFO_FALLBACK
        if not opening_hours:
            opening_hours = _VENUE_OPENING_INFO_FALLBACK

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
        cats = f"Top Exhibitions, Arts and Culture, Exhibitions in {city_label}".strip()
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

        duration_val = _normalise_duration_hours(duration_raw) or "1"

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

    # Final export-stage dedupe: protect against duplicates that slip through due to differing
    # upstream fields but identical final titles.
    if rows:
        def _score_export_row(r: dict) -> int:
            score = 0
            if (r.get("URL of images") or "").strip():
                score += 2
            if (r.get("Opening and closing time") or "").strip() and (
                (r.get("Opening and closing time") or "").strip() != _VENUE_OPENING_INFO_FALLBACK
            ):
                score += 2
            if (r.get("Open days") or "").strip() and (
                (r.get("Open days") or "").strip() != _VENUE_OPENING_INFO_FALLBACK
            ):
                score += 1
            if len((r.get("Full address") or "").strip()) >= 20:
                score += 1
            if (r.get("Information") or "").strip():
                score += 1
            return score

        by_title: dict[str, dict] = {}
        extras: list[dict] = []
        for r in rows:
            title = (r.get("Name of site, City") or "").strip()
            if not title:
                extras.append(r)
                continue
            existing = by_title.get(title)
            if not existing:
                by_title[title] = r
                continue
            # Keep the better row and fill missing fields from the other.
            a, b = existing, r
            best, other = (b, a) if _score_export_row(b) > _score_export_row(a) else (a, b)
            merged = dict(best)
            for k, v in other.items():
                if k not in merged or merged.get(k) in ("", None):
                    merged[k] = v
            # Prefer longer address when present.
            if len((other.get("Full address") or "")) > len((merged.get("Full address") or "")):
                merged["Full address"] = other.get("Full address") or merged.get("Full address")
            by_title[title] = merged

        if len(by_title) + len(extras) != len(rows):
            logger.info(
                "temp_export_deduped city=%s before=%s after=%s",
                city,
                len(rows),
                len(by_title) + len(extras),
            )
        rows = list(by_title.values()) + extras

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
