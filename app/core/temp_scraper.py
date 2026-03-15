import asyncio
import contextvars
import random
import re
import uuid
import json
import logging
import unicodedata
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from datetime import datetime, timedelta
from html import unescape
from urllib.parse import urlparse
from urllib.parse import urljoin, parse_qs, quote, unquote

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
_TEMP_VENUE_ADDRESS_CACHE: dict[str, dict[str, str]] = {}
_TEMP_VENUE_DISCOVERY_CACHE: dict[str, list[dict[str, str]]] = {}
_TEMP_VENUE_HOURS_CACHE: dict[str, dict[str, str]] = {}
_TEMP_IMAGE_LICENSE_CACHE: dict[str, dict[str, str]] = {}
_TEMP_DURATION_CACHE: dict[str, dict[str, str]] = {}
_FORBIDDEN_TITLE_CHARS_RE = re.compile(r"[<>;=#{}]")

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
    # User-provided curated venue list (Milan, IT).
    "milan": [
        "Pinacoteca di Brera",
        "Palazzo Reale",
        "Fondazione Prada",
        "Triennale Milano",
        "Museo del Novecento",
        "PAC – Padiglione d’Arte Contemporanea",
        "Mudec – Museo delle Culture",
        "Castello Sforzesco",
        "Gallerie d’Italia",
        "Museo Poldi Pezzoli",
        "Museo Bagatti Valsecchi",
        "Museo del Duomo di Milano",
        "Pinacoteca Ambrosiana",
        "Casa Museo Boschi Di Stefano",
        "Museo del Design ADI",
        "Museo Nazionale della Scienza e della Tecnologia Leonardo da Vinci",
        "Museo Archeologico di Milano",
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
_OPENAI_USAGE_CALLBACK = contextvars.ContextVar(
    "temp_openai_usage_callback", default=None
)


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


@contextmanager
def temp_openai_usage_tracking(callback=None):
    token = _OPENAI_USAGE_CALLBACK.set(callback)
    try:
        yield
    finally:
        _OPENAI_USAGE_CALLBACK.reset(token)


def _usage_get(obj, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _usage_int(value) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _emit_openai_usage_event(resp) -> None:
    callback = _OPENAI_USAGE_CALLBACK.get()
    if not callable(callback):
        return

    usage = getattr(resp, "usage", None)
    input_tokens = _usage_int(_usage_get(usage, "input_tokens"))
    output_tokens = _usage_int(_usage_get(usage, "output_tokens"))
    total_tokens = _usage_int(_usage_get(usage, "total_tokens"))
    if total_tokens <= 0:
        total_tokens = input_tokens + output_tokens

    input_details = _usage_get(usage, "input_tokens_details", None)
    output_details = _usage_get(usage, "output_tokens_details", None)

    event = {
        "api_calls": 1,
        "model": str(getattr(resp, "model", "") or ""),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "cached_input_tokens": _usage_int(
            _usage_get(input_details, "cached_tokens", _usage_get(input_details, "cached_input_tokens"))
        ),
        "reasoning_tokens": _usage_int(_usage_get(output_details, "reasoning_tokens")),
    }
    try:
        callback(event)
    except Exception:
        logger.debug("temp_openai_usage_callback_error", exc_info=True)


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


def _split_exhibition_label(label: str) -> tuple[str, str]:
    """
    Split a label like:
    "Title, Exhibition, Venue, City: Date range"
    into (title, "Venue, City: Date range").
    """
    text = str(label or "").strip()
    if not text:
        return "", ""
    lower = text.lower()
    marker = ", exhibition,"
    idx = lower.find(marker)
    if idx < 0:
        return text, ""
    title = text[:idx].strip(" ,")
    remainder = text[idx + len(marker) :].strip(" ,")
    return title, remainder


def _strip_label_date_suffix(value: str) -> str:
    """
    Remove a trailing date suffix after the last ':' when that suffix looks date-like.
    """
    text = str(value or "").strip()
    if not text or ":" not in text:
        return text
    head, tail = text.rsplit(":", 1)
    tail_clean = tail.strip()
    if not tail_clean:
        return head.strip()
    if re.search(r"\d", tail_clean):
        return head.strip()
    return text


def _sanitize_export_title(value: str) -> str:
    text = str(value or "")
    text = _FORBIDDEN_TITLE_CHARS_RE.sub("", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _dedupe_export_row_key(row: dict) -> str:
    label = str(row.get("Name of site, City") or "").strip()
    if not label:
        return ""
    title, remainder = _split_exhibition_label(label)
    venue_city = _strip_label_date_suffix(remainder)
    if not title:
        title = _strip_label_date_suffix(label)
    real_city = str(row.get("Real city") or row.get("City") or "").strip()
    country = str(row.get("Country") or "").strip()
    return _normalise_for_dedupe("|".join([country, real_city, venue_city, title]))


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
    city = str(item.get("city") or "").strip()
    country = str(item.get("country") or "").strip()
    label = str(item.get("name") or "").strip()
    title, _ = _split_exhibition_label(label)
    title = title or label
    return _normalise_for_dedupe("|".join([country, city, venue, title]))


def _ascii_fold_text(value: str) -> str:
    text = unescape(str(value or ""))
    text = unicodedata.normalize("NFKD", text)
    return text.encode("ascii", "ignore").decode("ascii")


def _normalise_for_similarity(value: str) -> str:
    text = _ascii_fold_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s{2,}", " ", text).strip()


def _canonical_venue_for_similarity(value: str) -> str:
    raw_src = _ascii_fold_text(value).lower().strip()
    if not raw_src:
        return ""
    if "," in raw_src:
        raw_src = raw_src.split(",", 1)[0].strip()
    raw = re.sub(r"[^a-z0-9]+", " ", raw_src)
    raw = re.sub(r"\s{2,}", " ", raw).strip()
    # Venue aliases often append long descriptors after a comma.
    # Keep the leading stable part for matching.
    head = raw.strip()
    if head.startswith("the "):
        head = head[4:].strip()
    return head or raw


def _stable_date_pair_strings(start_raw: str, end_raw: str) -> tuple[str, str]:
    s = _parse_date(str(start_raw or "").strip())
    e = _parse_date(str(end_raw or "").strip()) if str(end_raw or "").strip() else None
    start_iso = s.isoformat() if s else str(start_raw or "").strip()
    end_iso = (e.isoformat() if e else str(end_raw or "").strip()) or start_iso
    return start_iso, end_iso


_TITLE_DEDUPE_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "of",
    "in",
    "on",
    "for",
    "to",
    "de",
    "du",
    "des",
    "la",
    "le",
    "les",
    "et",
    "un",
    "une",
    "el",
    "los",
    "las",
    "una",
    "unos",
    "unas",
    "del",
    "al",
    "y",
    "o",
    "en",
    "il",
    "lo",
    "gli",
    "i",
    "una",
    "uno",
    "un",
    "e",
    "ed",
    "di",
    "da",
    "della",
    "delle",
    "museum",
    "musee",
    "museo",
    "gallery",
    "galleria",
    "exhibition",
    "exposition",
    "exhibicion",
    "mostra",
    "show",
}


_TITLE_TOKEN_EN_EQUIVALENTS = {
    "transparence": "transparency",
    "transparencia": "transparency",
    "transparenza": "transparency",
    "journee": "day",
    "journees": "days",
    "dia": "day",
    "dias": "days",
    "giornata": "day",
    "giornate": "days",
    "licorne": "unicorn",
    "licornes": "unicorns",
    "licornio": "unicorn",
    "licornios": "unicorns",
    "unicorno": "unicorn",
    "unicorni": "unicorns",
    "amour": "love",
    "amore": "love",
    "amor": "love",
    "lettres": "letters",
    "lettre": "letter",
    "lettere": "letters",
    "parisiennes": "parisian",
    "parisienne": "parisian",
    "ciel": "sky",
    "cielo": "sky",
    "reine": "queen",
    "mere": "mother",
}


def _englishise_title_for_dedupe(title: str) -> str:
    """
    Best-effort token-level normalization into English for first-word dedupe.
    """
    norm = _normalise_for_similarity(title)
    if not norm:
        return ""
    tokens = norm.split()
    mapped = [_TITLE_TOKEN_EN_EQUIVALENTS.get(tok, tok) for tok in tokens]
    return " ".join(mapped).strip()


def _significant_title_tokens(title: str) -> set[str]:
    norm = _normalise_for_similarity(title)
    if not norm:
        return set()
    out = set()
    for tok in norm.split():
        if len(tok) <= 1 or tok in _TITLE_DEDUPE_STOPWORDS or tok.isdigit():
            continue
        out.add(tok)
    return out


def _significant_title_token_list(title: str) -> list[str]:
    norm = _normalise_for_similarity(title)
    if not norm:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for tok in norm.split():
        if len(tok) <= 1 or tok in _TITLE_DEDUPE_STOPWORDS or tok.isdigit():
            continue
        if tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
    return out


def _first_title_token_for_dedupe(title: str) -> str:
    """
    Return a stable leading semantic token for title-variant matching.
    Titles are best-effort normalized to English first so bilingual variants
    can share the same first token.
    """
    english_title = _englishise_title_for_dedupe(title)
    ordered = _significant_title_token_list(english_title)
    if ordered:
        return ordered[0]
    norm = _normalise_for_similarity(english_title)
    if not norm:
        return ""
    return norm.split()[0]


def _titles_likely_same_exhibition(title_a: str, title_b: str) -> bool:
    a = _normalise_for_similarity(title_a)
    b = _normalise_for_similarity(title_b)
    if not a or not b:
        return False
    if a == b:
        return True

    # Strong containment catches cases like:
    # "Auguste Bartholdi" vs "Auguste Bartholdi Liberty Enlightening the World".
    if len(a) >= 8 and a in b:
        return True
    if len(b) >= 8 and b in a:
        return True

    ta = _significant_title_tokens(a)
    tb = _significant_title_tokens(b)
    if not ta or not tb:
        return False
    overlap = len(ta & tb)
    min_size = min(len(ta), len(tb))
    if min_size == 0:
        return False

    containment = overlap / float(min_size)
    if overlap >= 2 and containment >= 0.8:
        return True
    if overlap >= 3 and containment >= 0.67 and abs(len(ta) - len(tb)) <= 3:
        return True

    # Match bilingual/expanded variants that share the same leading proper-name stem,
    # e.g. "Cheryl Marie Wade ..." vs "Cheryl Marie Wade ...".
    ordered_a = _significant_title_token_list(a)
    ordered_b = _significant_title_token_list(b)
    # Name-anchor rule for bilingual title variants:
    # if both titles share the same first two significant tokens (typically artist/person),
    # treat as the same exhibition within the same venue+date bucket.
    # Example: "Madame de Sevigne Lettres parisiennes" vs "Madame de Sevigne Parisian letters".
    if len(ordered_a) >= 2 and len(ordered_b) >= 2 and ordered_a[:2] == ordered_b[:2]:
        return True
    if len(ordered_a) >= 3 and len(ordered_b) >= 3 and ordered_a[:3] == ordered_b[:3]:
        return True
    return False


def _fuzzy_dedupe_items_same_dates(items: list[dict]) -> list[dict]:
    if not items:
        return items
    date_only = int(getattr(settings, "TEMP_DEDUPE_BY_VENUE_DATES_ONLY", 0) or 0) != 0

    grouped: dict[str, list[dict]] = {}
    extras: list[dict] = []
    for it in items:
        label = str(it.get("name") or "").strip()
        title, remainder = _split_exhibition_label(label)
        venue_raw = str(it.get("venue") or "").strip()
        if not venue_raw and remainder:
            venue_raw = remainder.split(":", 1)[0].strip()
        start_iso, end_iso = _stable_date_pair_strings(
            str(it.get("start_date") or "").strip(),
            str(it.get("end_date") or "").strip(),
        )
        venue_key = _canonical_venue_for_similarity(venue_raw)
        city_key = _normalise_for_similarity(str(it.get("city") or "").strip())
        country_key = _normalise_for_similarity(str(it.get("country") or "").strip())
        if not (title and venue_key and start_iso and end_iso):
            extras.append(it)
            continue
        group_key = "|".join([country_key, city_key, venue_key, start_iso, end_iso])
        grouped.setdefault(group_key, []).append(it)

    deduped: list[dict] = []
    for group_items in grouped.values():
        if date_only:
            ordered = sorted(group_items, key=_score_exhibition_item, reverse=True)
            kept_by_first: dict[str, dict] = {}
            for pos, candidate in enumerate(ordered):
                cand_title, _ = _split_exhibition_label(str(candidate.get("name") or "").strip())
                cand_title = cand_title or str(candidate.get("name") or "").strip()
                first_key = _first_title_token_for_dedupe(cand_title) or f"__row_{pos}"
                existing = kept_by_first.get(first_key)
                if not existing:
                    kept_by_first[first_key] = candidate
                    continue
                kept_by_first[first_key] = _merge_exhibition_items_keep_best(existing, candidate)
            deduped.extend(kept_by_first.values())
            continue
        ordered = sorted(group_items, key=_score_exhibition_item, reverse=True)
        kept: list[dict] = []
        for candidate in ordered:
            cand_title, _ = _split_exhibition_label(str(candidate.get("name") or "").strip())
            cand_title = cand_title or str(candidate.get("name") or "").strip()
            duplicate_idx = -1
            for idx, existing in enumerate(kept):
                existing_title, _ = _split_exhibition_label(str(existing.get("name") or "").strip())
                existing_title = existing_title or str(existing.get("name") or "").strip()
                if _titles_likely_same_exhibition(cand_title, existing_title):
                    duplicate_idx = idx
                    break
            if duplicate_idx < 0:
                kept.append(candidate)
            else:
                kept[duplicate_idx] = _merge_exhibition_items_keep_best(
                    kept[duplicate_idx], candidate
                )
        deduped.extend(kept)

    deduped.extend(extras)
    return deduped


def _dedupe_export_rows_same_dates_cross_venue(rows: list[dict], score_row_fn) -> list[dict]:
    """
    Final safety dedupe for export rows:
    if rows share the exact same city+country+start/end dates and the titles look like the
    same exhibition, keep only the best row even when venue strings differ.
    """
    if not rows:
        return rows

    grouped: dict[str, list[dict]] = {}
    extras: list[dict] = []
    for r in rows:
        label = str(r.get("Name of site, City") or "").strip()
        title, _remainder = _split_exhibition_label(label)
        title = title or _strip_label_date_suffix(label)
        city_key = _normalise_for_similarity(str(r.get("Real city") or r.get("City") or "").strip())
        country_key = _normalise_for_similarity(str(r.get("Country") or "").strip())
        start_iso, end_iso = _stable_date_pair_strings(
            str(r.get("Start date (YYYY-MM-DD)") or "").strip(),
            str(r.get("End date (YYYY-MM-DD)") or "").strip(),
        )
        if not (title and start_iso and end_iso):
            extras.append(r)
            continue
        group_key = "|".join([country_key, city_key, start_iso, end_iso])
        grouped.setdefault(group_key, []).append(r)

    out: list[dict] = []
    for group in grouped.values():
        ordered = sorted(group, key=score_row_fn, reverse=True)
        kept: list[dict] = []
        for cand in ordered:
            cand_label = str(cand.get("Name of site, City") or "").strip()
            cand_title, _ = _split_exhibition_label(cand_label)
            cand_title = cand_title or _strip_label_date_suffix(cand_label)

            dup_idx = -1
            for idx, existing in enumerate(kept):
                ex_label = str(existing.get("Name of site, City") or "").strip()
                ex_title, _ = _split_exhibition_label(ex_label)
                ex_title = ex_title or _strip_label_date_suffix(ex_label)
                if _titles_likely_same_exhibition(cand_title, ex_title):
                    dup_idx = idx
                    break

            if dup_idx < 0:
                kept.append(cand)
                continue

            a, b = kept[dup_idx], cand
            best, other = (b, a) if score_row_fn(b) > score_row_fn(a) else (a, b)
            merged = dict(best)
            for k, v in other.items():
                if k not in merged or merged.get(k) in ("", None):
                    merged[k] = v
            if len((other.get("Full address") or "")) > len((merged.get("Full address") or "")):
                merged["Full address"] = other.get("Full address") or merged.get("Full address")
            kept[dup_idx] = merged

        out.extend(kept)

    out.extend(extras)
    return out


def _dedupe_keep_first_same_venue_dates(items: list[dict]) -> list[dict]:
    """
    Order-preserving safety dedupe for non-curated search paths.
    If multiple rows share the same city/country and exact start/end dates,
    keep the first row and fill any missing fields from later rows.

    This is intentionally aggressive for non-curated paths: title and venue are ignored.
    """
    if not items:
        return items

    seen: dict[str, int] = {}
    out: list[dict] = []
    for item in items:
        city_key = _normalise_for_similarity(str(item.get("city") or "").strip())
        country_key = _normalise_for_similarity(str(item.get("country") or "").strip())
        start_iso, end_iso = _stable_date_pair_strings(
            str(item.get("start_date") or "").strip(),
            str(item.get("end_date") or "").strip(),
        )
        if not (city_key and country_key and start_iso and end_iso):
            out.append(item)
            continue

        group_key = "|".join([country_key, city_key, start_iso, end_iso])
        existing_idx = seen.get(group_key)
        if existing_idx is None:
            seen[group_key] = len(out)
            out.append(item)
            continue

        merged = dict(out[existing_idx])
        for k, v in item.items():
            if merged.get(k) in ("", None) and v not in ("", None):
                merged[k] = v
        out[existing_idx] = merged

    return out


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
    start_raw = str(item.get("start_date") or "").strip()
    end_raw = str(item.get("end_date") or "").strip()
    s = _parse_date(start_raw)
    e = _parse_date(end_raw) if end_raw else None
    if s:
        score += 2
    if e:
        score += 2
    if s and e and e >= s:
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


async def _lookup_venue_address_async(
    *,
    client: AsyncOpenAI,
    venue: str,
    city: str,
    country: str,
    source_url: str,
    use_web_search_tool: bool,
) -> dict[str, str] | None:
    """
    Best-effort venue address lookup.
    Returns: {address, source_url}
    """
    raw_key = "|".join([venue or "", city or "", country or "", source_url or ""])
    key = _normalise_for_dedupe(raw_key)
    if key and key in _TEMP_VENUE_ADDRESS_CACHE:
        return _TEMP_VENUE_ADDRESS_CACHE.get(key)

    tools = [{"type": "web_search"}] if use_web_search_tool else None
    prompt = (
        "Find the full postal address for this museum/gallery venue.\n"
        "Return ONLY JSON with keys: address, source_url.\n"
        "Rules:\n"
        "- Prefer the official venue website contact/visit page.\n"
        "- If not available, use a reliable map or institutional listing.\n"
        "- address should be the fullest postal address you can verify.\n"
        "- If you cannot verify a full postal address, return the best venue location string you can verify for publication.\n"
        "- If nothing reliable is found, return empty strings.\n\n"
        f"Venue: {venue}\n"
        f"City: {city}\n"
        f"Country: {country}\n"
        f"Known source_url (may help): {source_url}\n"
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=700,
            ),
            max_attempts=3,
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            return None
        address = str(data.get("address") or "").strip()
        src = str(data.get("source_url") or "").strip()
        if not address:
            return None
        out = {"address": address, "source_url": src}
        if key:
            _TEMP_VENUE_ADDRESS_CACHE[key] = out
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_venue_address_lookup_error venue=%s city=%s country=%s err=%r",
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


async def _lookup_exhibition_duration_async(
    *,
    client: AsyncOpenAI,
    title: str,
    venue: str,
    city: str,
    country: str,
    source_url: str,
    use_web_search_tool: bool,
) -> dict[str, str] | None:
    """
    Best-effort lookup of an explicitly stated visit duration for an exhibition/venue.
    Returns keys: duration, source_url.
    """
    raw_key = "|".join([title or "", venue or "", city or "", country or "", source_url or ""])
    key = _normalise_for_dedupe(raw_key)
    if key and key in _TEMP_DURATION_CACHE:
        return _TEMP_DURATION_CACHE.get(key)

    tools = [{"type": "web_search"}] if use_web_search_tool else None
    prompt = (
        "Find the visit duration for this exhibition (or the hosting venue's suggested time to visit if the exhibition duration is not stated).\n"
        "Use official venue pages or clearly reliable sources.\n"
        "Return ONLY JSON with keys: duration, source_url.\n"
        "Rules:\n"
        "- duration must be an explicitly stated estimate from the source (e.g. '60 minutes', '90 minutes', '1 hour', '1.5 hours', '2 hours').\n"
        "- If you cannot find an explicit duration, return an empty string for duration.\n"
        "- source_url must be the page where the duration is stated (or empty string if duration is empty).\n"
        "- Return no prose.\n\n"
        f"Exhibition title: {title}\n"
        f"Venue: {venue}\n"
        f"City: {city}\n"
        f"Country: {country}\n"
        f"Known source_url (may help): {source_url}\n"
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
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            return None
        duration = str(data.get("duration") or "").strip()
        src = str(data.get("source_url") or "").strip()
        out = {"duration": duration, "source_url": src}
        if key:
            _TEMP_DURATION_CACHE[key] = out
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_duration_lookup_error title=%s venue=%s city=%s country=%s err=%r",
            title,
            venue,
            city,
            country,
            exc,
        )
        return None


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
    title: str,
    venue_for_copy: str,
    short: str,
    long_html: str,
    rating: int | None = None,
    address: str = "",
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
    else:
        title_head = (title or "").split(":", 1)[0].strip()
        title_norm = _normalise_for_match(title_head)
        short_norm = _normalise_for_match(short_clean)
        if title_norm and short_norm and title_norm in short_norm:
            violations.append("short repeats the exhibition title")

    if not long_clean:
        violations.append("long is empty")
        return violations

    if rating is not None:
        try:
            r = int(rating)
        except Exception:
            r = 0
        if r not in (1, 2, 3, 4, 5):
            violations.append("rating must be 1, 2, 3, 4, or 5")

    if not long_clean.startswith("<p>") or "</p>" not in long_clean:
        violations.append("long must be HTML paragraphs wrapped in <p> tags")

    # Avoid em/en dashes; hyphens are allowed for compounds like "16th-century".
    # Allow em/en dashes ONLY when they are part of the official exhibition title or venue name.
    if "—" in combined_text or "–" in combined_text:
        text_no_html = _strip_html(long_clean)
        scrubbed = " ".join([short_clean, text_no_html])
        for allowed in [title, venue_for_copy]:
            a = (allowed or "").strip()
            if not a:
                continue
            scrubbed = re.sub(re.escape(a), " ", scrubbed, flags=re.IGNORECASE)
        if "—" in scrubbed or "–" in scrubbed:
            violations.append("copy contains dash characters (—/–) outside the title/venue name")

    if _contains_source_citations(long_clean):
        violations.append("long contains source citations/URLs")

    wc = _word_count_html(long_clean)
    if wc < 280 or wc > 420:
        violations.append(f"long word count should be near 350 (got {wc})")

    lead = _strip_html(long_clean)[:80].lower()
    if lead.startswith("explore "):
        violations.append("long starts with 'Explore' (vary the opening)")
    title_head = (title or "").split(":", 1)[0].strip().lower()
    if title_head and lead.startswith(title_head):
        violations.append("long starts with exhibition name")
    venue_head = (venue_for_copy or "").strip().lower()
    if venue_head and lead.startswith(venue_head):
        violations.append("long starts with venue name")
    if lead.startswith("this exhibition"):
        violations.append("long starts with 'this exhibition'")
    if re.match(r"^(across|through|inside)\\b", lead):
        violations.append("long starts with a formula opener (Across/Through/Inside)")
    if lead.startswith("at the "):
        violations.append("long starts with a formula opener (At The ...)")
    if lead.startswith("in this show"):
        violations.append("long starts with a formula opener ('In this show')")

    # Avoid openings that prioritise the layout/structure of the exhibition.
    # Only enforce on the opening (not the full body), and ignore matches inside title/venue strings.
    opening_slice = _strip_html(long_clean)[:240]
    scrubbed_opening = opening_slice
    for allowed in [title, venue_for_copy]:
        a = (allowed or "").strip()
        if not a:
            continue
        scrubbed_opening = re.sub(re.escape(a), " ", scrubbed_opening, flags=re.IGNORECASE)
    opening_forbidden = [
        "room",
        "rooms",
        "gallery",
        "galleries",
        "space",
        "spaces",
        "display",
        "displays",
        "label",
        "labels",
        "layout",
    ]
    if any(
        re.search(r"\b" + re.escape(w) + r"\b", scrubbed_opening, flags=re.IGNORECASE)
        for w in opening_forbidden
    ):
        violations.append("long opening mentions rooms/spaces/galleries/displays/labels/layout (disallowed)")

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
    low = combined_low
    for w in banned:
        if re.search(r"\b" + re.escape(w) + r"\b", low):
            violations.append(f"long uses banned word '{w}'")
            break

    # Short opening strategy: avoid layout/rooms/galleries/sequence framing.
    short_forbidden = [
        "room",
        "rooms",
        "gallery",
        "galleries",
        "space",
        "spaces",
        "display",
        "displays",
        "layout",
        "organisation",
        "organization",
        "label",
        "labels",
        "panel",
        "panels",
        "walkthrough",
        "first",
        "next",
        "final",
    ]
    if any(
        re.search(r"\b" + re.escape(w) + r"\b", short_clean, flags=re.IGNORECASE)
        for w in short_forbidden
    ):
        violations.append("short uses a layout/rooms/galleries/sequence framing word (disallowed)")

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


def _is_svg_image_url(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return False
    try:
        path = (urlparse(u).path or "").lower()
    except Exception:
        path = u
    if path.endswith(".svg") or ".svg?" in u:
        return True
    return False


def _is_icon_like_image_url(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return False
    try:
        parsed = urlparse(u)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        host = ""
        path = u
    if "google.com" in host and "/s2/favicons" in path:
        return True
    icon_markers = ("favicon", "apple-touch-icon", "mask-icon", "/icon")
    return any(marker in path for marker in icon_markers)


def _is_small_image_url(url: str, *, min_px: int = 600) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return False
    try:
        parsed = urlparse(u)
        path = unquote(parsed.path or "").lower()
        query = parse_qs(parsed.query or "")
    except Exception:
        path = u
        query = {}

    for m in re.finditer(r"(\d{2,4})x(\d{2,4})", path):
        try:
            w = int(m.group(1))
            h = int(m.group(2))
            if w < min_px or h < min_px:
                return True
        except Exception:
            continue

    for k in ("w", "width", "h", "height"):
        vals = query.get(k) or []
        for v in vals:
            try:
                if int(str(v).strip()) < min_px:
                    return True
            except Exception:
                continue
    return False


def _wp_thumbnail_to_original(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        parsed = urlparse(u)
        path = parsed.path or ""
        # Typical WP resized assets: image-300x200.jpg -> image.jpg
        orig_path = re.sub(r"-\d{2,4}x\d{2,4}(?=\.[a-z0-9]{3,5}$)", "", path, flags=re.I)
        if orig_path == path:
            return ""
        return parsed._replace(path=orig_path).geturl()
    except Exception:
        return ""


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc or ""
    except Exception:
        return ""


def _is_image_url_ok(url: str) -> bool:
    if not url:
        return False
    if _is_svg_image_url(url):
        return False
    if _is_icon_like_image_url(url):
        return False
    if _is_small_image_url(url):
        return False
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.head(url, timeout=8, allow_redirects=True, headers=headers)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "image/svg+xml" in ct:
            return False
        if r.status_code == 200 and ct.startswith("image/"):
            return True
    except Exception:
        pass
    try:
        r = requests.get(url, timeout=10, allow_redirects=True, headers=headers, stream=True)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "image/svg+xml" in ct:
            return False
        if r.status_code == 200 and ct.startswith("image/"):
            return True
    except Exception:
        return False
    return False


def _is_image_url_ok_relaxed(url: str) -> bool:
    if not url:
        return False
    if _is_svg_image_url(url):
        return False
    if _is_icon_like_image_url(url):
        return False
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.head(url, timeout=8, allow_redirects=True, headers=headers)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "image/svg+xml" in ct:
            return False
        if r.status_code == 200 and ct.startswith("image/"):
            return True
    except Exception:
        pass
    try:
        r = requests.get(url, timeout=10, allow_redirects=True, headers=headers, stream=True)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "image/svg+xml" in ct:
            return False
        if r.status_code == 200 and ct.startswith("image/"):
            return True
    except Exception:
        return False
    return False


async def _find_relaxed_venue_image_meta_async(
    *,
    client: AsyncOpenAI | None,
    venue: str,
    city: str,
    country: str,
    source_url: str,
    homepage: str,
) -> dict[str, str] | None:
    candidates: list[tuple[str, str]] = []
    page_a = source_url or ""
    page_b = homepage or ""

    if page_a:
        html_a = await asyncio.to_thread(_fetch_url_text, page_a)
        candidates.append((_normalise_http_url(_extract_meta_image_url(html_a), base_url=page_a), page_a))
    if page_b and page_b != page_a:
        html_b = await asyncio.to_thread(_fetch_url_text, page_b)
        candidates.append((_normalise_http_url(_extract_meta_image_url(html_b), base_url=page_b), page_b))

    if client is not None:
        use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
        tools = [{"type": "web_search"}] if use_web_search_tool else None
        prompt = (
            "Find any usable image URL for this venue (exhibition visual preferred, venue photo otherwise).\n"
            "Return ONLY JSON with keys: image_url, page_url.\n"
            "Rules:\n"
            "- Hard reject favicon/logo/icon assets.\n"
            "- Direct image URL preferred.\n"
            "- If only a smaller image is available, it is acceptable as a last resort.\n"
            "- If no usable image exists, return empty strings.\n\n"
            f"Venue: {venue}\nCity: {city}\nCountry: {country}\n"
        )
        try:
            resp = await _call_with_backoff(
                lambda: client.responses.create(
                    model=TEMP_SEARCH_MODEL,
                    input=prompt,
                    tools=tools,
                    max_output_tokens=400,
                ),
                max_attempts=2,
            )
            content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
            data = _extract_json_object(content) if content else None
            if isinstance(data, dict):
                img = _normalise_http_url(str(data.get("image_url") or ""), base_url="")
                page = _normalise_http_url(str(data.get("page_url") or ""), base_url="")
                candidates.append((img, page))
        except Exception:
            pass

    seen: set[str] = set()
    for img, page in candidates:
        url = (img or "").strip()
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        wp_original = _wp_thumbnail_to_original(url)
        if wp_original and _is_image_url_ok_relaxed(wp_original):
            return {
                "image_url": wp_original,
                "page_url": page or "",
                "license": "",
                "license_url": "",
                "credit": "",
                "rights": "Venue fallback image (quality checks relaxed)",
                "mode": "venue_relaxed_fallback",
            }
        if _is_image_url_ok_relaxed(url):
            return {
                "image_url": url,
                "page_url": page or "",
                "license": "",
                "license_url": "",
                "credit": "",
                "rights": "Venue fallback image (quality checks relaxed)",
                "mode": "venue_relaxed_fallback",
            }
    return None


def _commons_image_url_from_page(page_url: str) -> str:
    try:
        parsed = urlparse(page_url)
    except Exception:
        return ""
    if "commons.wikimedia.org" not in (parsed.netloc or ""):
        return ""
    title = ""
    if parsed.path.startswith("/wiki/"):
        title = parsed.path.split("/wiki/", 1)[1]
    elif parsed.path.endswith("/w/index.php"):
        qs = parse_qs(parsed.query or "")
        title = (qs.get("title") or [""])[0]
    if not title:
        return ""
    title = unquote(title)
    if not title.startswith("File:"):
        return ""
    api_url = (
        "https://commons.wikimedia.org/w/api.php"
        f"?action=query&titles={quote(title)}&prop=imageinfo&iiprop=url&format=json"
    )
    try:
        resp = requests.get(api_url, timeout=10)
        if resp.status_code != 200:
            return ""
        data = resp.json()
        pages = (data.get("query") or {}).get("pages") or {}
        for _k, v in pages.items():
            info = (v.get("imageinfo") or [])
            if info and isinstance(info, list):
                url = (info[0] or {}).get("url") or ""
                return str(url).strip()
    except Exception:
        return ""
    return ""


def _maybe_fix_image_meta(meta: dict[str, str]) -> dict[str, str]:
    if not meta or not isinstance(meta, dict):
        return meta
    img = (meta.get("image_url") or "").strip()
    page = (meta.get("page_url") or "").strip()
    if not img:
        return meta
    # If image URL is a Commons file page, treat it as the page URL.
    if "commons.wikimedia.org/wiki/File:" in img and not page:
        page = img
        meta["page_url"] = page
    # If this looks like a WordPress thumbnail, try the original-sized path first.
    wp_original = _wp_thumbnail_to_original(img)
    if wp_original and _is_image_url_ok(wp_original):
        meta["image_url"] = wp_original
        return meta
    if _is_image_url_ok(img):
        return meta
    # Try to resolve Commons file page -> direct upload URL.
    if page and "commons.wikimedia.org" in page:
        resolved = _commons_image_url_from_page(page)
        if resolved and _is_image_url_ok(resolved):
            meta["image_url"] = resolved
            return meta
    # If still invalid, fall back to placeholder.
    meta["image_url"] = str(getattr(settings, "TEMP_IMAGE_FALLBACK_URL", "") or "").strip()
    meta["mode"] = "fallback"
    return meta


def _is_placeholder_image_url(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return True
    fallback = str(getattr(settings, "TEMP_IMAGE_FALLBACK_URL", "") or "").strip().lower()
    if fallback and u == fallback:
        return True
    if "placehold.co" in u and "divento" in u:
        return True
    return False


def _google_favicon_url(domain: str, *, size: int) -> str:
    d = (domain or "").strip()
    if not d:
        return ""
    # Returns an image for most domains; used as last-resort to guarantee non-empty URLs.
    return f"https://www.google.com/s2/favicons?domain={d}&sz={size}"


def _csv_to_set(value: str) -> set[str]:
    raw = (value or "").strip()
    if not raw:
        return set()
    parts = [p.strip().lower() for p in raw.split(",")]
    return {p for p in parts if p}


def _domain_matches_any(domain: str, allowed: set[str]) -> bool:
    d = (domain or "").strip().lower()
    if not d or not allowed:
        return False
    for a in allowed:
        a = a.strip().lower()
        if not a:
            continue
        if d == a or d.endswith("." + a):
            return True
    return False


def _license_keyword_ok(license_text: str, allowed_keywords: set[str]) -> bool:
    t = (license_text or "").strip().lower()
    if not t:
        return False
    # Normalise common punctuation variants.
    t = t.replace("creative commons", "cc")
    t = re.sub(r"\s+", " ", t)
    for kw in allowed_keywords:
        if kw and kw in t:
            return True
    return False


def _page_mentions_license(text: str, allowed_keywords: set[str]) -> bool:
    if not text or not allowed_keywords:
        return False
    t = _strip_html(text).lower()
    t = t.replace("creative commons", "cc")
    t = re.sub(r"\s+", " ", t)
    return any(kw in t for kw in allowed_keywords if kw)


def _same_site_domain(url_a: str, url_b: str) -> bool:
    da = _domain_from_url(url_a).lower()
    db = _domain_from_url(url_b).lower()
    if not da or not db:
        return False
    return da == db or da.endswith("." + db) or db.endswith("." + da)


def _clean_rights_text(value: str) -> str:
    text = _strip_html(value or "")
    text = re.sub(r"\s+", " ", text).strip(" |;:,.-")
    text = re.sub(r"[\"'<>]+$", "", text).strip(" |;:,.-")
    text = re.sub(r"^©\s*photo\b", "© photo", text, flags=re.I)
    if len(text) > 180:
        text = text[:180].rsplit(" ", 1)[0].strip()
    return text


def _normalise_compact_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _image_name_tokens(image_url: str) -> set[str]:
    try:
        raw_name = unquote(Path(urlparse(image_url).path).name or "")
    except Exception:
        raw_name = ""
    if not raw_name:
        return set()
    no_ext = re.sub(r"\.[a-z0-9]{2,5}$", "", raw_name, flags=re.I)
    no_ext = re.sub(r"([a-z])([A-Z])", r"\1 \2", no_ext)
    parts = [p for p in re.split(r"[^a-zA-Z0-9]+", no_ext.lower()) if p]
    stop = {
        "image",
        "img",
        "photo",
        "visuel",
        "exposition",
        "exhibitions",
        "museum",
        "musee",
        "gallery",
        "paris",
        "france",
        "grandpalais",
        "header",
        "hero",
        "banner",
        "default",
        "cover",
        "thumbnail",
        "thumb",
        "small",
        "large",
        "desktop",
        "mobile",
        "webp",
        "jpeg",
        "jpg",
        "png",
    }
    return {p for p in parts if len(p) >= 4 and p not in stop and not p.isdigit()}


def _pick_best_credit(candidates: list[str], image_url: str) -> str:
    if not candidates:
        return ""
    tokens = _image_name_tokens(image_url)
    best = ""
    best_score = -1
    seen: set[str] = set()
    for raw in candidates:
        cand = _clean_rights_text(raw)
        if not cand:
            continue
        key = _normalise_for_dedupe(cand)
        if not key or key in seen:
            continue
        seen.add(key)
        score = 0
        compact = _normalise_compact_token(cand)
        lower = cand.lower()
        if "©" in cand or "&copy;" in lower or "copyright" in lower:
            score += 2
        if "photo" in lower or "credit" in lower:
            score += 1
        for token in tokens:
            if token and token in compact:
                score += 3
        if len(cand) <= 120:
            score += 1
        if score > best_score:
            best_score = score
            best = cand
    return best


def _extract_legal_links_from_html(
    html: str, *, base_url: str, max_links: int
) -> list[str]:
    if not html or max_links <= 0 or not base_url:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return []
    keywords = (
        "copyright",
        "rights",
        "legal",
        "terms",
        "conditions",
        "mentions-legales",
        "mentions_legales",
        "credits",
        "credit",
        "license",
        "licence",
        "reuse",
        "impressum",
        "imprint",
    )
    out: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a"):
        href = _normalise_http_url(str(a.get("href") or ""), base_url=base_url)
        if not href:
            continue
        if not _same_site_domain(href, base_url):
            continue
        label = " ".join(
            [
                str(a.get_text(" ", strip=True) or "").lower(),
                href.lower(),
            ]
        )
        if not any(k in label for k in keywords):
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append(href)
        if len(out) >= max_links:
            break
    return out


def _extract_rights_from_image_headers(image_url: str) -> dict[str, str]:
    out = {"rights": "", "license": "", "license_url": "", "credit": ""}
    if not image_url:
        return out
    headers = {"User-Agent": "Mozilla/5.0"}
    response_headers: dict[str, str] = {}
    try:
        resp = requests.head(image_url, timeout=8, allow_redirects=True, headers=headers)
        response_headers = dict(resp.headers or {})
    except Exception:
        response_headers = {}
    if not response_headers:
        try:
            with requests.get(
                image_url,
                timeout=10,
                allow_redirects=True,
                headers=headers,
                stream=True,
            ) as resp:
                response_headers = dict(resp.headers or {})
        except Exception:
            response_headers = {}
    if not response_headers:
        return out
    for key, value in response_headers.items():
        k = (key or "").lower()
        v = _clean_rights_text(str(value or ""))
        if not v:
            continue
        if any(x in k for x in ("copyright", "rights")) and not out["rights"]:
            out["rights"] = v
            continue
        if any(x in k for x in ("license", "licence")):
            if v.lower().startswith(("http://", "https://")) and not out["license_url"]:
                out["license_url"] = v
            elif not out["license"]:
                out["license"] = v
            continue
        if any(x in k for x in ("credit", "creator", "author", "byline")) and not out["credit"]:
            out["credit"] = v
    return out


def _extract_rights_from_html(
    html: str, *, base_url: str = "", image_url: str = ""
) -> dict[str, str]:
    out = {"rights": "", "license": "", "license_url": "", "credit": ""}
    if not html:
        return out
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        soup = None

    rights_candidates: list[str] = []
    license_candidates: list[str] = []
    credit_candidates: list[str] = []

    if soup is not None:
        for tag in soup.find_all("meta"):
            key = (
                str(tag.get("name") or tag.get("property") or tag.get("itemprop") or "")
                .strip()
                .lower()
            )
            content = _clean_rights_text(str(tag.get("content") or ""))
            if not key or not content:
                continue
            if "license" in key or "licence" in key:
                if content.lower().startswith(("http://", "https://")) and not out["license_url"]:
                    out["license_url"] = content
                else:
                    license_candidates.append(content)
            if "copyright" in key or "rights" in key:
                rights_candidates.append(content)
                credit_candidates.append(content)
            if "credit" in key or "credits" in key:
                credit_candidates.append(content)

        for link in soup.find_all(["a", "link"]):
            href = _normalise_http_url(str(link.get("href") or ""), base_url=base_url)
            if not href:
                continue
            rel = " ".join([str(x).lower() for x in (link.get("rel") or [])])
            if ("license" in rel or "licence" in rel) and not out["license_url"]:
                out["license_url"] = href
            if "creativecommons.org/licenses/" in href.lower() and not out["license_url"]:
                out["license_url"] = href

        def _json_to_text(value) -> str:
            if isinstance(value, str):
                return _clean_rights_text(value)
            if isinstance(value, dict):
                for k in ("name", "text", "title", "url", "@id"):
                    if k in value and isinstance(value.get(k), str):
                        txt = _clean_rights_text(value.get(k) or "")
                        if txt:
                            return txt
                return ""
            if isinstance(value, list):
                bits = [_json_to_text(v) for v in value[:5]]
                bits = [b for b in bits if b]
                return " | ".join(bits[:3]).strip()
            return ""

        def _walk_json(node) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    lk = str(k).strip().lower()
                    if lk in ("license", "licence"):
                        txt = _json_to_text(v)
                        if txt:
                            if txt.lower().startswith(("http://", "https://")) and not out["license_url"]:
                                out["license_url"] = txt
                            else:
                                license_candidates.append(txt)
                    elif lk in (
                        "copyrightnotice",
                        "copyright",
                        "copyrightholder",
                        "copyrightyear",
                        "rights",
                    ):
                        txt = _json_to_text(v)
                        if txt:
                            rights_candidates.append(txt)
                    elif lk in ("credittext", "credit", "creator", "author", "photographer"):
                        txt = _json_to_text(v)
                        if txt:
                            credit_candidates.append(txt)
                    _walk_json(v)
            elif isinstance(node, list):
                for v in node:
                    _walk_json(v)

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = (script.string or script.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            _walk_json(data)

    plain = _strip_html(html)
    if plain:
        m_license = re.search(
            r"(Creative\s+Commons[^.;,\n]{0,80}|\bCC(?:[\s\-]?(?:BY(?:[\s\-]?(?:SA|NC|ND))?|0))\b(?:\s+[0-9](?:\.[0-9])?)?|Public\s+Domain)",
            plain,
            flags=re.I,
        )
        if m_license:
            license_candidates.append(_clean_rights_text(m_license.group(0)))
        m_rights = re.search(r"\b(All rights reserved|Some rights reserved)\b", plain, flags=re.I)
        if m_rights:
            rights_candidates.append(_clean_rights_text(m_rights.group(0)))

    for m in re.finditer(r"(?:©|&copy;)\s*[^<\n\r]{2,160}", html, flags=re.I):
        credit_candidates.append(_clean_rights_text(m.group(0)))

    if image_url:
        try:
            image_name = unquote(Path(urlparse(image_url).path).name or "")
        except Exception:
            image_name = ""
        if image_name:
            idx = html.lower().find(image_name.lower())
            if idx >= 0:
                window = html[max(0, idx - 3000) : idx + 3000]
                for m in re.finditer(r"(?:©|&copy;)\s*[^<\n\r]{2,160}", window, flags=re.I):
                    credit_candidates.append(_clean_rights_text(m.group(0)))
                m_window_rights = re.search(
                    r"\b(All rights reserved|Some rights reserved)\b", window, flags=re.I
                )
                if m_window_rights:
                    rights_candidates.append(_clean_rights_text(m_window_rights.group(0)))

    m_cc_url = re.search(
        r"https?://creativecommons\.org/licenses/[^\s\"'<>]+",
        html,
        flags=re.I,
    )
    if m_cc_url and not out["license_url"]:
        out["license_url"] = _normalise_http_url(m_cc_url.group(0), base_url=base_url)

    if not out["license"]:
        for cand in license_candidates:
            c = _clean_rights_text(cand)
            if not c:
                continue
            if re.search(r"(creative\s+commons|^cc|public\s+domain)", c, flags=re.I):
                out["license"] = c
                break
        if not out["license"] and license_candidates:
            out["license"] = _clean_rights_text(license_candidates[0])

    if not out["rights"]:
        for cand in rights_candidates:
            c = _clean_rights_text(cand)
            if not c:
                continue
            if re.search(r"\b(all rights reserved|some rights reserved)\b", c, flags=re.I):
                out["rights"] = c
                break
        if not out["rights"] and rights_candidates:
            out["rights"] = _clean_rights_text(rights_candidates[0])

    if not out["credit"]:
        out["credit"] = _pick_best_credit(credit_candidates, image_url)

    if out["license_url"]:
        out["license_url"] = _normalise_http_url(out["license_url"], base_url=base_url)
    return out


def _merge_image_rights_fields(
    base_meta: dict[str, str], extra_meta: dict[str, str] | None
) -> dict[str, str]:
    out = dict(base_meta or {})
    extra = extra_meta or {}
    for key in ("credit", "license", "license_url", "rights"):
        incoming = _clean_rights_text(str(extra.get(key) or ""))
        if not incoming:
            continue
        existing = str(out.get(key) or "").strip()
        if key == "rights" and existing.lower() == "rights unknown":
            out[key] = incoming
            continue
        if not existing:
            out[key] = incoming
    if not (out.get("page_url") or "").strip():
        page = _normalise_http_url(str(extra.get("page_url") or ""), base_url="")
        if page:
            out["page_url"] = page
    return out


async def _search_image_rights_with_openai_async(
    *,
    client: AsyncOpenAI,
    venue: str,
    city: str,
    country: str,
    page_url: str,
    image_url: str,
    use_web_search_tool: bool,
) -> dict[str, str] | None:
    tools = [{"type": "web_search"}] if use_web_search_tool else None
    image_name = ""
    try:
        image_name = unquote(Path(urlparse(image_url).path).name or "")
    except Exception:
        image_name = ""
    prompt = (
        "Find copyright/licensing attribution details for the image used on this venue page.\n"
        "Return ONLY JSON with keys: rights, credit, license, license_url, source_url.\n"
        "Rules:\n"
        "- Use explicit statements only; do not infer or invent.\n"
        "- Prefer official venue pages/legal pages/image pages.\n"
        "- If a field is unknown, return an empty string for that field.\n\n"
        f"Venue: {venue}\n"
        f"City: {city}\n"
        f"Country: {country}\n"
        f"Venue page URL: {page_url}\n"
        f"Image URL: {image_url}\n"
        f"Image filename hint: {image_name}\n"
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=700,
            ),
            max_attempts=3,
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            return None
        source = _normalise_http_url(str(data.get("source_url") or ""), base_url=page_url)
        if not source:
            return None
        src_domain = _domain_from_url(source)
        allowed = _csv_to_set(getattr(settings, "TEMP_IMAGE_ALLOWED_SOURCE_DOMAINS", "") or "")
        allowed.update(_csv_to_set(getattr(settings, "TEMP_IMAGE_OPEN_ACCESS_DOMAINS", "") or ""))
        if not _same_site_domain(source, page_url) and (
            not allowed or not _domain_matches_any(src_domain, allowed)
        ):
            return None
        out = {
            "rights": _clean_rights_text(str(data.get("rights") or "")),
            "credit": _clean_rights_text(str(data.get("credit") or "")),
            "license": _clean_rights_text(str(data.get("license") or "")),
            "license_url": _normalise_http_url(str(data.get("license_url") or ""), base_url=source),
            "page_url": source,
        }
        if not out["rights"] and not out["license"] and not out["credit"]:
            return None
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_image_rights_search_error venue=%s city=%s country=%s err=%r",
            venue,
            city,
            country,
            exc,
        )
        return None


async def _enrich_image_rights_meta_async(
    *,
    meta: dict[str, str],
    venue: str,
    city: str,
    country: str,
    source_url: str,
) -> dict[str, str]:
    out = dict(meta or {})
    image_url = (out.get("image_url") or "").strip()
    if not image_url:
        return out
    page_url = _normalise_http_url((out.get("page_url") or "").strip(), base_url=source_url)
    if not page_url:
        page_url = _normalise_http_url(source_url, base_url="")
    if not page_url:
        return out
    out["page_url"] = page_url

    page_html = await asyncio.to_thread(_fetch_url_text, page_url)
    if page_html:
        found = _extract_rights_from_html(page_html, base_url=page_url, image_url=image_url)
        out = _merge_image_rights_fields(out, found)

    need_more = not (out.get("rights") or out.get("license"))
    if need_more:
        header_found = await asyncio.to_thread(_extract_rights_from_image_headers, image_url)
        out = _merge_image_rights_fields(out, header_found)

    need_more = not (out.get("rights") or out.get("license"))
    if need_more and page_html:
        max_links = max(0, int(getattr(settings, "TEMP_IMAGE_RIGHTS_LEGAL_LINK_MAX", 3) or 0))
        links = _extract_legal_links_from_html(page_html, base_url=page_url, max_links=max_links)
        for link in links:
            legal_html = await asyncio.to_thread(_fetch_url_text, link)
            if not legal_html:
                continue
            found = _extract_rights_from_html(legal_html, base_url=link, image_url=image_url)
            if found.get("license") and not found.get("license_url"):
                found["license_url"] = link
            found["page_url"] = link
            out = _merge_image_rights_fields(out, found)
            if out.get("rights") or out.get("license"):
                break

    need_more = not (out.get("rights") or out.get("license"))
    web_search_enabled = (
        int(getattr(settings, "TEMP_IMAGE_RIGHTS_WEB_SEARCH_ENABLED", 1) or 0) != 0
    )
    if need_more and web_search_enabled:
        client = _get_openai_client()
        if client is not None:
            use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
            found = await _search_image_rights_with_openai_async(
                client=client,
                venue=venue,
                city=city,
                country=country,
                page_url=page_url,
                image_url=image_url,
                use_web_search_tool=use_web_search_tool,
            )
            out = _merge_image_rights_fields(out, found)

    return out


def _format_image_legend(meta: dict[str, str] | None) -> str:
    if not isinstance(meta, dict):
        return ""
    img = (meta.get("image_url") or "").strip()
    if not img:
        return ""
    if (meta.get("mode") or "").strip().lower() in ("fallback", "placeholder"):
        return "Placeholder image"

    def _no_commas_text(value: str) -> str:
        s = (value or "").replace(",", " ")
        return re.sub(r"\s{2,}", " ", s).strip()

    def _no_commas_url(value: str) -> str:
        # Avoid commas because downstream CSV-style parsing may treat them as separators.
        # Keep the URL valid by percent-encoding commas.
        return (value or "").strip().replace(",", "%2C")

    parts: list[str] = []
    credit = _no_commas_text(meta.get("credit") or "")
    if credit:
        parts.append(credit)
    lic = _no_commas_text(meta.get("license") or "")
    if lic:
        parts.append(f"License: {lic}")
    rights = (meta.get("rights") or "").strip()
    if rights:
        parts.append(_no_commas_text(rights))
    lic_url = _no_commas_url(meta.get("license_url") or "")
    if lic_url:
        parts.append(f"License URL: {lic_url}")
    page = _no_commas_url(meta.get("page_url") or "")
    if page:
        parts.append(f"Source: {page}")
    return " | ".join(parts).strip()


async def _find_reusable_venue_image_async(
    *,
    client: AsyncOpenAI,
    venue: str,
    city: str,
    country: str,
    use_web_search_tool: bool,
    exhibition_title: str = "",
) -> dict[str, str] | None:
    """
    Best-effort search for a venue image with explicit reuse-friendly licensing signals.
    Returns dict keys: image_url, page_url, license, license_url, credit.
    """
    tools = [{"type": "web_search"}] if use_web_search_tool else None
    allowed_kw = _csv_to_set(getattr(settings, "TEMP_IMAGE_ALLOWED_LICENSE_KEYWORDS", "") or "")
    allowed_domains = _csv_to_set(
        getattr(settings, "TEMP_IMAGE_ALLOWED_SOURCE_DOMAINS", "") or ""
    )
    exhibition_line = (
        f"Exhibition title: {exhibition_title}\n" if exhibition_title else ""
    )
    prompt = (
        "Find a reusable image for this museum/gallery venue.\n"
        "Return ONLY JSON with keys: image_url, page_url, license, license_url, credit.\n"
        "Rules:\n"
        "- The image MUST be allowed for reuse under a permissive license.\n"
        "- If an exhibition title is provided, prioritise an image that clearly matches that specific exhibition.\n"
        "- Otherwise, fall back to a reusable venue image.\n"
        "- Prefer sources with explicit licensing info like Wikimedia Commons or Europeana.\n"
        "- Hard reject favicon/logo/icon assets.\n"
        "- Hard reject small images where width or height is under ~600px.\n"
        "- If you find a WordPress-style thumbnail URL (e.g. *-300x200.jpg), try the original-size version first.\n"
        "- license should be something like 'CC0', 'Public Domain', 'CC BY 4.0', 'CC BY-SA 4.0'.\n"
        "- license_url must link to the license page (or the image page section that clearly states the license).\n"
        "- credit must be a short attribution string suitable for publication when required (author/creator if shown, plus source).\n"
        "- image_url must be a direct image URL (jpg/png/webp).\n"
        "- If you cannot find a clearly reusable image, return empty strings for all keys.\n\n"
        f"{exhibition_line}"
        f"Venue: {venue}\nCity: {city}\nCountry: {country}\n"
        f"Allowed license keywords: {', '.join(sorted(allowed_kw)) or '(none)'}\n"
        f"Preferred source domains: {', '.join(sorted(allowed_domains)) or '(none)'}\n"
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
        img = _normalise_http_url(str(data.get("image_url") or ""), base_url="")
        page = _normalise_http_url(str(data.get("page_url") or ""), base_url="")
        lic = str(data.get("license") or "").strip()
        lic_url = _normalise_http_url(str(data.get("license_url") or ""), base_url=page)
        credit = str(data.get("credit") or "").strip()
        if not img or not page:
            return None
        wp_original = _wp_thumbnail_to_original(img)
        if wp_original and _is_image_url_ok(wp_original):
            img = wp_original
        if not _is_image_url_ok(img):
            return None
        domain = _domain_from_url(page)
        if allowed_domains and not _domain_matches_any(domain, allowed_domains):
            return None
        if allowed_kw and not _license_keyword_ok(lic, allowed_kw):
            return None
        return {
            "image_url": img,
            "page_url": page,
            "license": lic,
            "license_url": lic_url,
            "credit": credit,
        }
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_image_reuse_search_error venue=%s city=%s country=%s err=%r",
            venue,
            city,
            country,
            exc,
        )
        return None


async def _find_generic_venue_image_meta_async(
    *,
    client: AsyncOpenAI,
    venue: str,
    city: str,
    country: str,
    use_web_search_tool: bool,
) -> dict[str, str] | None:
    """
    Best-effort generic venue photo lookup (not exhibition-specific).
    Intended as a fallback before placeholder.
    Returns keys: image_url, page_url, license, license_url, credit, mode.
    """
    tools = [{"type": "web_search"}] if use_web_search_tool else None
    prompt = (
        "Find a photo image URL of the venue building/interior for this museum/gallery.\n"
        "Return ONLY JSON with keys: image_url, page_url, license, license_url, credit.\n"
        "Rules:\n"
        "- Prefer a currently relevant exhibition image (poster/hero/banner) for this venue when available.\n"
        "- If no suitable exhibition image is found, fall back to a venue building/interior photo.\n"
        "- Hard reject favicon/logo/icon assets.\n"
        "- Hard reject small images where width or height is under ~600px.\n"
        "- If you find a WordPress-style thumbnail URL (e.g. *-300x200.jpg), try the original-size version first.\n"
        "- Prefer official venue pages, Wikimedia Commons, or other public pages.\n"
        "- Avoid press-login pages and ticketing overlays.\n"
        "- image_url must be a direct image URL (jpg/png/webp) when possible.\n"
        "- If license/credit is unknown, leave those fields empty.\n\n"
        f"Venue: {venue}\nCity: {city}\nCountry: {country}\n"
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=700,
            ),
            max_attempts=3,
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            return None
        img = _normalise_http_url(str(data.get("image_url") or ""), base_url="")
        page = _normalise_http_url(str(data.get("page_url") or ""), base_url="")
        if not img and page:
            html_page = await asyncio.to_thread(_fetch_url_text, page)
            img = _normalise_http_url(_extract_meta_image_url(html_page), base_url=page)
            if not img:
                icon = _normalise_http_url(_extract_icon_url(html_page), base_url=page)
                if icon:
                    img = icon
        if not img:
            return None
        if not _is_image_url_ok(img):
            return None
        meta = {
            "image_url": img,
            "page_url": page,
            "license": str(data.get("license") or "").strip(),
            "license_url": _normalise_http_url(str(data.get("license_url") or ""), base_url=page),
            "credit": str(data.get("credit") or "").strip(),
            "mode": "venue_fallback",
        }
        meta = _maybe_fix_image_meta(meta)
        if not meta.get("image_url") or _is_placeholder_image_url(str(meta.get("image_url") or "")):
            return None
        return meta
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_generic_venue_image_search_error venue=%s city=%s country=%s err=%r",
            venue,
            city,
            country,
            exc,
        )
        return None


async def _find_generic_venue_image_meta_multi_async(
    *,
    client: AsyncOpenAI,
    venue: str,
    city: str,
    country: str,
    use_web_search_tool: bool,
) -> dict[str, str] | None:
    venue_variants = _dedupe_preserve_order([
        str(venue or "").strip(),
        str(venue or "").split(",", 1)[0].strip(),
        re.sub(r"\s*\(.*?\)\s*", " ", str(venue or "")).strip(),
    ])
    raw_locality_variants = [
        (str(city or "").strip(), str(country or "").strip()),
        (str(city or "").strip(), ""),
        ("", str(country or "").strip()),
    ]
    locality_variants: list[tuple[str, str]] = []
    seen_locality: set[tuple[str, str]] = set()
    for city_variant, country_variant in raw_locality_variants:
        key = (
            _normalise_for_dedupe(city_variant),
            _normalise_for_dedupe(country_variant),
        )
        if key in seen_locality:
            continue
        seen_locality.add(key)
        locality_variants.append((city_variant, country_variant))

    for venue_variant in venue_variants:
        if not venue_variant:
            continue
        for city_variant, country_variant in locality_variants:
            found = await _find_generic_venue_image_meta_async(
                client=client,
                venue=venue_variant,
                city=city_variant,
                country=country_variant,
                use_web_search_tool=use_web_search_tool,
            )
            if found and found.get("image_url") and not _is_placeholder_image_url(str(found.get("image_url") or "")):
                return found
    return None


async def _find_exhibition_specific_image_meta_async(
    *,
    client: AsyncOpenAI,
    exhibition_title: str,
    venue: str,
    city: str,
    country: str,
    source_url: str,
    use_web_search_tool: bool,
) -> dict[str, str] | None:
    tools = [{"type": "web_search"}] if use_web_search_tool else None
    prompt = (
        "Find an image for this specific exhibition (not a generic venue image unless nothing else exists).\n"
        "Return ONLY JSON with keys: image_url, page_url, license, license_url, credit.\n"
        "Rules:\n"
        "- Prioritise this exact exhibition title at the specified venue/city.\n"
        "- Prefer the official exhibition page or official museum pages first.\n"
        "- If not available, use reputable pages that clearly match this same exhibition.\n"
        "- Hard reject favicon/logo/icon assets.\n"
        "- Hard reject small images where width or height is under ~600px.\n"
        "- If you find a WordPress-style thumbnail URL (e.g. *-300x200.jpg), try the original-size version first.\n"
        "- image_url must be a direct image URL (jpg/png/webp) when possible.\n"
        "- If nothing valid is found for this exhibition, return empty strings.\n\n"
        f"Exhibition title: {exhibition_title}\n"
        f"Venue: {venue}\nCity: {city}\nCountry: {country}\n"
        f"Known source_url (may help): {source_url}\n"
    )
    try:
        resp = await _call_with_backoff(
            lambda: client.responses.create(
                model=TEMP_SEARCH_MODEL,
                input=prompt,
                tools=tools,
                max_output_tokens=700,
            ),
            max_attempts=3,
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        data = _extract_json_object(content) if content else None
        if not isinstance(data, dict):
            return None
        img = _normalise_http_url(str(data.get("image_url") or ""), base_url="")
        page = _normalise_http_url(str(data.get("page_url") or ""), base_url="")
        if not img and page:
            html_page = await asyncio.to_thread(_fetch_url_text, page)
            img = _normalise_http_url(_extract_meta_image_url(html_page), base_url=page)
        if not img:
            return None
        wp_original = _wp_thumbnail_to_original(img)
        if wp_original and _is_image_url_ok(wp_original):
            img = wp_original
        if not _is_image_url_ok(img):
            return None
        meta = {
            "image_url": img,
            "page_url": page,
            "license": str(data.get("license") or "").strip(),
            "license_url": _normalise_http_url(str(data.get("license_url") or ""), base_url=page),
            "credit": str(data.get("credit") or "").strip(),
            "mode": "exhibition_web_search",
        }
        meta = _maybe_fix_image_meta(meta)
        if not meta.get("image_url") or _is_placeholder_image_url(str(meta.get("image_url") or "")):
            return None
        return meta
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "temp_exhibition_image_search_error title=%s venue=%s city=%s country=%s err=%r",
            exhibition_title,
            venue,
            city,
            country,
            exc,
        )
        return None


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

    license_mode = str(getattr(settings, "TEMP_IMAGE_LICENSE_MODE", "strict") or "strict").strip().lower()
    strict_license = license_mode == "strict"
    soft_license = license_mode == "soft"

    # In strict mode, skip non-licensed extraction paths and only accept reusable images
    # from sources with explicit licensing signals. Otherwise fall back to the configured placeholder.
    if strict_license:
        client = _get_openai_client()
        if client is not None:
            use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
            found = await _find_reusable_venue_image_async(
                client=client,
                venue=venue,
                city=city,
                country=country,
                use_web_search_tool=use_web_search_tool,
                exhibition_title=exhibition_title,
            )
            if found and found.get("image_url"):
                img = found["image_url"]
                if cache_key:
                    _TEMP_VENUE_IMAGE_CACHE[cache_key] = img
                logger.info(
                    "temp_image_selected mode=strict venue=%s city=%s country=%s image=%s license=%s",
                    venue,
                    city,
                    country,
                    img,
                    (found.get("license") or ""),
                )
                return img
        img = str(getattr(settings, "TEMP_IMAGE_FALLBACK_URL", "") or "").strip()
        if cache_key:
            _TEMP_VENUE_IMAGE_CACHE[cache_key] = img or ""
        logger.info(
            "temp_image_selected mode=strict_fallback venue=%s city=%s country=%s image=%s",
            venue,
            city,
            country,
            img,
        )
        return img or ""

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

    # Do not keep icon-like or svg assets as the final image.
    if img and (_is_icon_like_image_url(img) or _is_svg_image_url(img)):
        img = ""

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
            # In soft mode, try to prefer reusable images, but fall back to any public image.
            if soft_license:
                found = await _find_reusable_venue_image_async(
                    client=client,
                    venue=venue,
                    city=city,
                    country=country,
                    use_web_search_tool=use_web_search_tool,
                    exhibition_title=exhibition_title,
                )
                if found and found.get("image_url"):
                    img = found["image_url"]
            if not img:
                prompt = (
                    "Find a public, non-press venue photo image URL for this museum/gallery.\n"
                    "Return ONLY JSON with keys: image_url, page_url.\n"
                    "- Do NOT return exhibition posters/banners/artwork crops when a venue photo is available.\n"
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


async def _get_venue_image_meta_async(
    *,
    venue: str,
    city: str,
    country: str,
    source_url: str,
    exhibition_title: str = "",
) -> dict[str, str]:
    """
    Return image metadata for a venue image suitable for Excel export.
    Keys: image_url, page_url, license, license_url, credit, mode.
    """
    homepage = _venue_homepage_from_url(source_url) or ""
    cache_key = _normalise_for_dedupe(
        "|".join(
            [
                "exhibition",
                exhibition_title or "",
                source_url or "",
                venue or "",
                city or "",
                country or "",
            ]
        )
    )
    if cache_key and cache_key in _TEMP_IMAGE_LICENSE_CACHE:
        cached = _TEMP_IMAGE_LICENSE_CACHE.get(cache_key) or {}
        if cached.get("image_url"):
            return dict(cached)

    license_mode = str(getattr(settings, "TEMP_IMAGE_LICENSE_MODE", "strict") or "strict").strip().lower()
    strict_license = license_mode == "strict"
    soft_license = license_mode == "soft"

    # Strict mode: only accept reusable images (with explicit license fields).
    if strict_license:
        client = _get_openai_client()
        if client is not None:
            use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
            found = await _find_reusable_venue_image_async(
                client=client,
                venue=venue,
                city=city,
                country=country,
                use_web_search_tool=use_web_search_tool,
            )
            if found and found.get("image_url"):
                meta = {
                    "image_url": found.get("image_url") or "",
                    "page_url": found.get("page_url") or "",
                    "license": found.get("license") or "",
                    "license_url": found.get("license_url") or "",
                    "credit": found.get("credit") or "",
                    "mode": "strict",
                }
                logger.info(
                    "temp_image_selected mode=strict venue=%s city=%s country=%s image=%s license=%s",
                    venue,
                    city,
                    country,
                    meta.get("image_url") or "",
                    meta.get("license") or "",
                )
                meta = _maybe_fix_image_meta(meta)
                if not _is_placeholder_image_url(str(meta.get("image_url") or "")):
                    if cache_key:
                        _TEMP_IMAGE_LICENSE_CACHE[cache_key] = dict(meta)
                        _TEMP_VENUE_IMAGE_CACHE[cache_key] = meta["image_url"]
                    return meta
            # If strict reusable search fails, try a generic venue photo before placeholder.
            generic = await _find_generic_venue_image_meta_multi_async(
                client=client,
                venue=venue,
                city=city,
                country=country,
                use_web_search_tool=use_web_search_tool,
            )
            if generic and generic.get("image_url"):
                meta = await _enrich_image_rights_meta_async(
                    meta=generic,
                    venue=venue,
                    city=city,
                    country=country,
                    source_url=source_url,
                )
                if not (meta.get("rights") or meta.get("license")):
                    meta["rights"] = "Rights unknown"
                if cache_key:
                    _TEMP_IMAGE_LICENSE_CACHE[cache_key] = dict(meta)
                    _TEMP_VENUE_IMAGE_CACHE[cache_key] = meta.get("image_url") or ""
                logger.info(
                    "temp_image_selected mode=strict_venue_fallback venue=%s city=%s country=%s image=%s",
                    venue,
                    city,
                    country,
                    meta.get("image_url") or "",
                )
                return meta
        img = str(getattr(settings, "TEMP_IMAGE_FALLBACK_URL", "") or "").strip()
        meta = {
            "image_url": img,
            "page_url": "",
            "license": "",
            "license_url": "",
            "credit": "",
            "mode": "fallback",
        }
        logger.info(
            "temp_image_selected mode=strict_fallback venue=%s city=%s country=%s image=%s",
            venue,
            city,
            country,
            img,
        )
        meta = _maybe_fix_image_meta(meta)
        if cache_key:
            _TEMP_IMAGE_LICENSE_CACHE[cache_key] = dict(meta)
            _TEMP_VENUE_IMAGE_CACHE[cache_key] = meta.get("image_url") or ""
        return meta

    # Non-strict modes: try meta tags / favicon, then OpenAI fallback.
    img = ""
    page = ""

    html = await asyncio.to_thread(_fetch_url_text, source_url)
    img = _normalise_http_url(_extract_meta_image_url(html), base_url=source_url)
    if img:
        page = source_url
    if not img:
        icon = _normalise_http_url(_extract_icon_url(html), base_url=source_url)
        if icon:
            img = icon
            page = source_url

    # 2) If exhibition page has no valid image, search for this specific exhibition first.
    if not img:
        client = _get_openai_client()
        if client is not None and (exhibition_title or source_url):
            use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
            specific = await _find_exhibition_specific_image_meta_async(
                client=client,
                exhibition_title=exhibition_title or venue or city,
                venue=venue,
                city=city,
                country=country,
                source_url=source_url,
                use_web_search_tool=use_web_search_tool,
            )
            if specific and specific.get("image_url"):
                meta = await _enrich_image_rights_meta_async(
                    meta=specific,
                    venue=venue,
                    city=city,
                    country=country,
                    source_url=source_url,
                )
                if not (meta.get("rights") or meta.get("license")):
                    meta["rights"] = "Rights unknown"
                if cache_key:
                    _TEMP_IMAGE_LICENSE_CACHE[cache_key] = dict(meta)
                    _TEMP_VENUE_IMAGE_CACHE[cache_key] = meta.get("image_url") or ""
                return meta

    # 3) Venue-level fallbacks (homepage/search) only after exhibition-specific attempts fail.
    if not img and homepage:
        html_home = await asyncio.to_thread(_fetch_url_text, homepage)
        img = _normalise_http_url(_extract_meta_image_url(html_home), base_url=homepage)
        if img:
            page = homepage
        if not img:
            icon = _normalise_http_url(_extract_icon_url(html_home), base_url=homepage)
            if icon:
                img = icon
                page = homepage

    # If we only found favicon/icon/svg assets, force deeper venue image search.
    if img and (_is_icon_like_image_url(img) or _is_svg_image_url(img)):
        img = ""
        page = ""

    if not img:
        domain = _domain_from_url(homepage or source_url)
        if domain:
            img = _google_favicon_url(
                domain, size=int(getattr(settings, "TEMP_IMAGE_FAVICON_SIZE", 256) or 256)
            )
            page = homepage or source_url

    if not img:
        client = _get_openai_client()
        if client is not None:
            use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
            tools = [{"type": "web_search"}] if use_web_search_tool else None
            if soft_license:
                found = await _find_reusable_venue_image_async(
                    client=client,
                    venue=venue,
                    city=city,
                    country=country,
                    use_web_search_tool=use_web_search_tool,
                )
                if found and found.get("image_url"):
                    meta = {
                        "image_url": found.get("image_url") or "",
                        "page_url": found.get("page_url") or "",
                        "license": found.get("license") or "",
                        "license_url": found.get("license_url") or "",
                        "credit": found.get("credit") or "",
                        "mode": "soft",
                    }
                    meta = _maybe_fix_image_meta(meta)
                    if not _is_placeholder_image_url(str(meta.get("image_url") or "")):
                        if cache_key:
                            _TEMP_IMAGE_LICENSE_CACHE[cache_key] = dict(meta)
                            _TEMP_VENUE_IMAGE_CACHE[cache_key] = meta["image_url"]
                        return meta
            prompt = (
                "Find a public, non-press venue photo image URL for this museum/gallery.\n"
                "Return ONLY JSON with keys: image_url, page_url.\n"
                "- Prefer a currently relevant exhibition image (poster/hero/banner) for this venue when available.\n"
                "- If no suitable exhibition image is found, fall back to a venue building/interior photo.\n"
                "- Hard reject favicon/logo/icon assets.\n"
                "- Hard reject small images where width or height is under ~600px.\n"
                "- If you find a WordPress-style thumbnail URL (e.g. *-300x200.jpg), try the original-size version first.\n"
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
                content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
                data = _extract_json_object(content) if content else None
                if isinstance(data, dict):
                    img = _normalise_http_url(str(data.get("image_url") or ""), base_url="")
                    page = _normalise_http_url(str(data.get("page_url") or ""), base_url="")
            except Exception:
                pass

    # Open-access domain fallback (only if page explicitly mentions reuse-friendly license).
    if img and page:
        allowed_open_access = _csv_to_set(
            getattr(settings, "TEMP_IMAGE_OPEN_ACCESS_DOMAINS", "") or ""
        )
        allowed_kw = _csv_to_set(getattr(settings, "TEMP_IMAGE_ALLOWED_LICENSE_KEYWORDS", "") or "")
        page_domain = _domain_from_url(page)
        if allowed_open_access and _domain_matches_any(page_domain, allowed_open_access):
            html_page = await asyncio.to_thread(_fetch_url_text, page)
            if _page_mentions_license(html_page, allowed_kw):
                meta = _maybe_fix_image_meta({
                    "image_url": img,
                    "page_url": page,
                    "license": "",
                    "license_url": "",
                    "credit": "",
                    "mode": "open_access",
                })
                meta = await _enrich_image_rights_meta_async(
                    meta=meta,
                    venue=venue,
                    city=city,
                    country=country,
                    source_url=source_url,
                )
                if cache_key:
                    _TEMP_IMAGE_LICENSE_CACHE[cache_key] = dict(meta)
                    _TEMP_VENUE_IMAGE_CACHE[cache_key] = meta.get("image_url") or ""
                return meta

    # Soft fallback: allow official page images but mark rights unknown.
    soft_fallback_enabled = int(getattr(settings, "TEMP_IMAGE_SOFT_FALLBACK_ENABLED", 1) or 0) != 0
    if soft_fallback_enabled and img:
        meta = _maybe_fix_image_meta({
            "image_url": img,
            "page_url": page,
            "license": "",
            "license_url": "",
            "credit": "",
            "rights": "",
            "mode": "soft_fallback",
        })
        if _is_placeholder_image_url(str(meta.get("image_url") or "")):
            generic = None
            client = _get_openai_client()
            if client is not None:
                use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
                generic = await _find_generic_venue_image_meta_multi_async(
                    client=client,
                    venue=venue,
                    city=city,
                    country=country,
                    use_web_search_tool=use_web_search_tool,
                )
            if generic and generic.get("image_url"):
                meta = generic
        meta = await _enrich_image_rights_meta_async(
            meta=meta,
            venue=venue,
            city=city,
            country=country,
            source_url=source_url,
        )
        if not (meta.get("rights") or meta.get("license")):
            meta["rights"] = "Rights unknown"
    else:
        # Before placeholder, try a generic venue photo search.
        generic = None
        client = _get_openai_client()
        if client is not None:
            use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
            generic = await _find_generic_venue_image_meta_multi_async(
                client=client,
                venue=venue,
                city=city,
                country=country,
                use_web_search_tool=use_web_search_tool,
            )
        if generic and generic.get("image_url"):
            meta = await _enrich_image_rights_meta_async(
                meta=generic,
                venue=venue,
                city=city,
                country=country,
                source_url=source_url,
            )
            if not (meta.get("rights") or meta.get("license")):
                meta["rights"] = "Rights unknown"
        else:
            img = str(getattr(settings, "TEMP_IMAGE_FALLBACK_URL", "") or "").strip()
            meta = _maybe_fix_image_meta({
                "image_url": img,
                "page_url": "",
                "license": "",
                "license_url": "",
                "credit": "",
                "mode": "fallback",
            })

    if _is_placeholder_image_url(str(meta.get("image_url") or "")):
        client = _get_openai_client()
        relaxed = await _find_relaxed_venue_image_meta_async(
            client=client,
            venue=venue,
            city=city,
            country=country,
            source_url=source_url,
            homepage=homepage,
        )
        if relaxed and relaxed.get("image_url"):
            meta = await _enrich_image_rights_meta_async(
                meta=relaxed,
                venue=venue,
                city=city,
                country=country,
                source_url=source_url,
            )
            if not (meta.get("rights") or meta.get("license")):
                meta["rights"] = "Venue fallback image (quality checks relaxed)"
            logger.info(
                "temp_image_selected mode=venue_relaxed_fallback venue=%s city=%s country=%s image=%s",
                venue,
                city,
                country,
                meta.get("image_url") or "",
            )

    if cache_key:
        _TEMP_IMAGE_LICENSE_CACHE[cache_key] = dict(meta)
        _TEMP_VENUE_IMAGE_CACHE[cache_key] = meta.get("image_url") or ""
    return meta


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
            resp = await awaitable_factory()
            _emit_openai_usage_event(resp)
            return resp
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
        "- Do not start the short description with any of these opening words/phrases: "
        + ", ".join(sorted(set(avoid_short_openers))[:12])
        + ".\n"
        if avoid_short_openers
        else ""
    )
    avoid_long_clause = (
        "- Do not start the long description with any of these opening words/phrases: "
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
        "- Keep it casual, like talking to a friend, while staying factual and concrete.\n"
        "- Use natural rhythm and occasional contractions (it's, there's, don't) to avoid robotic phrasing.\n"
        "- Make the writing flow: vary sentence length, use smooth transitions between ideas, and avoid a rigid checklist feel.\n"
        "- Never use the first person.\n"
        "- Do not address the reader directly (no second person).\n"
        "- Do not use these words anywhere: You, visitor, visitors, located, feature, featured, showcase, blend, period, accessible.\n"
        "- Do not use em/en dash characters: — or –, except if they are part of the official exhibition title or venue name.\n"
        "- Do not include citations, links, or URLs.\n"
        "- Do not mention the exhibition dates anywhere.\n"
        "- Do not mention the visit duration/time-to-spend anywhere.\n"
        "- Spell out numbers one to ten in words (no digits 1–10).\n"
        "- Do not include the venue’s postal address (no street, postcode, or 'address is …').\n"
        f"- When mentioning the venue name in the copy, always write it with a leading 'The' (capital T), matching: {venue_for_copy}.\n\n"
        "LONG (HTML)\n"
        "- Aim for 350 words.\n"
        "- Multiple paragraphs; wrap each paragraph in <p> tags.\n"
        "- Do not begin with the exhibition name, the venue name, or phrases like 'This exhibition'.\n"
        "- Do not use formula openings such as: Across…, Through…, Inside…, At The venue…, In this show…\n"
        "- Use an informal, easy to read style.\n"
        "- Avoid concluding or summary-style final sentences.\n"
        "- When a major historical figure is mentioned for the first time in the main body text, include dates immediately after the name in brackets using ASCII hyphens: Name (1840-1926) or Name (born 1929).\n"
        "- Only include those dates on the first mention of that person.\n"
        "- Do not add dates for curators, living gallery staff, or minor references unless they are historically significant.\n"
        "- Use dates mainly for artists, architects, writers, collectors, rulers, and major historical figures.\n"
        "OPENING STRATEGY (CRITICAL FOR VARIATION)\n"
        "- Each exhibition description must open using a different narrative angle.\n"
        "- Anchor the opening on why the exhibition is worth a visit.\n"
        "- Do not prioritise the lay out of the exhibition.\n"
        "- Avoid openings that mention rooms, spaces, galleries, displays, labels, or layout structure.\n"
        "- Do not reuse opening sentence structure across exhibitions.\n"
        "- Avoid physical walkthrough descriptions or layout framing.\n"
        "- Avoid openings about rooms, galleries, spaces, layout, organisation, labels/interpretation panels, or sequences such as first/next/final.\n"
        + f"{avoid_long_clause}"
        "- Include naturally: one highlight, two don't-miss elements, and one hidden gem.\n"
        "- Mention at least three specific works, artists, or items when possible.\n"
        "- Use strong verbs, concrete nouns, active voice; cut filler.\n"
        "- No brochure-style language, clichés, or exaggerated adjectives.\n\n"
        "SHORT\n"
        "- Maximum 164 characters.\n"
        "- Write one concise factual sentence that explains what the exhibition is about and why it is worth visiting.\n"
        "- It must still read naturally and include a verb.\n"
        "- Must not repeat the exhibition name.\n"
        "- Must not repeat the phrasing or start of the long description.\n\n"
        "- Include a sentence identifying the subject of the exhibition and a reason to visit.\n"
        "- The reason to visit should refer to artworks, objects, discoveries, or themes presented.\n"
        "- Do not write pure teaser copy; prioritise a compact subject-led summary with a concrete reason to go.\n"
        "- Keep it to roughly 20-25 words when possible, while always staying under 164 characters.\n"
        "- If the exhibition is about a person, include name, role, nationality if relevant, and birth-death dates when known. If living, use '(born YEAR)'.\n"
        "- If it is about an artistic movement, include the movement name and approximate period.\n"
        "- If it is about a historical period, include the timeframe.\n"
        "- If it is about a place or culture, briefly identify the location and historical context.\n"
        "- If several subjects appear, identify the primary one only.\n"
        "- Acceptable opening patterns include: subject first, subject within the opening clause, movement anchored at the start, or subject after a short opening phrase.\n"
        "- Do not prioritise the lay out of the exhibition.\n"
        "- The short description MUST NOT focus on rooms, spaces, galleries, displays, labels, or layout structure.\n"
        "- Also avoid: exhibition organisation, interpretation panels, sequences such as first/next/final, or physical walkthrough descriptions.\n"
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

        last_violations = _validate_temp_copy(
            title, venue_for_copy, short, long_html, None, address
        )
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


def _editorial_rating_caps(batch_size: int) -> tuple[int, int]:
    if batch_size <= 0:
        return 0, 0
    max_fives = 0 if batch_size < 8 else (1 if batch_size < 25 else max(1, (batch_size + 19) // 20))
    max_fours = 1 if batch_size < 6 else max(2, (batch_size + 4) // 5)
    max_fours = min(max_fours, max(0, batch_size - max_fives))
    return max_fives, max_fours


async def _assign_city_editorial_ratings_async(rows: list[dict], *, city: str) -> list[str]:
    if not rows:
        return []

    client = _get_openai_client()
    if client is None:
        return ["3"] * len(rows)

    max_fives, max_fours = _editorial_rating_caps(len(rows))
    evidence_rows: list[dict] = []
    for idx, row in enumerate(rows):
        label = str(row.get("Name of site, City") or "").strip()
        title, remainder = _split_exhibition_label(label)
        title = title or _strip_label_date_suffix(label)
        venue = _strip_label_date_suffix(remainder)
        long_excerpt = re.sub(r"\s+", " ", _strip_html(str(row.get("Long description") or ""))).strip()
        if len(long_excerpt) > 240:
            long_excerpt = long_excerpt[:237].rsplit(" ", 1)[0].strip() + "..."
        evidence_rows.append(
            {
                "id": idx,
                "title": title,
                "venue": venue,
                "short": str(row.get("Short description") or "").strip(),
                "information": str(row.get("Information") or "").strip(),
                "long_excerpt": long_excerpt,
            }
        )

    prompt = (
        f"Assign Divento editorial ratings for a batch of temporary exhibitions in {city}.\n\n"
        "Use this exact scale:\n"
        "1 = Avoid\n"
        "2 = Negligible interest\n"
        "3 = Worth a look\n"
        "4 = Worth planning for\n"
        "5 = Worth a detour\n\n"
        "Judge independently from the available evidence. Do not copy or average ratings from other websites.\n"
        "Calibration:\n"
        "- 3 = average competent exhibition\n"
        "- 4 = clearly above average\n"
        "- 5 = rare and exceptional\n"
        "- If evidence is thin or neutral, default to 3 unless there is strong reason otherwise.\n\n"
        "Distribution guidance:\n"
        "- Most exhibitions should be 3.\n"
        f"- Keep rating 4 limited to about {max_fours} exhibitions in this batch.\n"
        f"- Keep rating 5 very rare, at most about {max_fives} exhibitions in this batch.\n"
        "- Use rating 2 for occasional minor or weak exhibitions.\n"
        "- Use rating 1 only for rare clearly poor exhibitions.\n"
        "- Avoid rating inflation.\n\n"
        "Return a JSON array sorted from strongest exhibition to weakest exhibition.\n"
        "Each object must have exactly these keys: 'id' and 'rating'.\n"
        "Include every id exactly once.\n\n"
        "EXHIBITIONS JSON\n"
        f"{json.dumps(evidence_rows, ensure_ascii=False)}"
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
                        "name": "temp_city_editorial_ratings",
                        "strict": True,
                        "schema": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "id": {"type": "integer"},
                                    "rating": {"type": "integer", "enum": [1, 2, 3, 4, 5]},
                                },
                                "required": ["id", "rating"],
                            },
                        },
                    },
                },
                max_output_tokens=max(900, min(4000, 100 + len(rows) * 30)),
            )
        )
        content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
    except Exception as exc_schema:  # noqa: BLE001
        logger.debug("temp_city_rating_schema_error city=%s err=%r", city, exc_schema)
        try:
            resp = await _call_with_backoff(
                lambda: client.responses.create(
                    model=TEMP_COPY_MODEL,
                    input=prompt,
                    reasoning={"effort": "medium"},
                    text={"verbosity": "low", "format": {"type": "json_object"}},
                    max_output_tokens=max(900, min(4000, 100 + len(rows) * 30)),
                )
            )
            content = _clean_json_content(resp.output_text or _extract_response_text(resp) or "")
        except Exception as exc_obj:  # noqa: BLE001
            logger.debug("temp_city_rating_object_error city=%s err=%r", city, exc_obj)
            return ["3"] * len(rows)

    data = _extract_json_array(content) if content else None
    if not isinstance(data, list):
        logger.info("temp_city_rating_invalid_json city=%s", city)
        return ["3"] * len(rows)

    rating_by_id: dict[int, int] = {}
    ordered_ids: list[int] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("id"))
            rating = int(item.get("rating"))
        except Exception:
            continue
        if idx < 0 or idx >= len(rows) or rating not in (1, 2, 3, 4, 5) or idx in rating_by_id:
            continue
        rating_by_id[idx] = rating
        ordered_ids.append(idx)

    for idx in range(len(rows)):
        if idx not in rating_by_id:
            rating_by_id[idx] = 3
            ordered_ids.append(idx)

    final_ratings = ["3"] * len(rows)
    used_fives = 0
    used_fours = 0
    for idx in ordered_ids:
        rating = rating_by_id.get(idx, 3)
        if rating == 5:
            if used_fives < max_fives:
                final_ratings[idx] = "5"
                used_fives += 1
            elif used_fours < max_fours:
                final_ratings[idx] = "4"
                used_fours += 1
            else:
                final_ratings[idx] = "3"
        elif rating == 4:
            if used_fours < max_fours:
                final_ratings[idx] = "4"
                used_fours += 1
            else:
                final_ratings[idx] = "3"
        elif rating in (1, 2, 3):
            final_ratings[idx] = str(rating)
        else:
            final_ratings[idx] = "3"

    logger.info(
        "temp_city_ratings city=%s count=%s dist_1=%s dist_2=%s dist_3=%s dist_4=%s dist_5=%s",
        city,
        len(final_ratings),
        final_ratings.count("1"),
        final_ratings.count("2"),
        final_ratings.count("3"),
        final_ratings.count("4"),
        final_ratings.count("5"),
    )
    return final_ratings


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
        raise RuntimeError("OpenAI client unavailable for temporary exhibition search")

    hard_max = max(1, int(getattr(settings, "TEMP_HARD_MAX_EXHIBITIONS", 200) or 200))
    absolute_max_setting = int(getattr(settings, "TEMP_ABSOLUTE_MAX_EXHIBITIONS", 10) or 0)
    absolute_max = absolute_max_setting if absolute_max_setting > 0 else hard_max
    if int(target_max) <= 0 or int(target_max) > absolute_max:
        target_max = absolute_max

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
    pass_max = max(5, int(getattr(settings, "TEMP_SEARCH_PASS_MAX_ITEMS", 60) or 60))
    desired_max = int(target_max) if int(target_max) > 0 else hard_max
    desired_max = min(desired_max, absolute_max)
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

    # Venue discovery is expensive. Only run it if we still need more exhibitions after the
    # initial passes/curated phase (see deepening block below).
    venue_discovery_block = ""
    discovered_venues: list[dict[str, str]] = []

    curated_enabled = int(getattr(settings, "TEMP_CURATED_VENUES_ENABLED", 1) or 0) != 0
    curated_max_venues = int(getattr(settings, "TEMP_CURATED_VENUES_MAX_VENUES", 0) or 0)
    curated_max_items_per_venue = max(
        1, int(getattr(settings, "TEMP_CURATED_VENUES_MAX_ITEMS_PER_VENUE", 8) or 8)
    )
    curated_venues = _dedupe_preserve_order(_CITY_CURATED_VENUES.get(city_norm, []))
    if curated_max_venues > 0:
        curated_venues = curated_venues[:curated_max_venues]
    curated_only_mode = curated_enabled and bool(curated_venues)

    # Cost-control mode for non-curated cities:
    # keep the broad search to a single pass, but still allow targeted fallback recovery
    # (official-page retry + venue discovery/deepening) when results are empty or very thin.
    if not curated_only_mode:
        passes = 1
        venue_deepen_passes = max(1, min(venue_deepen_passes, 1))
        if venue_deepen_max_venues <= 0 or venue_deepen_max_venues > 12:
            venue_deepen_max_venues = 12
        venue_deepen_max_per_venue = max(1, min(venue_deepen_max_per_venue, 3))
        if venue_discovery_max <= 0 or venue_discovery_max > 18:
            venue_discovery_max = 18

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
        "Retrieval strategy:\n"
        "- Primary goal is to identify real temporary exhibitions and their official source pages.\n"
        "- It is better to return a real in-window exhibition with some ancillary fields empty than to return an empty array.\n"
        "- Prioritise these fields first: name, venue, start_date, end_date, source_url.\n"
        "- Supporting fields like address, coordinates, duration, opening pattern, and opening hours may be left empty when they are not quickly verifiable and can be backfilled later.\n\n"
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
        "- latitude and longitude: provide decimal coordinates for the venue when they are reliably available.\n"
        "- If coordinates are not quickly verifiable, return empty strings and they can be backfilled later.\n\n"
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

        had_search_failures = False
        had_successful_search_response = False
        last_search_error = ""

        async def _run_search(input_prompt: str, *, stage: str) -> tuple[str, str]:
            try:
                resp = await _call_with_backoff(
                    lambda: client.responses.create(
                        model=TEMP_SEARCH_MODEL,
                        input=input_prompt,
                        tools=tools_arg,
                        max_output_tokens=12000,
                    )
                )
                return (
                    _clean_json_content(
                        resp.output_text or _extract_response_text(resp) or ""
                    ),
                    "",
                )
            except Exception as exc_resp:  # noqa: BLE001
                nonlocal had_search_failures, last_search_error
                had_search_failures = True
                last_search_error = repr(exc_resp)
                logger.warning(
                    "temp_search_request_error city=%s stage=%s err=%r",
                    city,
                    stage,
                    exc_resp,
                )
                return "", repr(exc_resp)

        async def _run_search_with_retries(
            input_prompt: str,
            *,
            stage: str,
            max_tries: int | None = None,
        ) -> tuple[str, str, str]:
            nonlocal had_search_failures, had_successful_search_response, last_search_error
            tries = max_tries if max_tries is not None else (4 if curated_only_mode else 5)
            last_content = ""
            saw_valid_empty = False
            for attempt in range(1, max(1, tries) + 1):
                content, err = await _run_search(input_prompt, stage=f"{stage}:attempt_{attempt}")
                stripped = (content or "").strip()
                if not stripped:
                    if attempt < tries:
                        await _sleep_with_jitter(min(4.0, 0.4 * (2 ** (attempt - 1))))
                    continue
                last_content = content
                data = _extract_json_array(content)
                if data is None:
                    had_search_failures = True
                    last_search_error = f"JSON parse failed for {stage}"
                    logger.warning(
                        "temp_search_parse_retry city=%s stage=%s attempt=%s raw=%r",
                        city,
                        stage,
                        attempt,
                        (content or "")[:600],
                    )
                    if attempt < tries:
                        await _sleep_with_jitter(min(4.0, 0.4 * (2 ** (attempt - 1))))
                    continue
                had_successful_search_response = True
                if isinstance(data, list) and data:
                    return content, "ok", ""
                saw_valid_empty = True
                if attempt < tries:
                    await _sleep_with_jitter(min(3.0, 0.3 * (2 ** (attempt - 1))))
            if saw_valid_empty:
                return last_content, "empty", ""
            return last_content, "failed", last_search_error

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
            "temp_search_plan city=%s passes=%s target_min=%s target_max=%s pool_max=%s venue_deepen_passes=%s venue_deepen_max_venues=%s venue_deepen_max_per_venue=%s venue_discovery_enabled=%s curated_only_mode=%s",
            city,
            passes,
            desired_min,
            desired_max,
            pool_max,
            venue_deepen_passes,
            venue_deepen_max_venues,
            venue_deepen_max_per_venue,
            venue_discovery_enabled,
            curated_only_mode,
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
                if int(target_max) > 0 and _kept_estimate_count(combined) >= desired_max:
                    break
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
                content, run_status, run_err = await _run_search_with_retries(
                    curated_prompt,
                    stage=f"curated:{venue_name}",
                    max_tries=4,
                )
                if run_status != "ok":
                    logger.info(
                        "temp_search_curated city=%s venue=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                        city,
                        venue_name,
                        0,
                        0,
                        len(combined),
                        _kept_estimate_count(combined),
                        run_status if not run_err else f"{run_status}:{run_err[:120]}",
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

        # Base passes: stop as soon as we have enough kept items.
        # For cities with a curated venue list, we intentionally skip broad passes and rely
        # on the curated venue phase only.
        if (
            not curated_only_mode
            and (int(target_max) <= 0 or _kept_estimate_count(combined) < desired_max)
        ):
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

                    content, run_status, run_err = await _run_search_with_retries(
                        input_prompt,
                        stage=f"base_pass_{pass_idx + 1}",
                    )
                    last_raw = content or ""
                    logger.debug(
                        "temp_search_raw city=%s pass=%s chars=%s snippet=%r",
                        city,
                        pass_idx + 1,
                        len(content or ""),
                        (content or "")[:1200],
                    )
                    if run_status != "ok":
                        logger.info(
                            "temp_search_pass city=%s pass=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                            city,
                            pass_idx + 1,
                            0,
                            0,
                            len(combined),
                            _kept_estimate_count(combined),
                            run_status if not run_err else f"{run_status}:{run_err[:120]}",
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

                    logger.debug(
                        "temp_search_parsed city=%s pass=%s items=%s",
                        city,
                        pass_idx + 1,
                        len(data),
                    )
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

        if not combined and not curated_only_mode:
            empty_retry_prompt = (
                base_prompt
                + "\n\nFallback step: the first broad search returned no usable exhibitions.\n"
                + "- Search again by checking official museum/gallery 'what's on' and exhibition pages in this city.\n"
                + "- Focus on real exhibitions first; ancillary fields may be empty if they are not quickly verifiable.\n"
                + "- Do not return an empty array unless you cannot verify any qualifying exhibition after checking multiple likely venues.\n"
            )
            content, run_status, run_err = await _run_search_with_retries(
                empty_retry_prompt,
                stage="empty_retry",
                max_tries=5,
            )
            last_raw = content or last_raw
            if run_status != "ok":
                logger.warning(
                    "temp_search_empty_retry city=%s parsed=%s new=%s total=%s kept_est=%s note=%s raw=%r",
                    city,
                    0,
                    0,
                    len(combined),
                    _kept_estimate_count(combined),
                    run_status if not run_err else f"{run_status}:{run_err[:120]}",
                    (last_raw or "")[:600],
                )
            else:
                data = _extract_json_array(content)
                if data is None:
                    logger.warning(
                        "temp_search_empty_retry city=%s parsed=%s new=%s total=%s kept_est=%s note=%s raw=%r",
                        city,
                        0,
                        0,
                        len(combined),
                        _kept_estimate_count(combined),
                        "parse_failed",
                        (content or "")[:600],
                    )
                else:
                    before_total = len(combined)
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
                    logger.info(
                        "temp_search_empty_retry city=%s parsed=%s new=%s total=%s kept_est=%s note=%s",
                        city,
                        len(data),
                        max(0, len(combined) - before_total),
                        len(combined),
                        _kept_estimate_count(combined),
                        "",
                    )

        # Per-venue deepening: query specific venues to extract additional temporary exhibitions.
        if (
            not curated_only_mode
            and (
            venue_deepen_passes > 0
            and len(combined) < pool_max
            and _kept_estimate_count(combined) < desired_min
            )
        ):
            logger.info(
                "temp_search_non_curated_fallback city=%s kept_est=%s target_min=%s venue_discovery_enabled=%s",
                city,
                _kept_estimate_count(combined),
                desired_min,
                venue_discovery_enabled,
            )
            if (
                venue_discovery_enabled
                and venue_discovery_max > 0
                and not discovered_venues
            ):
                discovered_venues = await _discover_venues_async(
                    client=client,
                    city=city,
                    country="",
                    use_web_search_tool=use_web_search_tool,
                    max_venues=venue_discovery_max,
                )
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
                    content, run_status, run_err = await _run_search_with_retries(
                        deepen_prompt,
                        stage=f"deepen:{venue_name}",
                        max_tries=4,
                    )
                    logger.debug(
                        "temp_search_venue_raw city=%s venue=%s chars=%s snippet=%r",
                        city,
                        venue_name,
                        len(content or ""),
                        (content or "")[:600],
                    )
                    if run_status != "ok":
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

        if not combined:
            if had_search_failures and not had_successful_search_response:
                raise RuntimeError(
                    "Temporary exhibition search failed before any valid search response. "
                    f"Last error: {last_search_error or 'unknown'}"
                )
            logger.warning("temp_search_empty city=%s raw=%r", city, last_raw[:2000])
            return []

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

            # Secondary fuzzy dedupe: same venue + same run dates + highly similar titles.
            # This catches variants like:
            # "Auguste Bartholdi" vs "Auguste Bartholdi Liberty Enlightening the World".
            before_fuzzy = len(filtered)
            filtered = _fuzzy_dedupe_items_same_dates(filtered)
            if len(filtered) != before_fuzzy:
                logger.info(
                    "temp_search_fuzzy_deduped city=%s before=%s after=%s",
                    city,
                    before_fuzzy,
                    len(filtered),
                )

            if not curated_only_mode:
                before_same_dates = len(filtered)
                filtered = _dedupe_keep_first_same_venue_dates(filtered)
                if len(filtered) != before_same_dates:
                    logger.info(
                        "temp_search_non_curated_date_deduped city=%s before=%s after=%s",
                        city,
                        before_same_dates,
                        len(filtered),
                    )

        # Apply the hard cap early so we don't do hours/coords backfills for items we won't return.
        if desired_max > 0 and len(filtered) > desired_max:
            filtered = filtered[:desired_max]

        # Fill missing venue addresses (best-effort) for the kept set only.
        missing_address = [
            it
            for it in filtered
            if not str(it.get("address") or "").strip()
        ]
        if missing_address:
            before_missing_address = sum(
                1 for it in filtered if not str(it.get("address") or "").strip()
            )
            sem = asyncio.Semaphore(geo_conc)

            async def _fill_address_one(it: dict) -> None:
                async with sem:
                    looked = await _lookup_venue_address_async(
                        client=client,
                        venue=(it.get("venue") or "").strip(),
                        city=(it.get("city") or city).strip(),
                        country=(it.get("country") or "").strip(),
                        source_url=(it.get("source_url") or "").strip(),
                        use_web_search_tool=use_web_search_tool,
                    )
                    found_address = str((looked or {}).get("address") or "").strip()
                    if found_address:
                        it["address"] = _abbrev_country_in_address(
                            found_address,
                            str(it.get("country") or "").strip(),
                        )

            await asyncio.gather(*[_fill_address_one(it) for it in missing_address])

            for it in filtered:
                if str(it.get("address") or "").strip():
                    continue
                venue = str(it.get("venue") or "").strip()
                item_city = str(it.get("city") or city).strip()
                item_country = str(it.get("country") or "").strip()
                fallback_parts = [p for p in [venue, item_city, item_country] if p]
                if fallback_parts:
                    it["address"] = ", ".join(fallback_parts)
            after_missing_address = sum(
                1 for it in filtered if not str(it.get("address") or "").strip()
            )
            logger.info(
                "temp_search_address_backfill city=%s before_missing=%s after_missing=%s",
                city,
                before_missing_address,
                after_missing_address,
            )

        # Fill missing venue coordinates (best-effort) for the kept set only.
        missing = [
            it
            for it in filtered
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

            await asyncio.gather(*[_fill_one(it) for it in missing])

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
    max_exhibitions: int | None = None,
    openai_usage_callback=None,
) -> pd.DataFrame:
    # `asyncio.run()` cannot be called from inside an existing event loop (e.g. notebooks).
    # This function is called from a FastAPI background thread in `app/ui.py`, so it is safe.
    with temp_openai_usage_tracking(openai_usage_callback):
        return asyncio.run(
            scrape_temporary_exhibitions_async(
                city,
                months=months,
                start_date=start_date,
                end_date=end_date,
                languages=languages,
                max_exhibitions=max_exhibitions,
            )
        )


async def scrape_temporary_exhibitions_async(
    city: str,
    *,
    months: int = 24,
    start_date=None,
    end_date=None,
    languages: list[str] | None = None,
    max_exhibitions: int | None = None,
) -> pd.DataFrame:
    if languages is None:
        languages = LANGUAGES

    hard_max = max(1, int(getattr(settings, "TEMP_HARD_MAX_EXHIBITIONS", 200) or 200))
    absolute_max_setting = int(getattr(settings, "TEMP_ABSOLUTE_MAX_EXHIBITIONS", 10) or 0)
    absolute_max = absolute_max_setting if absolute_max_setting > 0 else hard_max
    target_max = int(getattr(settings, "TEMP_MAX_EXHIBITIONS", absolute_max) or 0)
    if target_max <= 0 or target_max > absolute_max:
        target_max = absolute_max
    if max_exhibitions is not None:
        try:
            per_call = int(max_exhibitions)
        except Exception:
            per_call = 0
        if per_call <= 0:
            return pd.DataFrame(columns=TEMPORARY_COLUMNS_ORDER)
        if per_call < target_max:
            target_max = per_call
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

        name_head = (name.split(":", 1)[0] if name else "").strip()
        parsed_title, _parsed_rest = _split_exhibition_label(name_head)
        exhibition_title_for_image = parsed_title or name_head

        # Best-effort venue image URL + legend (cached).
        image_meta = await _get_venue_image_meta_async(
            venue=venue,
            city=ex_city,
            country=country,
            source_url=source_url,
            exhibition_title=exhibition_title_for_image,
        )
        image_url = (image_meta.get("image_url") or "").strip()
        image_legend = _format_image_legend(image_meta)

        title_for_copy = (
            name.split(":", 1)[0].strip()
            if name
            else ", ".join([p for p in [venue, ex_city] if p])
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
                or "A concise subject-led summary of the exhibition, highlighting the main figure, movement or theme and why the works shown matter."
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
        title = _sanitize_export_title(title)

        venue_category_path = ""
        if country and ex_city and venue:
            venue_category_path = f"{country}, {ex_city}, {venue}"
        elif country and ex_city:
            venue_category_path = f"{country}, {ex_city}"

        duration_val = _normalise_duration_hours(duration_raw) or "1"
        duration_backfill_enabled = (
            int(getattr(settings, "TEMP_DURATION_BACKFILL_ENABLED", 1) or 0) != 0
        )
        if duration_backfill_enabled and (
            not duration_raw
            or duration_raw.strip().lower() in ("1", "1h", "1 hour", "1 hours", "60 minutes", "60 mins")
            or duration_val == "1"
        ):
            try:
                client = _get_openai_client()
                if client is not None:
                    use_web_search_tool = "search-api" not in TEMP_SEARCH_MODEL.lower()
                    title_guess = (name.split(":", 1)[0] if name else "").strip() or title_for_copy
                    looked = await _lookup_exhibition_duration_async(
                        client=client,
                        title=title_guess,
                        venue=venue,
                        city=ex_city,
                        country=country,
                        source_url=source_url,
                        use_web_search_tool=use_web_search_tool,
                    )
                    found_raw = (looked or {}).get("duration") or ""
                    found_norm = _normalise_duration_hours(found_raw)
                    if found_norm:
                        duration_val = found_norm
            except Exception as exc:  # noqa: BLE001
                logger.debug("temp_duration_backfill_error city=%s err=%r", city, exc)

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
            "Rating": "3",
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
        row["Legends of images"] = image_legend or ""

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

        row["Name of site city fr"] = _sanitize_export_title(
            bundle.get("fr", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city es"] = _sanitize_export_title(
            bundle.get("es", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city it"] = _sanitize_export_title(
            bundle.get("it", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city ru"] = _sanitize_export_title(
            bundle.get("ru", {}).get("name") or row["Name of site, City"]
        )
        row["Name of site city zh"] = _sanitize_export_title(
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

    # Final export-stage dedupe: use a canonical identity key (country/city/venue/title)
    # so date-string differences do not leak duplicate exhibitions.
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
            s = _parse_date(str(r.get("Start date (YYYY-MM-DD)") or "").strip())
            e = _parse_date(str(r.get("End date (YYYY-MM-DD)") or "").strip())
            if s:
                score += 2
            if e:
                score += 2
            if s and e and e >= s:
                score += 1
            return score

        by_key: dict[str, dict] = {}
        extras: list[dict] = []
        for r in rows:
            key = _dedupe_export_row_key(r)
            if not key:
                extras.append(r)
                continue
            existing = by_key.get(key)
            if not existing:
                by_key[key] = r
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
            by_key[key] = merged

        if len(by_key) + len(extras) != len(rows):
            logger.info(
                "temp_export_deduped city=%s before=%s after=%s",
                city,
                len(rows),
                len(by_key) + len(extras),
            )
        rows = list(by_key.values()) + extras

        # Secondary fuzzy dedupe for title variants with the same venue + exact run dates.
        # This protects the final sheet from near-duplicate AI title variants.
        grouped_rows: dict[str, list[dict]] = {}
        fuzzy_extras: list[dict] = []
        for r in rows:
            label = str(r.get("Name of site, City") or "").strip()
            title, remainder = _split_exhibition_label(label)
            title = title or _strip_label_date_suffix(label)
            venue_label = _strip_label_date_suffix(remainder)
            venue_key = _canonical_venue_for_similarity(venue_label)
            city_key = _normalise_for_similarity(str(r.get("Real city") or r.get("City") or "").strip())
            country_key = _normalise_for_similarity(str(r.get("Country") or "").strip())
            start_iso, end_iso = _stable_date_pair_strings(
                str(r.get("Start date (YYYY-MM-DD)") or "").strip(),
                str(r.get("End date (YYYY-MM-DD)") or "").strip(),
            )
            if not (title and venue_key and start_iso and end_iso):
                fuzzy_extras.append(r)
                continue
            group_key = "|".join([country_key, city_key, venue_key, start_iso, end_iso])
            grouped_rows.setdefault(group_key, []).append(r)

        before_fuzzy = len(rows)
        fuzzy_rows: list[dict] = []
        date_only = int(getattr(settings, "TEMP_DEDUPE_BY_VENUE_DATES_ONLY", 0) or 0) != 0
        for group in grouped_rows.values():
            if date_only:
                ordered = sorted(group, key=_score_export_row, reverse=True)
                kept_by_first: dict[str, dict] = {}
                for pos, cand in enumerate(ordered):
                    cand_label = str(cand.get("Name of site, City") or "").strip()
                    cand_title, _ = _split_exhibition_label(cand_label)
                    cand_title = cand_title or _strip_label_date_suffix(cand_label)
                    first_key = _first_title_token_for_dedupe(cand_title) or f"__row_{pos}"
                    existing = kept_by_first.get(first_key)
                    if not existing:
                        kept_by_first[first_key] = cand
                        continue
                    a, b = existing, cand
                    best, other = (b, a) if _score_export_row(b) > _score_export_row(a) else (a, b)
                    merged = dict(best)
                    for k, v in other.items():
                        if k not in merged or merged.get(k) in ("", None):
                            merged[k] = v
                    if len((other.get("Full address") or "")) > len((merged.get("Full address") or "")):
                        merged["Full address"] = other.get("Full address") or merged.get("Full address")
                    kept_by_first[first_key] = merged
                fuzzy_rows.extend(kept_by_first.values())
                continue
            ordered = sorted(group, key=_score_export_row, reverse=True)
            kept: list[dict] = []
            for cand in ordered:
                cand_label = str(cand.get("Name of site, City") or "").strip()
                cand_title, _ = _split_exhibition_label(cand_label)
                cand_title = cand_title or _strip_label_date_suffix(cand_label)
                dup_idx = -1
                for idx, existing in enumerate(kept):
                    ex_label = str(existing.get("Name of site, City") or "").strip()
                    ex_title, _ = _split_exhibition_label(ex_label)
                    ex_title = ex_title or _strip_label_date_suffix(ex_label)
                    if _titles_likely_same_exhibition(cand_title, ex_title):
                        dup_idx = idx
                        break
                if dup_idx < 0:
                    kept.append(cand)
                else:
                    a, b = kept[dup_idx], cand
                    best, other = (b, a) if _score_export_row(b) > _score_export_row(a) else (a, b)
                    merged = dict(best)
                    for k, v in other.items():
                        if k not in merged or merged.get(k) in ("", None):
                            merged[k] = v
                    if len((other.get("Full address") or "")) > len((merged.get("Full address") or "")):
                        merged["Full address"] = other.get("Full address") or merged.get("Full address")
                    kept[dup_idx] = merged
            fuzzy_rows.extend(kept)
        rows = fuzzy_rows + fuzzy_extras
        if len(rows) != before_fuzzy:
            logger.info(
                "temp_export_fuzzy_deduped city=%s before=%s after=%s",
                city,
                before_fuzzy,
                len(rows),
            )

        # Tertiary safety dedupe: exact same run dates across the same city/country can still
        # leak duplicate rows when the venue label differs (e.g. partner venues for one show).
        before_cross_venue_dates = len(rows)
        rows = _dedupe_export_rows_same_dates_cross_venue(rows, _score_export_row)
        if len(rows) != before_cross_venue_dates:
            logger.info(
                "temp_export_exact_date_deduped city=%s before=%s after=%s",
                city,
                before_cross_venue_dates,
                len(rows),
            )

        calibrated_ratings = await _assign_city_editorial_ratings_async(rows, city=city)
        for idx, rating in enumerate(calibrated_ratings):
            if idx < len(rows):
                rows[idx]["Rating"] = rating

    df = pd.DataFrame(rows)
    for title_col in (
        "Name of site, City",
        "Name of site city",
        "Name of site city fr",
        "Name of site city es",
        "Name of site city it",
        "Name of site city ru",
        "Name of site city zh",
    ):
        if title_col in df.columns:
            df[title_col] = df[title_col].map(_sanitize_export_title)
    for col in TEMPORARY_COLUMNS_ORDER:
        if col not in df:
            df[col] = ""
    return df[TEMPORARY_COLUMNS_ORDER]


def scrape_destinations_temp(cities: list[str], months: int = 24) -> str:
    hard_max = max(1, int(getattr(settings, "TEMP_HARD_MAX_EXHIBITIONS", 200) or 200))
    absolute_total_setting = int(
        getattr(settings, "TEMP_ABSOLUTE_MAX_TOTAL_EXHIBITIONS", 10) or 0
    )
    absolute_total = absolute_total_setting if absolute_total_setting > 0 else hard_max
    total_max = int(getattr(settings, "TEMP_TOTAL_MAX_EXHIBITIONS", absolute_total) or 0)
    if total_max <= 0 or total_max > absolute_total:
        total_max = absolute_total

    remaining = total_max
    frames: list[pd.DataFrame] = []
    for c in cities:
        if remaining <= 0:
            break
        df_city = scrape_temporary_exhibitions(c, months=months, max_exhibitions=remaining)
        frames.append(df_city)
        try:
            remaining -= int(len(df_city.index))
        except Exception:
            pass

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    Path(settings.RESULT_DIR).mkdir(parents=True, exist_ok=True)
    out_path = Path(settings.RESULT_DIR) / f"{uuid.uuid4()}_places.xlsx"

    combinations = _build_combinations_sheet(df)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Exhibitions", index=False)
        combinations.to_excel(writer, sheet_name="Combinations", index=False)
    return str(out_path)
