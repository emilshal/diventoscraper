import re
import uuid
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta
from openai import OpenAI

from app.config import settings
from app.core.ml import classify


# Temporary-exhibition specific constants
LANGUAGES = ["fr", "es", "it", "ru", "zh-CN"]

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
client: OpenAI | None = None
if settings.OPENAI_API_KEY:
    client = OpenAI(api_key=settings.OPENAI_API_KEY)


# Model selection (temporary exhibitions only)
TEMP_MODEL = settings.OPENAI_TEMP_MODEL


def _ordinal_day(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


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
        start_label = f"{_ordinal_day(start.day)} {start.strftime('%B')}"
        end_label = f"{_ordinal_day(end.day)} {end.strftime('%B')}"
        return f"{start_label}–{end_label} {end.year}"
    if start:
        return f"{_ordinal_day(start.day)} {start.strftime('%B')} {start.year}"
    return f"{_ordinal_day(end.day)} {end.strftime('%B')} {end.year}"


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
    token = name.split(",")[0]
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


def _translate_text(text: str, lang: str) -> str:
    """Translate text using ChatGPT, preserving HTML."""
    if not text or client is None:
        return ""
    prompt = (
        f"Translate the following tourist description to {lang}, ensuring that its inviting and descriptive tone is maintained. "
        f"Keep all HTML tags intact and return only the translated text.\n{text}"
    )
    try:
        resp = client.chat.completions.create(
            model=TEMP_MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return ""


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
    if client is None:
        return []

    today_iso = window_start.isoformat()
    end_iso = window_end.isoformat()
    select_clause = (
        f"- Return exactly {target_max} distinct exhibition (no duplicates).\n\n"
        if target_max <= 1
        else "- Return as many exhibitions as you can find for this window: it is VERY IMPORTANT to reach 10 to 20 distinct exhibitions. Search deeply across multiple venues/sources, keep count, and keep adding until you hit the maximum or truly run out. No duplicates.\n\n"
    )
    prompt = (
        "I need to create new exhibitions for Divento.\n"
        "Please follow these steps exactly.\n\n"
        "1. Select exhibitions\n"
        f"- Assume today's date is {today_iso}.\n"
        f"- The end of the search window is {end_iso}.\n"
        f"- Search for all temporary exhibitions happening in {city} between {today_iso} "
        f"and {end_iso} (inclusive).\n"
        f"- Include exhibitions that started before {today_iso} but are still running during this window (any date overlap counts).\n"
        "- Only include temporary exhibitions, not permanent collections or long-term displays.\n"
        f"{select_clause}"
        "2. Create Divento text\n"
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
        "- short: short description in English for Divento.\n"
        "  * Maximum 164 characters.\n"
        "  * Informal, factual tone.\n"
        "  * Must include a verb.\n"
        "  * Must not repeat the exhibition name.\n"
        "  * No brochure clichés such as 'world-class', 'unforgettable', 'must-see', "
        "'breathtaking', and similar.\n"
        "- long: long description in English for Divento.\n"
        "  * Minimum 350 words.\n"
        "  * Conversational but observational and factual.\n"
        "  * No brochure or marketing language.\n"
        "  * Do not end with a wrap-up or conclusion sentence.\n"
        "  * Do not begin with the exhibition name or with 'this exhibition'.\n"
        "  * Integrate any highlight / don't-miss / lesser-known aspects naturally.\n"
        "  * For museum shows, name at least three specific works, artists or items when possible.\n"
        "  * Use English spelling. Write numbers one to ten in full and numbers 11 upwards as digits.\n\n"
        "For the short and long description fields, follow these additional directions for temporary exhibitions:\n"
        "Temporary exhibitions\n"
        "Maintain an informal tone. Emphasise facts and context throughout the piece. Avoid brochure-style language, cliches, and exaggerated adjectives. "
        "Write as though the exhibition is already running - do not use the future tense. Do not use the words: visitor, visitors, located, feature, featured, showcase, blend, period, or accessible. "
        "Be specific and concrete. Use precise nouns and strong verbs. Cut out filler words, and use the active voice to ensure clarity and directness. Always use British English spelling. "
        "Spell out numbers below 10: one, nine, first, fourth, 17th, 123rd, 999. Compound adjectives such as '16th-century' need a dash, not sixteenth century or 16th century. "
        "Ensure consistent and correct spacing throughout. Do not begin the long description with the name of the exhibition, and avoid concluding sentences or the use of dashes. "
        "Wrap each paragraph in <p> tags and use appropriate HTML tags and entities where needed. Never use the first person. "
        "Each long description should be between 350 and 400 words, aiming for the higher end. Break the content into multiple paragraphs. "
        'Try to include one highlight, two "don\'t miss" elements, and one "hidden gem" but incorporate them smoothly into the text. Mention specific exhibits. '
        "For the short description: do not include the name of the exhibition. It must be no more than 164 characters. It must contain a verb, and must not repeat the phrasing or start of the long description. "
        "Return the text in JSON format within the short and long fields.\n\n"
        "3. Pricing and ticketing\n"
        "- is_free: explicitly check ticketing; set to 0 ONLY if the exhibition is clearly free (explicitly free entry). If there is any ticket price or it is unclear, set to 1. Do not guess 0. Return this as the string '0' or '1'.\n"
        "- ticket_url: the URL of the page where a visitor can buy tickets for the exhibition. "
        "If there is no obvious ticketing page, use an empty string.\n\n"
        "4. Additional info\n"
        "- information: one or two sentences of additional factual context or practical "
        "information about the exhibition, without marketing language.\n"
        "- latitude and longitude: REQUIRED. Provide decimal coordinates for the venue based on reliable sources; do not leave blank. Use decimals (e.g. 48.8606, 2.3376).\n\n"
        "5. Opening pattern\n"
        "- repeat_pattern: 'daily' or 'weekly' based on how the exhibition runs.\n"
        "- open_days: comma-separated weekdays when open (e.g. Tue,Wed,Thu,Fri,Sat,Sun). If open daily, return Mon,Tue,Wed,Thu,Fri,Sat,Sun.\n\n"
        "Do not include image URLs or legends.\n\n"
        "Return ONLY a raw JSON array and nothing else (no code fences, no prose). If you find no exhibitions, return an empty JSON array [].\n"
        "The top-level value must be a JSON array. Each element must "
        "be an object with exactly the keys: "
        "'name', 'city', 'country', 'address', 'duration', 'start_date', 'end_date', "
        "'short', 'long', 'is_free', 'ticket_url', 'information', 'venue', 'latitude', 'longitude', 'repeat_pattern', 'open_days'."
        "And once again, please remember to avoid the use of any dashes and do not include citations."
        "VERY IMPORTANT also remember each long description must be at least 350 words, and short descriptions must never exceed 164 characters."
    )

    try:
        try:
            resp = client.responses.create(
                model=TEMP_MODEL,
                input=prompt,
                tools=[{"type": "web_search"}],
                max_output_tokens=12000,
            )
            content = _clean_json_content(
                resp.output_text or _extract_response_text(resp)
            )
        except Exception as exc_resp:
            print("DEBUG Responses API error:", repr(exc_resp))
            content = ""

        def _chat_retry():
            try:
                resp_chat = client.chat.completions.create(
                    model=TEMP_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    max_completion_tokens=12000,
                )
                return _clean_json_content(resp_chat.choices[0].message.content or "")
            except Exception as exc_chat:
                print("DEBUG Chat Completions error:", repr(exc_chat))
                return ""

        if not content:
            content = _chat_retry()

        if content.strip() in ("[]", ""):
            content = _chat_retry()

        print("DEBUG _fetch_temporary_exhibitions_window raw content:", content[:500])
        if not content or content.strip() in ("[]", ""):
            print("DEBUG _fetch_temporary_exhibitions_window parsed items: 0 (empty)")
            return []

        data = _extract_json_array(content)
        if data is None:
            print(
                "DEBUG _fetch_temporary_exhibitions_window JSON parse failed, raw content follows:"
            )
            print(content)
            print("DEBUG _fetch_temporary_exhibitions_window retrying chat")
            content_retry = _chat_retry()
            if content_retry and content_retry.strip() not in ("[]", ""):
                content_retry = _clean_json_content(content_retry)
                print("DEBUG _fetch_temporary_exhibitions_window retry content:")
                print(content_retry)
                data = _extract_json_array(content_retry)
            if data is None:
                print(
                    "DEBUG _fetch_temporary_exhibitions_window JSON parse failed after retry"
                )
                return []
        print("DEBUG _fetch_temporary_exhibitions_window parsed items:", len(data))

        filtered: list[dict] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            s_raw = (item.get("start_date") or "").strip()
            e_raw = (item.get("end_date") or "").strip()
            s_date = _parse_date(s_raw)
            e_date = _parse_date(e_raw)
            if not s_date:
                continue
            if not e_date:
                e_date = s_date
            if e_date < window_start or s_date > window_end:
                continue
            item["start_date"] = s_date.isoformat()
            item["end_date"] = e_date.isoformat()
            item["latitude"] = _normalise_coord(item.get("latitude"))
            item["longitude"] = _normalise_coord(item.get("longitude"))
            item["venue"] = (item.get("venue") or "").strip()
            item["city"] = (item.get("city") or city).strip()
            item["country"] = _abbrev_country((item.get("country") or "").strip())
            item["address"] = _abbrev_country_in_address(
                (item.get("address") or "").strip(), item["country"]
            )
            filtered.append(item)

        print(
            "DEBUG _fetch_temporary_exhibitions_window kept items after window filter:",
            len(filtered),
        )
        if target_max > 0 and len(filtered) > target_max:
            filtered = filtered[:target_max]
        return filtered
    except Exception as exc:
        print("DEBUG _fetch_temporary_exhibitions_window OpenAI error:", repr(exc))
    return []


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
    languages: list[str] | None = None,
) -> pd.DataFrame:
    if languages is None:
        languages = LANGUAGES

    exhibitions = _fetch_temporary_exhibitions_window(
        city,
        datetime.utcnow().date(),
        datetime.utcnow().date() + relativedelta(months=months),
        target_min=1,
        target_max=20,
    )
    rows: list[dict] = []

    for ex in exhibitions:
        name = (ex.get("name") or "").strip()
        venue = (ex.get("venue") or "").strip()
        address = (ex.get("address") or "").strip()
        duration_raw = (ex.get("duration") or "").strip()
        short_desc = (ex.get("short") or "").strip()
        long_desc = (ex.get("long") or "").strip()
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

        req_city_norm = _normalise_city_name(city)
        ex_city_norm = _normalise_city_name(ex_city)
        if req_city_norm and ex_city_norm and req_city_norm != ex_city_norm:
            continue

        cats = _choose_temp_category(long_desc or short_desc)

        if isinstance(is_free, str):
            is_free_clean = is_free.strip()
        else:
            is_free_clean = str(is_free) if is_free is not None else ""
        free_flag = "0" if is_free_clean in ("0", "free", "FREE") else "1"

        pretty_range = _format_date_range_label(start_date, end_date)

        translations = {lang: {"short": "", "long": ""} for lang in languages}
        for lang in languages:
            if short_desc:
                translations[lang]["short"] = (
                    short_desc if lang == "en" else _translate_text(short_desc, lang)
                )
            if long_desc:
                translations[lang]["long"] = (
                    long_desc if lang == "en" else _translate_text(long_desc, lang)
                )

        if not venue and name:
            head = name.split(":", 1)[0]
            parts = [p.strip() for p in head.split(",") if p.strip()]
            if len(parts) >= 3:
                venue = parts[-2]
                ex_city = parts[-1] or ex_city

        city_label = ex_city or city
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
            return any(val.lower() in seg.lower() for seg in base_segments)

        if venue and not _seg_contains(venue):
            base_segments.append(venue)
        if city_label:
            last_norm = _normalise_city_name(base_segments[-1]) if base_segments else ""
            city_norm = _normalise_city_name(city_label)
            if city_norm and city_norm != last_norm:
                base_segments.append(city_label)
        base_joined = ", ".join(base_segments) if base_segments else (city_label or "")
        if pretty_range:
            title = f"{base_joined}: {pretty_range}"
        else:
            title = base_joined

        name_translations: dict[str, str] = {}
        for lang in languages:
            if lang == "en":
                name_translations[lang] = title
            else:
                translated = _translate_text(title, lang)
                name_translations[lang] = translated or title

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
            "Opening and closing time": "",
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
            "Longitude": longitude,
            "Activity type": "",
            "Rating": "4",
            "Name of site city": title,
            "Name of site city fr": name_translations.get("fr", title),
            "Name of site city es": name_translations.get("es", title),
            "Name of site city it": name_translations.get("it", title),
            "Name of site city ru": name_translations.get("ru", title),
            "Name of site city zh": name_translations.get("zh-CN", title),
            "Real city": ex_city,
            "Start date (YYYY-MM-DD)": start_date,
            "End date (YYYY-MM-DD)": end_date or start_date,
            "Venue category path": venue_category_path,
            "Repeat pattern": repeat_pattern,
            "Open days": open_days,
        }

        for lang in languages:
            code = lang.split("-")[0]
            row[f"Short description {code}"] = translations[lang]["short"]
            row[f"Long description {code}"] = translations[lang]["long"]
            meta_key = {
                "fr": "Meta description fr",
                "es": "Meta description es",
                "it": "Meta description it",
                "ru": "Meta description ru",
                "zh": "Meta description zh",
            }.get(code)
            if meta_key:
                row[meta_key] = (
                    translations[lang]["short"] or translations[lang]["long"]
                )

        rows.append(row)

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
