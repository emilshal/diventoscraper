import re
import uuid
import json
from pathlib import Path

import pandas as pd
import openai
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.sync_api import sync_playwright

from app.config import settings
from app.core.ml import classify

categories = (
    "Whats_hot", "Performing_Arts", "Famous_Places",
    "Eating_and_Drinking", "Arts_and_Culture", "Hidden_Gems",
    "Family", "Parks_and_Gardens", "Historic_Houses_and_Sites",
)

def _generate_descriptions(place: str, city: str) -> tuple[str, str]:
    """Return short and long descriptions using ChatGPT."""
    openai.api_key = settings.OPENAI_API_KEY
    if not openai.api_key:
        return "", ""

    prompt = (
        "Permanent Attractions\n"
        "Write about the following tourist attractions in {city}: {place}. Maintain an informal tone and address the reader directly as \"you\". "
        "Emphasise historical facts and context throughout the piece. Make it sound as though the author has visited the place but never use the first person. "
        "Avoid brochure-style language, cliches, and exaggerated adjectives. Do not use the words: visitor, visitors, located, feature, featured, showcase, blend, period, or accessible. "
        "Be specific and concrete; use precise nouns and strong verbs. Cut out filler words, and use the active voice to ensure clarity and directness. Always use British English spelling. "
        "Spell out numbers below 10: one, nine, first, fourth, 17th, 123rd, 999. Compound adjectives such as '16th-century' need a dash, not sixteenth century or 16th century. "
        "Ensure consistent and correct spacing throughout. "
        "Do not begin the long description or the short description with the name of the attraction, and avoid concluding sentences or the use of dashes. "
        "Wrap each paragraph in <p> tags and use appropriate HTML tags and entities where needed. "
        "Each long description should be between 350 and 400 words, aiming for the higher end. Break the content into multiple paragraphs. "
        "Try to include one highlight, two \"don't miss\" elements, and one \"hidden gem\" but incorporate them smoothly into the text. "
        "For the short description: do not include the name of the attraction. It must be no more than 164 characters, must contain a verb, and must not repeat the phrasing or start of the long description. "
        "Return the output in JSON format with two keys: short and long."
    ).format(place=place, city=city)

    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4.1-nano",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
        )
        content = resp.choices[0].message.content
        data = json.loads(content)
        return data.get("short", ""), data.get("long", "")
    except Exception:
        return "", ""

ua = UserAgent()

DEFAULT_SEARCH_TERMS = [
    "Things to do in {city}",
    "Things to see in {city}",
    "Amusement Parks in {city}",
    "Best Art Galleries {city}",
    "Best Parks in {city}",
    "Tourist attractions {city}",
    "Famous Places in {city}",
    "Top 50 attractions {city}",
    "Best Churches in {city}",
    "Best Museums in {city}",
    "Best Opera Theaters in {city}",
]

LANGUAGES = ["fr", "es", "it", "ru", "zh-CN"]

def _translate_text(text: str, lang: str) -> str:
    """Translate text using ChatGPT."""
    openai.api_key = settings.OPENAI_API_KEY
    if not text or not openai.api_key:
        return ""
    prompt = (
        f"Translate the following tourist description to {lang}, ensuring that its inviting and descriptive tone is maintained. Keep all HTML tags intact and return only the translated text.\n" + text
    )
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4.1-nano",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return ""

def _fetch_results(query: str) -> str:
    escaped = query.replace(" ", "+")
    url = f"https://www.google.com/search?q={escaped}&num=1&hl=en-en"
    headers = {"User-Agent": ua.random}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text

def _parse_address_duration(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    address = ""
    duration = ""
    addr = soup.find("a", text="Address")
    if addr:
        span = addr.find_next("span")
        if span:
            address = span.text
    dur_node = soup.find("div", class_="UYKlhc")
    if dur_node:
        tokens = dur_node.text.split()
        if len(tokens) >= 4:
            if tokens[0] and tokens[3] != "up":
                duration = tokens[3]
            elif len(tokens) >= 6:
                duration = tokens[5]
    return address, duration


def scrape_city(city: str, *, min_reviews: int = 2500,
                search_terms: list[str] | None = None,
                languages: list[str] | None = None) -> pd.DataFrame:
    if search_terms is None:
        search_terms = [t.format(city=city) for t in DEFAULT_SEARCH_TERMS]
    if languages is None:
        languages = LANGUAGES

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-gpu", "--single-process", "--renderer-process-limit=1"],
        )
        page = browser.new_page()
        places: list[str] = []
        for term in search_terms:
            page.goto(f"https://www.google.com/maps/search/{term.replace(' ', '+')}")
            page.wait_for_timeout(3000)
            for card in page.locator('.Nv2PK').all():
                try:
                    title = card.locator('.qBF1Pd').inner_text()
                    reviews = int(re.sub(r"\D", "", card.locator('.UY7F9').inner_text()))
                except Exception:
                    continue
                if reviews >= min_reviews and title not in places:
                    places.append(title)
        browser.close()

    rows = []
    for place in places:
        html = _fetch_results(f"{place} {city}")
        address, duration = _parse_address_duration(html)

        short_desc, long_desc = _generate_descriptions(place, city)
        desc_for_ml = long_desc or short_desc
        label = classify(desc_for_ml)
        cats = ", ".join(
            c.replace("_", " ") for c, v in zip(categories, label) if v
        )

        translations = {lang: {"short": "", "long": ""} for lang in languages}
        for lang in languages:
            if short_desc:
                translations[lang]["short"] = _translate_text(short_desc, lang)
            if long_desc:
                translations[lang]["long"] = _translate_text(long_desc, lang)

        row = {
            'Name  of site, City': f'{place}, {city}',
            'City / Country': city,
            'full address': address,
            'Type(s) of activity': '',
            'Divento Categories': cats,
            'Free activity?': '',
            'short description': short_desc,
            'long description': long_desc,
            'URLof  images': '',
            'duration of visit': duration,
            'opening and closing time': '',
            'legendsof images': ''
        }

        for lang in languages:
            row[f'short description {lang.split("-")[0]}'] = translations[lang]['short']
            row[f'long description {lang.split("-")[0]}'] = translations[lang]['long']

        rows.append(row)

    columns_order = [
        'Name  of site, City',
        'City / Country',
        'full address',
        'Type(s) of activity',
        'Divento Categories',
        'Free activity?',
        'short description',
        'long description',
        'URLof  images',
        'duration of visit',
        'opening and closing time',
        'short description fr',
        'long description fr',
        'short description es',
        'long description es',
        'short description it',
        'long description it',
        'short description ru',
        'long description ru',
        'short description zh',
        'long description zh',
        'legendsof images'
    ]

    df = pd.DataFrame(rows)
    for col in columns_order:
        if col not in df:
            df[col] = ''
    return df[columns_order]

def scrape_destinations(cities: list[str]) -> str:
    frames = [scrape_city(c) for c in cities]
    df = pd.concat(frames, ignore_index=True)
    Path(settings.RESULT_DIR).mkdir(parents=True, exist_ok=True)
    out_path = Path(settings.RESULT_DIR) / f'{uuid.uuid4()}_places.xlsx'
    df.to_excel(out_path, index=False)
    return str(out_path)
