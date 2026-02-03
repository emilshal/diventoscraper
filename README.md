# Divento Scraper V2

## Overview
Temporary exhibitions scraper that queries GPT (with web search fallback) to find 10–20 temporary exhibitions per city within a date window and produce Divento-ready output (name, venue, dates, descriptions, ticket flag, ticket URL, coordinates, opening pattern). Results are saved to Excel.

## Setup
- Python 3 installed.
- Install deps: `pip install -r requirements.txt`
- Create `.env` with your OpenAI key: `OPENAI_API_KEY=...`
- Run commands from the repo root with `PYTHONPATH=.`.

## Run: Temporary Exhibitions Scraper
```bash
PYTHONPATH=. python3 -m app.cli_scraper_temp cities.txt --months 24
```
- `cities.txt`: one city per line (TXT or CSV).
- `--months`: months from today to search (default 24).
- `--output`: optional path for the Excel output; otherwise `data/<uuid>_places.xlsx` is used.

## Output
- An Excel file with exhibitions plus a combinations sheet (date rows per exhibition). Path is printed when done.
- Descriptions follow the prompt constraints (informal tone, British English, 350–400 word long description, short description ≤164 chars with a verb, etc.).

## Notes
- Debug logs print model responses/parse status; JSON parse issues will log a snippet of the raw reply.
- Data files and `.env` are ignored via `.gitignore`.

### Tuning (optional)
- Target count: `TEMP_TARGET_MIN_EXHIBITIONS` / `TEMP_TARGET_MAX_EXHIBITIONS`
- Search passes: `TEMP_SEARCH_PASSES` (default 3)
- Venue discovery: `TEMP_VENUE_DISCOVERY_ENABLED=1`, `TEMP_VENUE_DISCOVERY_MAX=50`
- Per-venue deepening: `TEMP_VENUE_DEEPEN_PASSES`, `TEMP_VENUE_DEEPEN_MAX_VENUES`, `TEMP_VENUE_DEEPEN_MAX_PER_VENUE`
- Image fallback: `TEMP_IMAGE_FALLBACK_URL` and `TEMP_IMAGE_FAVICON_SIZE`
