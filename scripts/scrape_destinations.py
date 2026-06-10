#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Permanent destinations dispatcher (cron-driven, runs every minute).

This is a THIN dispatcher — it replaces the old ~1700-line Google-Places
+ enrichment + translation + Excel monolith with a small shim that:

  1. Recovers stale `processing` rows so they don't hang forever.
  2. Polls the existing `processing` row (if any) for completion.
  3. Otherwise claims the oldest `new` row and POSTs it to the FastAPI
     permanent scraper at /api/runs/permanent.
  4. On `done`, downloads the produced Excel and saves it to Laravel
     storage with the same filename convention the Filament UI expects.

All the heavy lifting (search, enrichment, copy, translate, rating, Excel
writing) lives in the FastAPI service at app/core/permanent_scraper.py
and app/ui.py.

Deploy: this file lives in the Filament admin tree at
  /var/www/zapbot/scripts/scrape_destinations.py
and the cron entry (per minute) runs:
  cd /var/www/zapbot && python3 scripts/scrape_destinations.py

The canonical version is in this repo so the dispatcher stays in lockstep
with the FastAPI endpoints it depends on.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pymysql
import requests
from dotenv import load_dotenv


PROJECT_PATH = Path("/var/www/zapbot")
if not PROJECT_PATH.exists():
    # Local dev fallback: project root is one level up from this script.
    PROJECT_PATH = Path(__file__).resolve().parents[1]

ENV_FILE = PROJECT_PATH / ".env"
if not ENV_FILE.exists():
    ENV_FILE = PROJECT_PATH / ".env.example"
load_dotenv(ENV_FILE)

DB_CONNECTION = os.getenv("DB_CONNECTION", "mysql")
DB_PATH = os.getenv("DB_DATABASE", str(PROJECT_PATH / "database" / "database.sqlite"))
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_DATABASE", "")
DB_USER = os.getenv("DB_USERNAME", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

PLACEHOLDER = "%s" if DB_CONNECTION == "mysql" else "?"
NOW_SQL = "NOW()" if DB_CONNECTION == "mysql" else "CURRENT_TIMESTAMP"
STALE_JOB_TIMEOUT_MINUTES = int(os.getenv("STALE_JOB_TIMEOUT_MINUTES", "360"))

STORAGE_PATH = str(PROJECT_PATH / "storage" / "app" / "private")
DEST_LOG_DIR = Path(STORAGE_PATH) / "destination_logs"

# The FastAPI service the permanent scraper runs in. Same host:port as the
# temp scraper — the permanent endpoints live under /api/runs/permanent.
PERM_API_BASE = os.getenv("TEMP_SCRAPER_API_URL", "http://127.0.0.1:8000").rstrip("/")
HTTP_TIMEOUT = float(os.getenv("TEMP_SCRAPER_HTTP_TIMEOUT", "30"))

HTTP = requests.Session()
CURRENT_LOG_FILE: Path | None = None
CURRENT_DESTINATION: str | None = None


def _stale_cutoff_sql() -> str:
    if DB_CONNECTION == "mysql":
        return f"(NOW() - INTERVAL {STALE_JOB_TIMEOUT_MINUTES} MINUTE)"
    return f"datetime('now', '-{STALE_JOB_TIMEOUT_MINUTES} minutes')"


def _now_iso() -> str:
    return datetime.now().isoformat()


def _safe_name(value: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", value or "").strip()
    return re.sub(r"[-\s]+", "_", cleaned) or "destination"


def setup_destination_log(destination_name: str, destination_id: str | None = None) -> Path:
    global CURRENT_LOG_FILE, CURRENT_DESTINATION

    DEST_LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{_safe_name(destination_name)}_{ts}.log"
    if destination_id:
        log_filename = f"{destination_id}_{_safe_name(destination_name)}_{ts}.log"

    CURRENT_LOG_FILE = DEST_LOG_DIR / log_filename
    CURRENT_DESTINATION = destination_name

    with open(CURRENT_LOG_FILE, "w", encoding="utf-8") as handle:
        handle.write("=== PERMANENT DESTINATION SCRAPING LOG ===\n")
        handle.write(f"Destination: {destination_name}\n")
        handle.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        handle.write(f"Log File: {log_filename}\n")
        handle.write("=" * 50 + "\n\n")

    return CURRENT_LOG_FILE


def log_to_destination_file(message: str, level: str = "INFO") -> None:
    if not CURRENT_LOG_FILE:
        return
    timestamp = datetime.now().strftime("%H:%M:%S")
    with open(CURRENT_LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {level}: {message}\n")


def print_and_log(message: str, level: str = "INFO") -> None:
    print(message)
    log_to_destination_file(message, level)


def close_destination_log() -> None:
    global CURRENT_LOG_FILE, CURRENT_DESTINATION
    if CURRENT_LOG_FILE:
        with open(CURRENT_LOG_FILE, "a", encoding="utf-8") as handle:
            handle.write(f"\n=== COMPLETED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
    CURRENT_LOG_FILE = None
    CURRENT_DESTINATION = None


def get_connection():
    if DB_CONNECTION == "sqlite":
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.Cursor,
        autocommit=False,
        charset="utf8mb4",
    )


def _request_json(method: str, url: str, **kwargs) -> dict[str, Any]:
    response = HTTP.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
    response.raise_for_status()
    return response.json()


def _download_file(url: str, target_path: Path) -> None:
    with HTTP.get(url, timeout=max(HTTP_TIMEOUT, 600), stream=True) as response:
        response.raise_for_status()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    handle.write(chunk)


def _build_start_payload(row: tuple[Any, ...]) -> dict[str, Any]:
    (
        _id,
        destination_name,
        _job_type,
        _status,
        _filename,
        _date,
        minimum_reviews,
        city,
        country,
        _start_date,
        _end_date,
        _months,
        _temp_run_id,
        _error_message,
    ) = row

    city_s = (city or "").strip() if isinstance(city, str) else ""
    country_s = (country or "").strip() if isinstance(country, str) else ""
    # Fall back to the destination_name field if city is blank (matches the
    # temp dispatcher's defensive behaviour).
    if not city_s:
        raw = (destination_name or "").strip() if isinstance(destination_name, str) else ""
        if "," in raw:
            city_s, _, country_s = (p.strip() for p in raw.partition(","))
        else:
            city_s = raw

    if not city_s:
        raise ValueError(
            f"No city available for permanent job label '{destination_name}'"
        )

    payload: dict[str, Any] = {
        "cities": [city_s],
        # Country may legitimately be blank (Filament rows without a country
        # field) — pass it through as-is. Do NOT default it: a wrong country
        # makes the search prompt ask for e.g. "Helsinki, Italy" and the
        # model honestly finds zero venues.
        "country": country_s,
        "min_reviews": int(minimum_reviews) if minimum_reviews is not None else 1000,
    }
    return payload


def _format_progress(row: dict[str, Any]) -> str:
    """One-line summary of the FastAPI run state for the destination log."""
    parts: list[str] = []
    phase = str(row.get("current_phase") or "")
    if phase:
        parts.append(f"phase={phase}")
    cur_city = str(row.get("current_city") or "")
    if cur_city:
        parts.append(f"current_city={cur_city}")
    pct = row.get("progress_pct")
    if pct is not None:
        try:
            parts.append(f"progress={float(pct):.1f}%")
        except Exception:
            pass
    summary = row.get("result_summary")
    if isinstance(summary, dict) and summary:
        try:
            total = sum(int(v) for v in summary.values())
            parts.append(f"venues_so_far={total}")
        except Exception:
            pass
    return ", ".join(parts)


def _set_status(
    cur,
    dest_id: int,
    status: str,
    *,
    error_message: str | None = None,
    temp_run_id: str | None = None,
) -> None:
    parts = [f"status={PLACEHOLDER}"]
    params: list[Any] = [status]
    if error_message is not None:
        parts.append(f"error_message={PLACEHOLDER}")
        params.append(error_message[:5000])
    if temp_run_id is not None:
        parts.append(f"temp_run_id={PLACEHOLDER}")
        params.append(temp_run_id)
    parts.append(f"updated_at={NOW_SQL}")
    sql = f"UPDATE destinations SET {', '.join(parts)} WHERE id={PLACEHOLDER}"
    params.append(dest_id)
    cur.execute(sql, tuple(params))


def _mark_done(cur, dest_id: int, filename: str) -> None:
    cur.execute(
        f"UPDATE destinations "
        f"SET status={PLACEHOLDER}, filename={PLACEHOLDER}, date={PLACEHOLDER}, "
        f"error_message={PLACEHOLDER}, updated_at={NOW_SQL} "
        f"WHERE id={PLACEHOLDER}",
        ("done", filename, _now_iso(), None, dest_id),
    )


_SELECT_COLS = (
    "id, destination_name, job_type, status, filename, date, "
    "minimum_reviews, city, country, start_date, end_date, months, "
    "temp_run_id, error_message"
)


def _select_processing_permanent(cur):
    cur.execute(
        f"SELECT {_SELECT_COLS} FROM destinations "
        f"WHERE job_type='permanent' AND status='processing' AND temp_run_id IS NOT NULL "
        f"ORDER BY updated_at ASC, id ASC LIMIT 1"
    )
    return cur.fetchone()


def _select_new_permanent(cur):
    # NOTE the job_type='permanent' filter — this is THE original bug fix.
    # Earlier code had this commented out, which let the permanent scraper
    # claim temporary rows. Do NOT remove this filter.
    cur.execute(
        f"SELECT {_SELECT_COLS} FROM destinations "
        f"WHERE job_type='permanent' AND status='new' "
        f"ORDER BY id ASC LIMIT 1"
    )
    return cur.fetchone()


def _ensure_perm_api() -> bool:
    try:
        health = _request_json("GET", f"{PERM_API_BASE}/api/healthz")
        print_and_log(
            f"Perm API health: ok={health.get('ok')} status={health.get('status')} "
            f"running_jobs={health.get('running_jobs')}"
        )
        return bool(health.get("ok"))
    except Exception as exc:
        print_and_log(f"Perm API health check failed: {exc}", "ERROR")
        return False


def _start_perm_run(conn, cur, row) -> None:
    dest_id = int(row[0])
    destination_name = str(row[1] or f"destination-{dest_id}")
    setup_destination_log(destination_name, str(dest_id))
    print_and_log(f"Starting permanent job id={dest_id} name={destination_name}")

    if not _ensure_perm_api():
        _set_status(cur, dest_id, "error", error_message="Perm scraper API health check failed")
        conn.commit()
        close_destination_log()
        return

    try:
        payload = _build_start_payload(row)
        print_and_log(f"POST /api/runs/permanent payload={json.dumps(payload, ensure_ascii=False)}")
        data = _request_json("POST", f"{PERM_API_BASE}/api/runs/permanent", json=payload)
        run_id = str(data.get("run_id") or "").strip()
        if not run_id:
            raise RuntimeError(f"Perm API did not return run_id: {data!r}")

        _set_status(cur, dest_id, "processing", error_message=None, temp_run_id=run_id)
        conn.commit()
        print_and_log(f"Perm run started run_id={run_id}; status set to processing")
        close_destination_log()
    except Exception as exc:
        _set_status(cur, dest_id, "error", error_message=str(exc))
        conn.commit()
        print_and_log(f"Failed to start perm run: {exc}", "ERROR")
        close_destination_log()


def _poll_perm_run(conn, cur, row) -> None:
    dest_id = int(row[0])
    destination_name = str(row[1] or f"destination-{dest_id}")
    run_id = str(row[12] or "").strip()
    setup_destination_log(destination_name, str(dest_id))
    print_and_log(f"Polling permanent job id={dest_id} run_id={run_id}")

    try:
        status_payload = _request_json(
            "GET", f"{PERM_API_BASE}/api/runs/permanent/{run_id}"
        )
        status = str(status_payload.get("status") or "")
        print_and_log(f"Perm API run status={status}")
        progress_line = _format_progress(status_payload)
        if progress_line:
            print_and_log(f"Perm API progress: {progress_line}")

        # The run_store statuses: queued | searching | enriching | translating |
        # writing | done | error. Anything other than done/error means still
        # in flight — the next cron tick will poll again.
        if status in ("queued", "searching", "enriching", "translating", "writing"):
            close_destination_log()
            return

        if status == "error":
            error_message = str(status_payload.get("error_message") or "Perm API run failed")
            _set_status(cur, dest_id, "error", error_message=error_message)
            conn.commit()
            print_and_log(f"Marked error: {error_message}", "ERROR")
            close_destination_log()
            return

        if status != "done":
            print_and_log(f"Unexpected status {status!r}; leaving as processing", "WARNING")
            close_destination_log()
            return

        # status == done — download the Excel.
        slug = _safe_name(destination_name).lower()
        filename = f"perm_{slug}_{datetime.now():%Y%m%d%H%M%S}.xlsx"
        target_path = Path(STORAGE_PATH) / filename
        excel_url = f"{PERM_API_BASE}/api/runs/permanent/{run_id}/excel"
        print_and_log(f"Downloading Excel from {excel_url} -> {target_path}")
        _download_file(excel_url, target_path)

        _mark_done(cur, dest_id, filename)
        conn.commit()
        print_and_log(f"Done. Saved {filename}")
        close_destination_log()
    except Exception as exc:
        _set_status(cur, dest_id, "error", error_message=str(exc))
        conn.commit()
        print_and_log(f"Polling/downloading failed: {exc}", "ERROR")
        close_destination_log()


def _recover_stale_permanent(conn, cur) -> None:
    """Reset permanent jobs that can never make progress on their own.

    A) status='processing' with temp_run_id IS NULL: nothing polls these (the
       poll query requires a run id) and nothing re-claims them (the new query
       requires status='new'). Orphaned signature — straight to error.
    B) status='processing' with a temp_run_id but no progress past the
       timeout: the dispatched run died silently or got wedged.
    """
    cur.execute(
        f"UPDATE destinations SET status='error', error_message={PLACEHOLDER}, "
        f"updated_at={NOW_SQL} "
        f"WHERE job_type='permanent' AND status='processing' AND temp_run_id IS NULL",
        (
            "Reset by stale-job recovery: permanent job stuck in 'processing' "
            "with no run id (orphaned).",
        ),
    )
    orphaned = cur.rowcount

    cur.execute(
        f"UPDATE destinations SET status='error', error_message={PLACEHOLDER}, "
        f"updated_at={NOW_SQL} "
        f"WHERE job_type='permanent' AND status='processing' AND temp_run_id IS NOT NULL "
        f"AND (updated_at IS NULL OR updated_at < {_stale_cutoff_sql()})",
        (
            f"Reset by stale-job recovery: permanent run made no progress for "
            f"over {STALE_JOB_TIMEOUT_MINUTES} min (dispatched run likely died).",
        ),
    )
    stale = cur.rowcount

    if orphaned or stale:
        conn.commit()
        print(
            f"Stale-job recovery: reset {orphaned} orphaned and {stale} stale "
            f"permanent job(s) to error."
        )


def process_permanent_destinations() -> None:
    conn = get_connection()
    if not conn:
        print("Unable to establish DB connection.")
        return

    with conn:
        cur = conn.cursor()

        _recover_stale_permanent(conn, cur)

        processing_row = _select_processing_permanent(cur)
        if processing_row:
            _poll_perm_run(conn, cur, processing_row)
            return

        new_row = _select_new_permanent(cur)
        if new_row:
            _start_perm_run(conn, cur, new_row)
            return

        print("No permanent jobs found.")


if __name__ == "__main__":
    process_permanent_destinations()
