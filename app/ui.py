from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from hashlib import sha256
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.core.temp_scraper import _build_combinations_sheet, scrape_temporary_exhibitions
from app.logging_setup import init_logging

import logging

logger = logging.getLogger(__name__)

init_logging()

app = FastAPI(title="Divento Scraper v2")


def _key_fingerprint(key: str) -> str:
    digest = sha256(key.encode("utf-8")).hexdigest()
    prefix = key[:7]
    suffix = key[-4:] if len(key) >= 4 else key
    return f"{prefix}…{suffix} sha256:{digest[:12]}"


def _env_file_key() -> str | None:
    try:
        raw = Path(".env").read_text(encoding="utf-8")
    except Exception:
        return None
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        if k.strip() != "OPENAI_API_KEY":
            continue
        v = v.strip().strip("'").strip('"')
        return v or None
    return None


@app.on_event("startup")
def _log_startup_config() -> None:
    env_has_key = bool(os.environ.get("OPENAI_API_KEY"))
    envfile_key = _env_file_key()
    envfile_fp = _key_fingerprint(envfile_key) if envfile_key else ""

    # If `.env` has a key but the Settings instance doesn't match it, prefer `.env`.
    # This covers long-running dev servers where Settings was instantiated before the `.env` edit,
    # or environments where a stale process env key got captured early.
    if envfile_key and settings.OPENAI_API_KEY != envfile_key:
        settings.OPENAI_API_KEY = envfile_key
        os.environ["OPENAI_API_KEY"] = envfile_key
        logger.warning("openai_key_overridden_from_envfile %s", envfile_fp)

    if settings.OPENAI_API_KEY:
        loaded_fp = _key_fingerprint(settings.OPENAI_API_KEY)
        envfile_match = bool(envfile_key) and (_key_fingerprint(envfile_key) == loaded_fp)
        logger.info(
            "openai_key_loaded %s env_has_key=%s envfile_fp=%s envfile_matches=%s",
            loaded_fp,
            env_has_key,
            envfile_fp,
            envfile_match,
        )
    else:
        logger.warning(
            "openai_key_loaded missing env_has_key=%s envfile_has_key=%s",
            env_has_key,
            bool(envfile_key),
        )

_ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_WHITESPACE_BETWEEN_TAGS_RE = re.compile(r">\s+<")


def _sanitize_excel_cell(value: Any) -> Any:
    if value is None:
        return ""
    if not isinstance(value, str):
        return value

    # Fix known bad control character that sometimes replaces apostrophes in model output.
    value = value.replace("\x19", "'")

    value = value.replace("\r\n", "\n").replace("\r", "\n")
    value = value.replace("\n", " ")

    value = _ILLEGAL_EXCEL_CHARS_RE.sub("", value)
    value = _WHITESPACE_BETWEEN_TAGS_RE.sub("><", value)
    value = re.sub(r"\s{2,}", " ", value)
    return value.strip()


def _sanitize_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].map(_sanitize_excel_cell)
    return out


_FILENAME_SAFE_RE = re.compile(r"[^a-zA-Z0-9]+")


def _slugify_filename_part(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return "unknown"
    s = _FILENAME_SAFE_RE.sub("-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
    return s or "unknown"


def _excel_filename_for_run(*, cities: list[str], started_at_iso: str, run_id: str) -> str:
    parts: list[str] = []
    for raw in cities:
        parsed = _parse_city_input(raw or "")
        city = _slugify_filename_part(parsed.get("city") or raw)
        country = _slugify_filename_part(parsed.get("country") or "")
        parts.append(f"{city}-{country}".strip("-"))

    # Keep filenames readable and within reasonable length.
    if len(parts) > 4:
        head = parts[:4]
        parts = head + [f"{len(parts) - 4}more"]

    try:
        dt = datetime.fromisoformat(started_at_iso.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    stamp = dt.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")

    prefix = "_".join(p for p in parts if p)
    prefix = prefix[:80].rstrip("_-")
    if not prefix:
        prefix = "run"
    return f"{prefix}_{stamp}_{run_id}_places.xlsx"


class RunRequest(BaseModel):
    cities: list[str] = Field(default_factory=list)
    months: int | None = 24
    start_date: str | None = None
    end_date: str | None = None


@dataclass
class RunState:
    run_id: str
    started_at: str
    finished_at: str = ""
    months: int = 24
    start_date: str = ""
    end_date: str = ""
    cities_requested: list[str] = field(default_factory=list)
    status: str = "running"  # running|ok|error
    error: str = ""
    excel_path: str = ""
    report_path: str = ""
    city_results: list[dict[str, Any]] = field(default_factory=list)


_RUNS: dict[str, RunState] = {}
_LOCK = threading.Lock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_report(state: RunState) -> None:
    Path(settings.RESULT_DIR).mkdir(parents=True, exist_ok=True)
    report_path = Path(settings.RESULT_DIR) / f"{state.run_id}_run_report.json"
    payload: dict[str, Any] = {
        "run_id": state.run_id,
        "started_at": state.started_at,
        "finished_at": state.finished_at,
        "months": state.months,
        "start_date": state.start_date,
        "end_date": state.end_date,
        "status": state.status,
        "error": state.error,
        "excel_path": state.excel_path,
        "report_path": str(report_path),
        "cities_requested": state.cities_requested,
        "city_results": state.city_results,
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    state.report_path = str(report_path)


def _parse_city_input(raw: str) -> dict[str, str]:
    parts = [p.strip() for p in (raw or "").split(",")]
    city = parts[0] if parts else raw
    country = parts[-1] if len(parts) >= 2 else ""
    if len(parts) >= 2:
        a = (parts[0] or "").strip().lower()
        b = (parts[1] or "").strip()
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
        if a in known_countries and b:
            # User entered "Country, City"
            city = b
            country = parts[0].strip()
    return {"raw": raw, "city": city, "region": "", "country": country}


def _run_job(state: RunState) -> None:
    t0 = time.monotonic()
    try:
        logger.info(
            "run_start run_id=%s cities=%s months=%s",
            state.run_id,
            len(state.cities_requested),
            state.months,
        )

        window_start: date | None = None
        window_end: date | None = None
        if state.start_date and state.end_date:
            try:
                window_start = datetime.fromisoformat(state.start_date).date()
                window_end = datetime.fromisoformat(state.end_date).date()
            except Exception:
                window_start = None
                window_end = None

        frames: list[pd.DataFrame] = []
        city_results: list[dict[str, Any]] = []

        hard_max = max(1, int(getattr(settings, "TEMP_HARD_MAX_EXHIBITIONS", 200) or 200))
        absolute_total_setting = int(
            getattr(settings, "TEMP_ABSOLUTE_MAX_TOTAL_EXHIBITIONS", 10) or 0
        )
        absolute_total = absolute_total_setting if absolute_total_setting > 0 else hard_max
        total_max = int(getattr(settings, "TEMP_TOTAL_MAX_EXHIBITIONS", absolute_total) or 0)
        if total_max <= 0 or total_max > absolute_total:
            total_max = absolute_total
        remaining = total_max

        for raw_city in state.cities_requested:
            t_city = time.monotonic()
            try:
                status = ""
                err = ""
                if remaining <= 0:
                    df = pd.DataFrame()
                    exhibitions = []
                    status = "skipped"
                    err = "global exhibition cap reached"
                elif window_start is not None and window_end is not None:
                    df = scrape_temporary_exhibitions(
                        raw_city,
                        months=state.months,
                        start_date=window_start,
                        end_date=window_end,
                        max_exhibitions=remaining,
                    )
                else:
                    df = scrape_temporary_exhibitions(
                        raw_city, months=state.months, max_exhibitions=remaining
                    )
                frames.append(df)
                exhibitions = df.to_dict(orient="records")
                if status != "skipped":
                    status = "ok" if exhibitions else "empty"
                    try:
                        remaining -= int(len(df.index))
                    except Exception:
                        pass
            except Exception as exc:
                logger.exception("city_error run_id=%s city=%s: %r", state.run_id, raw_city, exc)
                exhibitions = []
                status = "error"
                err = repr(exc)

            city_results.append(
                {
                    "input": _parse_city_input(raw_city),
                    "status": status,
                    "error": err,
                    "exhibitions_found": len(exhibitions),
                    "exhibitions": exhibitions,
                }
            )
            logger.info(
                "city_done run_id=%s city=%s status=%s exhibitions=%s seconds=%.2f",
                state.run_id,
                raw_city,
                status,
                len(exhibitions),
                time.monotonic() - t_city,
            )

        state.city_results = city_results

        df_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        combinations = _build_combinations_sheet(df_all) if not df_all.empty else pd.DataFrame(
            columns=["Name of site, City", "Date", "Quantity"]
        )

        Path(settings.RESULT_DIR).mkdir(parents=True, exist_ok=True)
        target = Path(settings.RESULT_DIR) / _excel_filename_for_run(
            cities=state.cities_requested, started_at_iso=state.started_at, run_id=state.run_id
        )
        state.excel_path = str(target)
        with pd.ExcelWriter(target, engine="openpyxl") as writer:
            _sanitize_df_for_excel(df_all).to_excel(writer, sheet_name="Exhibitions", index=False)
            _sanitize_df_for_excel(combinations).to_excel(writer, sheet_name="Combinations", index=False)

        state.status = "ok" if any(cr["status"] == "ok" for cr in city_results) else "empty"
        state.error = ""
        logger.info(
            "run_done run_id=%s status=%s seconds=%.2f excel=%s",
            state.run_id,
            state.status,
            time.monotonic() - t0,
            state.excel_path,
        )
    except Exception as exc:
        state.status = "error"
        state.error = repr(exc)
        logger.exception("run_error run_id=%s: %r", state.run_id, exc)
    finally:
        state.finished_at = _utc_now_iso()
        _write_report(state)


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Divento Scraper v2</title></head>
  <body>
    <style>
      :root { color-scheme: light; }
      * { box-sizing: border-box; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
      h1 { margin: 0 0 12px; }
      .card { max-width: 820px; padding: 16px; border: 1px solid #e5e7eb; border-radius: 12px; }
      label { display: block; font-weight: 600; margin: 14px 0 6px; }
      textarea, input { width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 10px; font-size: 14px; }
      textarea { min-height: 96px; resize: vertical; }
      .row { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px; }
      .actions { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-top: 14px; }
      #runId { word-break: break-all; }
      button { padding: 10px 14px; border-radius: 10px; border: 1px solid #111827; background: #111827; color: white; font-weight: 600; cursor: pointer; }
      button.secondary { background: white; color: #111827; border-color: #d1d5db; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .status { margin-top: 14px; padding: 10px 12px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 10px; white-space: pre-wrap; }
      .links a { margin-right: 12px; }
      code { background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }
      @media (max-width: 720px) {
        .row { grid-template-columns: 1fr; }
      }
    </style>

    <div class="card">
      <h1>Divento Scraper v2</h1>
      <div class="row">
        <div>
          <label for="cities">Cities (one per line)</label>
          <textarea id="cities">London, UK</textarea>
        </div>
        <div>
          <label for="startDate">Start date</label>
          <input id="startDate" type="date" />
          <label for="endDate">End date</label>
          <input id="endDate" type="date" />
          <label for="poll">Poll interval (ms)</label>
          <input id="poll" type="number" min="250" step="250" value="1000" />
        </div>
      </div>

      <div class="actions">
        <button id="runBtn">Run</button>
        <button id="stopBtn" class="secondary" disabled>Stop polling</button>
        <span id="runId"></span>
      </div>

      <div class="links" id="links" style="display:none; margin-top: 12px;">
        <a id="excelLink" href="#" target="_blank" rel="noopener">Download Excel</a>
        <a id="reportLink" href="#" target="_blank" rel="noopener">View report</a>
      </div>

      <div class="status" id="status">Idle.</div>
      <p style="margin-top:12px;color:#6b7280;">
        API: <code>POST /api/runs</code>, <code>GET /api/runs/&lt;run_id&gt;</code>
      </p>
    </div>

    <script>
      const $ = (id) => document.getElementById(id);
      let pollTimer = null;
      let currentRunId = "";

      function setStatus(obj) {
        $("status").textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
      }

      function setLinks(runId) {
        $("links").style.display = "block";
        $("excelLink").href = `/api/runs/${runId}/excel`;
        $("reportLink").href = `/api/runs/${runId}/report`;
      }

      async function pollOnce() {
        if (!currentRunId) return;
        const res = await fetch(`/api/runs/${currentRunId}`);
        if (!res.ok) {
          setStatus(`Polling failed: HTTP ${res.status}`);
          return;
        }
        const data = await res.json();
        setStatus(data);
        if (data.status && data.status !== "running") {
          setLinks(currentRunId);
          stopPolling();
        }
      }

      function startPolling() {
        stopPolling();
        const interval = Math.max(250, parseInt($("poll").value || "1000", 10));
        $("stopBtn").disabled = false;
        pollTimer = setInterval(pollOnce, interval);
        pollOnce();
      }

      function stopPolling() {
        if (pollTimer) clearInterval(pollTimer);
        pollTimer = null;
        $("stopBtn").disabled = true;
      }

      $("stopBtn").addEventListener("click", stopPolling);

      $("runBtn").addEventListener("click", async () => {
        $("links").style.display = "none";
        currentRunId = "";
        $("runId").textContent = "";
        setStatus("Starting run...");

        const cities = $("cities").value.split("\\n").map(s => s.trim()).filter(Boolean);
        const start_date = $("startDate").value;
        const end_date = $("endDate").value;

        const res = await fetch("/api/runs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ cities, start_date, end_date }),
        });

        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          setStatus({ error: "Failed to start run", status: res.status, detail: data });
          return;
        }
        currentRunId = data.run_id;
        $("runId").textContent = `run_id: ${currentRunId}`;
        startPolling();
      });

      // Defaults: today → +24 months
      (function initDates() {
        const today = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const toISO = (d) => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
        const end = new Date(today);
        end.setMonth(end.getMonth() + 24);
        if (!$("startDate").value) $("startDate").value = toISO(today);
        if (!$("endDate").value) $("endDate").value = toISO(end);
      })();
    </script>
  </body>
</html>
"""


@app.post("/api/runs")
def start_run(req: RunRequest) -> dict[str, Any]:
    cities = [c.strip() for c in (req.cities or []) if c.strip()]
    if settings.TEMP_MAX_CITIES > 0:
        cities = cities[: settings.TEMP_MAX_CITIES]
    if not cities:
        raise HTTPException(status_code=400, detail="cities is required")

    # Log the actual parsed city strings so we can spot UI input issues
    # (e.g. two cities accidentally entered on the same line).
    preview = cities[:6]
    extra = max(0, len(cities) - len(preview))
    logger.info(
        "run_request cities=%s preview=%r extra=%s start_date=%s end_date=%s months=%s",
        len(cities),
        preview,
        extra,
        (req.start_date or "").strip(),
        (req.end_date or "").strip(),
        int(req.months or 24),
    )

    start_date = (req.start_date or "").strip()
    end_date = (req.end_date or "").strip()
    months = int(req.months or 24)
    if start_date and end_date:
        try:
            s = datetime.fromisoformat(start_date).date()
            e = datetime.fromisoformat(end_date).date()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="start_date/end_date must be YYYY-MM-DD") from exc
        if e < s:
            s, e = e, s
        start_date = s.isoformat()
        end_date = e.isoformat()
        months = max(1, int(round((e - s).days / 30.0)))
    elif start_date or end_date:
        raise HTTPException(status_code=400, detail="Provide both start_date and end_date, or neither")

    run_id = str(uuid.uuid4())
    state = RunState(
        run_id=run_id,
        started_at=_utc_now_iso(),
        months=months,
        start_date=start_date,
        end_date=end_date,
        cities_requested=cities,
        status="running",
    )
    with _LOCK:
        _RUNS[run_id] = state
    threading.Thread(target=_run_job, args=(state,), daemon=True).start()
    return {"run_id": run_id}


@app.get("/api/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    with _LOCK:
        state = _RUNS.get(run_id)
    if not state:
        raise HTTPException(status_code=404, detail="run not found")
    return {
        "run_id": state.run_id,
        "started_at": state.started_at,
        "finished_at": state.finished_at,
        "months": state.months,
        "start_date": state.start_date,
        "end_date": state.end_date,
        "cities_requested": state.cities_requested,
        "status": state.status,
        "error": state.error,
        "excel_path": state.excel_path,
        "report_path": state.report_path,
        "city_results": state.city_results,
    }


@app.get("/api/runs/{run_id}/excel")
def download_excel(run_id: str) -> FileResponse:
    with _LOCK:
        state = _RUNS.get(run_id)
    if not state:
        raise HTTPException(status_code=404, detail="run not found")
    if state.status == "running":
        raise HTTPException(status_code=409, detail="run still in progress")
    excel_path = state.excel_path
    if not excel_path:
        candidate = Path(settings.RESULT_DIR) / f"{run_id}_places.xlsx"
        excel_path = str(candidate) if candidate.exists() else ""
    if not excel_path or not Path(excel_path).exists():
        raise HTTPException(status_code=404, detail="excel not found")
    return FileResponse(excel_path, filename=Path(excel_path).name)


@app.get("/api/runs/{run_id}/report")
def download_report(run_id: str) -> JSONResponse:
    with _LOCK:
        state = _RUNS.get(run_id)
    if not state:
        raise HTTPException(status_code=404, detail="run not found")
    if not state.report_path or not Path(state.report_path).exists():
        raise HTTPException(status_code=404, detail="report not found")
    payload = json.loads(Path(state.report_path).read_text(encoding="utf-8"))
    return JSONResponse(payload)
