from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
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


class RunRequest(BaseModel):
    cities: list[str] = Field(default_factory=list)
    months: int = 24


@dataclass
class RunState:
    run_id: str
    started_at: str
    finished_at: str = ""
    months: int = 24
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

        frames: list[pd.DataFrame] = []
        city_results: list[dict[str, Any]] = []

        for raw_city in state.cities_requested:
            t_city = time.monotonic()
            try:
                df = scrape_temporary_exhibitions(raw_city, months=state.months)
                frames.append(df)
                exhibitions = df.to_dict(orient="records")
                status = "ok" if exhibitions else "empty"
                err = ""
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
        target = Path(settings.RESULT_DIR) / f"{state.run_id}_places.xlsx"
        with pd.ExcelWriter(target, engine="openpyxl") as writer:
            df_all.to_excel(writer, sheet_name="Exhibitions", index=False)
            combinations.to_excel(writer, sheet_name="Combinations", index=False)

        state.excel_path = str(target)
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
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
      h1 { margin: 0 0 12px; }
      .card { max-width: 820px; padding: 16px; border: 1px solid #e5e7eb; border-radius: 12px; }
      label { display: block; font-weight: 600; margin: 14px 0 6px; }
      textarea, input { width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 10px; font-size: 14px; }
      textarea { min-height: 96px; resize: vertical; }
      .row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
      .actions { display: flex; gap: 10px; align-items: center; margin-top: 14px; }
      button { padding: 10px 14px; border-radius: 10px; border: 1px solid #111827; background: #111827; color: white; font-weight: 600; cursor: pointer; }
      button.secondary { background: white; color: #111827; border-color: #d1d5db; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .status { margin-top: 14px; padding: 10px 12px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 10px; white-space: pre-wrap; }
      .links a { margin-right: 12px; }
      code { background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }
    </style>

    <div class="card">
      <h1>Divento Scraper v2</h1>
      <div class="row">
        <div>
          <label for="cities">Cities (one per line)</label>
          <textarea id="cities">London, UK</textarea>
        </div>
        <div>
          <label for="months">Months</label>
          <input id="months" type="number" min="1" max="60" value="24" />
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
        const months = parseInt($("months").value || "24", 10);

        const res = await fetch("/api/runs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ cities, months }),
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
    </script>
  </body>
</html>
"""


@app.post("/api/runs")
def start_run(req: RunRequest) -> dict[str, Any]:
    cities = [c.strip() for c in (req.cities or []) if c.strip()]
    if not cities:
        raise HTTPException(status_code=400, detail="cities is required")
    run_id = str(uuid.uuid4())
    state = RunState(
        run_id=run_id,
        started_at=_utc_now_iso(),
        months=int(req.months),
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
    if not state.excel_path or not Path(state.excel_path).exists():
        raise HTTPException(status_code=404, detail="excel not found")
    return FileResponse(state.excel_path, filename=Path(state.excel_path).name)


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
