"""SQLite-backed run-state store for the permanent venues scraper.

The FastAPI process can be restarted mid-run; this store keeps the run row,
the list of venues discovered in the search phase, and per-venue enrichment
state so a resumed run skips work that's already done.

Design:
- Two tables: `permanent_runs` (one row per dispatched run) and
  `permanent_run_venues` (one row per venue inside a run).
- All access is async via `aiosqlite`. We use a single shared connection
  guarded by an asyncio.Lock — SQLite handles concurrency fine for our
  workload (one run at a time, low write rate).
- `recover_stale_runs()` is called on FastAPI startup. Anything still in a
  mid-run status is either resumed (if `resume_supported` flag is set) or
  flipped to `error`.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

import aiosqlite

# Mid-run statuses. A run sitting in one of these on startup is either
# resumed or marked failed.
MID_RUN_STATUSES = ("queued", "searching", "enriching", "translating", "writing")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS permanent_runs (
    id              TEXT PRIMARY KEY,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    status          TEXT NOT NULL,
    cities          TEXT NOT NULL,           -- json list
    min_reviews     INTEGER NOT NULL,
    target_min      INTEGER NOT NULL,
    target_max      INTEGER NOT NULL,
    current_phase   TEXT,
    current_city    TEXT,
    progress_pct    REAL DEFAULT 0,
    excel_path      TEXT,
    error_message   TEXT,
    result_summary  TEXT                     -- json
);

CREATE INDEX IF NOT EXISTS idx_permanent_runs_status
    ON permanent_runs(status, updated_at);

CREATE TABLE IF NOT EXISTS permanent_run_venues (
    run_id          TEXT NOT NULL,
    venue_id        TEXT NOT NULL,           -- stable hash of name+address
    city            TEXT NOT NULL,
    search_json     TEXT NOT NULL,           -- raw search-phase venue
    enriched_json   TEXT,                    -- after coord check + photo verify
    copy_json       TEXT,                    -- after English long-desc gen
    translated_json TEXT,                    -- after FR/ES/IT/DE/PT
    PRIMARY KEY (run_id, venue_id),
    FOREIGN KEY (run_id) REFERENCES permanent_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_permanent_run_venues_city
    ON permanent_run_venues(run_id, city);
"""


class RunStore:
    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self._conn = await aiosqlite.connect(str(self._db_path))
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _now(self) -> float:
        # `time.time()` is allowed in app code; the workflow-script ban on
        # Date.now/Math.random does not apply to runtime Python.
        return time.time()

    async def create_run(
        self,
        *,
        cities: list[str],
        min_reviews: int,
        target_min: int,
        target_max: int,
    ) -> str:
        assert self._conn is not None
        run_id = uuid.uuid4().hex
        now = self._now()
        async with self._lock:
            await self._conn.execute(
                """
                INSERT INTO permanent_runs (
                    id, created_at, updated_at, status, cities,
                    min_reviews, target_min, target_max,
                    current_phase, current_city, progress_pct
                ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, NULL, NULL, 0)
                """,
                (
                    run_id,
                    now,
                    now,
                    json.dumps(cities),
                    min_reviews,
                    target_min,
                    target_max,
                ),
            )
            await self._conn.commit()
        return run_id

    async def update_run(self, run_id: str, **fields: Any) -> None:
        if not fields:
            return
        assert self._conn is not None
        allowed = {
            "status",
            "current_phase",
            "current_city",
            "progress_pct",
            "excel_path",
            "error_message",
            "result_summary",
        }
        sets: list[str] = []
        values: list[Any] = []
        for k, v in fields.items():
            if k not in allowed:
                raise ValueError(f"update_run: field {k!r} not allowed")
            if k == "result_summary" and v is not None and not isinstance(v, str):
                v = json.dumps(v)
            sets.append(f"{k} = ?")
            values.append(v)
        sets.append("updated_at = ?")
        values.append(self._now())
        values.append(run_id)
        async with self._lock:
            await self._conn.execute(
                f"UPDATE permanent_runs SET {', '.join(sets)} WHERE id = ?",
                tuple(values),
            )
            await self._conn.commit()

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        assert self._conn is not None
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT * FROM permanent_runs WHERE id = ?",
                (run_id,),
            )
            row = await cur.fetchone()
            await cur.close()
        if row is None:
            return None
        return self._row_to_dict(row)

    async def list_runs(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        assert self._conn is not None
        async with self._lock:
            if status is None:
                cur = await self._conn.execute(
                    "SELECT * FROM permanent_runs ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                )
            else:
                cur = await self._conn.execute(
                    "SELECT * FROM permanent_runs WHERE status = ? "
                    "ORDER BY created_at DESC LIMIT ?",
                    (status, limit),
                )
            rows = await cur.fetchall()
            await cur.close()
        return [self._row_to_dict(r) for r in rows]

    def _row_to_dict(self, row: aiosqlite.Row) -> dict[str, Any]:
        d = dict(row)
        for json_field in ("cities", "result_summary"):
            if d.get(json_field):
                try:
                    d[json_field] = json.loads(d[json_field])
                except json.JSONDecodeError:
                    pass
        return d

    # ────────────────────────────────────────────────────────────────────
    # Per-venue checkpoints (for resume after restart)
    # ────────────────────────────────────────────────────────────────────

    async def append_venues(
        self,
        run_id: str,
        city: str,
        venues: list[dict[str, Any]],
    ) -> None:
        """Insert search-phase venues. Idempotent via INSERT OR IGNORE on
        the (run_id, venue_id) primary key, so re-running the search phase
        on a resumed run doesn't blow away enrichment state."""
        if not venues:
            return
        assert self._conn is not None
        rows = [
            (
                run_id,
                v["venue_id"],
                city,
                json.dumps(v),
            )
            for v in venues
        ]
        async with self._lock:
            await self._conn.executemany(
                """
                INSERT OR IGNORE INTO permanent_run_venues
                    (run_id, venue_id, city, search_json)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            await self._conn.commit()

    async def list_venues(
        self,
        run_id: str,
        *,
        city: str | None = None,
    ) -> list[dict[str, Any]]:
        assert self._conn is not None
        async with self._lock:
            if city is None:
                cur = await self._conn.execute(
                    "SELECT * FROM permanent_run_venues WHERE run_id = ?",
                    (run_id,),
                )
            else:
                cur = await self._conn.execute(
                    "SELECT * FROM permanent_run_venues WHERE run_id = ? AND city = ?",
                    (run_id, city),
                )
            rows = await cur.fetchall()
            await cur.close()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            for jf in ("search_json", "enriched_json", "copy_json", "translated_json"):
                if d.get(jf):
                    try:
                        d[jf] = json.loads(d[jf])
                    except json.JSONDecodeError:
                        pass
            out.append(d)
        return out

    async def mark_venue_enriched(
        self,
        run_id: str,
        venue_id: str,
        enriched: dict[str, Any],
    ) -> None:
        await self._set_venue_field(run_id, venue_id, "enriched_json", enriched)

    async def mark_venue_copy(
        self,
        run_id: str,
        venue_id: str,
        copy: dict[str, Any],
    ) -> None:
        await self._set_venue_field(run_id, venue_id, "copy_json", copy)

    async def mark_venue_translated(
        self,
        run_id: str,
        venue_id: str,
        translated: dict[str, Any],
    ) -> None:
        await self._set_venue_field(run_id, venue_id, "translated_json", translated)

    async def _set_venue_field(
        self,
        run_id: str,
        venue_id: str,
        field: str,
        value: dict[str, Any],
    ) -> None:
        assert self._conn is not None
        async with self._lock:
            await self._conn.execute(
                f"UPDATE permanent_run_venues SET {field} = ? "
                "WHERE run_id = ? AND venue_id = ?",
                (json.dumps(value), run_id, venue_id),
            )
            await self._conn.commit()

    # ────────────────────────────────────────────────────────────────────
    # Resume / recovery
    # ────────────────────────────────────────────────────────────────────

    async def find_resumable_runs(self) -> list[dict[str, Any]]:
        """Mid-run rows that can be resumed by the scraper. Caller decides
        whether to actually resume or to flip them to error."""
        assert self._conn is not None
        async with self._lock:
            placeholders = ",".join("?" * len(MID_RUN_STATUSES))
            cur = await self._conn.execute(
                f"SELECT * FROM permanent_runs WHERE status IN ({placeholders}) "
                "ORDER BY created_at ASC",
                MID_RUN_STATUSES,
            )
            rows = await cur.fetchall()
            await cur.close()
        return [self._row_to_dict(r) for r in rows]

    async def mark_stale_as_error(self, max_age_seconds: float) -> int:
        """Flip mid-run rows whose `updated_at` is older than the threshold
        to `status=error`. Returns the count. Use this as a fallback when
        the scraper can't actually resume (e.g. after a long outage)."""
        assert self._conn is not None
        cutoff = self._now() - max_age_seconds
        placeholders = ",".join("?" * len(MID_RUN_STATUSES))
        async with self._lock:
            cur = await self._conn.execute(
                f"UPDATE permanent_runs SET status = 'error', "
                "error_message = COALESCE(error_message, 'service restarted mid-run; not resumable'), "
                "updated_at = ? "
                f"WHERE status IN ({placeholders}) AND updated_at < ?",
                (self._now(), *MID_RUN_STATUSES, cutoff),
            )
            await self._conn.commit()
            return cur.rowcount or 0


# Module-level singleton, configured by ui.py on startup.
_store: RunStore | None = None


def get_store() -> RunStore:
    if _store is None:
        raise RuntimeError("run_store not initialized; call init_store() first")
    return _store


async def init_store(db_path: str | Path) -> RunStore:
    global _store
    if _store is None:
        _store = RunStore(db_path)
        await _store.connect()
    return _store


async def close_store() -> None:
    global _store
    if _store is not None:
        await _store.close()
        _store = None
