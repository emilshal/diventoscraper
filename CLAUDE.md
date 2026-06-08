# CLAUDE.md — Divento Zapbot scraper

## What this project is

This repo is the **temporary-exhibitions FastAPI scraper** for "Zapbot" — a Divento internal tool that produces Excel files of exhibitions or attractions for cities the client provides.

Architecture on the production droplet (DigitalOcean, IP `178.128.162.229`):

- **Filament admin UI** (Laravel, at `/var/www/html/diventoscrapper/`) — the "Zapbot" UI the client uses. Served by Apache on port 80. Single Apache vhost, no `ServerName`, no HTTPS.
- **Two cron-driven Python scripts** in `/var/www/html/diventoscrapper/scripts/`:
  - `scrape_destinations.py` — **permanent** attractions, queries MySQL via `pymysql`, hits Google Maps + writes to Excel.
  - `scrape_temporary_destinations.py` — **temporary** exhibitions, dispatches to the FastAPI service via HTTP.
- **FastAPI scraper** (this repo's code, deployed at `/var/www/html/diventotempscrapper/`) — runs as `divento-temp.service` on `127.0.0.1:8000`. Receives city batches, calls OpenAI, writes Excel to `data/`.
- **MySQL** holds the `destinations` queue table. Both cron scripts poll it.

The Filament form writes a row to `destinations` with `job_type='permanent'|'temporary'`. The two cron scripts race to claim `status='new'` rows.

## Active incident (started 2026-05-13)

**The droplet is compromised.** A snapshot was taken via DigitalOcean for evidence; client was informed. Findings on the box:

- Multiple PHP web shells in `public/` and `storage/` (e.g. `bootstrap.cache.php`, `78k.php`, `kozlakola.php`, `okwokwaokw.php`)
- `www-data` crontab full of base64-encoded bash watchdogs that re-spawn malware binaries every hour
- Dropped Linux ELF binaries disguised as kernel processes (`systemd-logind-helpers`, `dbus-monitor-srv`, `sess_08fc47c70cd22805f3a981ffdbe2f303`, `stmept`, `udevd-sync`)
- A gs-netcat reverse shell registered as a `@reboot` cron entry
- A `guard.sh` watchdog (every 3 min) that reinstalls anything removed

**Decision: rebuild on a fresh droplet rather than clean in place.**

The droplet only hosts Zapbot — no other apps. Single vhost, single Apache, single FastAPI service. Scope is fully within the contractor's (Emil's) authority.

## Audit results — application code is CLEAN

Everything we pulled off the droplet went into `/Users/emilshalamberidze/Desktop/divento-scrapper 2/untrusted-from-compromised-box/`. All known malware is now in `quarantine/` there.

Verified clean:

- Filament admin PHP (`app/`, `routes/`, `config/`, providers) — no eval/shell_exec/base64 injection
- Both Python scrapers — no code injection patterns outside of vendored library code in `venv/`
- MySQL `users` table — only standard system accounts, no backdoor users
- MySQL schema — no rogue tables, no triggers, no stored procedures
- FastAPI `app/ui.py` deployed vs local Git — diff is benign version drift (deployed is older)

**The compromise was infrastructure-level (server foothold, miner, reverse shell), not supply-chain-level. Application logic was not tampered with.**

## Root cause of the original Zapbot bug

The client reported: Brussels "downloaded as permanent" despite selecting temporary; Barcelona / Lisbon failed with `(2013, 'Lost connection to MySQL server during query')` and produced no Excel.

Root cause found in `code/diventoscrapper/scripts/scrape_destinations.py` around line 1620:

```python
#status='new' AND
…
"SELECT id, destination_name, minimum_reviews "
"FROM destinations WHERE status='new' LIMIT 1"
```

The `job_type='permanent'` filter was commented out (note the dangling `#status='new' AND` line). The permanent scraper picks up ANY row with `status='new'`, including temporary ones. Both crons run every minute → race. When the permanent script wins, the temporary row gets processed as permanent.

The MySQL `(2013)` drops on Barcelona/Lisbon are consistent with the box's 2vCPU/4GB resources being eaten by the malware miner. The temp scraper crashed mid-run and there's no stale-job recovery, so those two rows are stuck at `status='processing'` forever.

Confirmed by examining row data in the MySQL dump: Brussels row has `job_type='temporary'` (so the UI badge was correct), but its filename is `brussels-expos-...xlsx` (no `temp_` prefix) and `temp_run_id IS NULL` — both signatures of the permanent script having processed it.

## Fix to apply on the new droplet

1. **One-line fix** in `scrape_destinations.py`:
   ```python
   "FROM destinations WHERE job_type='permanent' AND status='new' LIMIT 1"
   ```
2. **Add stale-job recovery**: any row in `status='processing'` for more than N minutes with no FastAPI progress → reset to `status='error'` with an explanatory `error_message`.
3. **Filament form** (`app/Filament/Resources/ScrapperResource.php` line 41): remove `->default('permanent')` or change it. The form pre-selects "Permanent" on every visit, which is a UX trap.

## Rebuild plan

- Provision a **fresh** DigitalOcean droplet — bigger than `2vCPU/4GB` (the old size was undersized once the miner was on board).
- Configure Apache + MySQL + Python venvs from scratch. Do **not** rsync from the old droplet.
- Deploy FastAPI scraper from this Git repo.
- Deploy Filament admin from `untrusted-from-compromised-box/code/diventoscrapper/` (audited clean). Bring up a Git repo for it as part of the move.
- Set up a **proper domain** (per client feedback — she said the URL should not be "numerals"). Create DNS A record, set Apache `ServerName`, issue Let's Encrypt cert, force HTTPS.
- Restore MySQL data from `untrusted-from-compromised-box/db/data.sql` after applying schema from `schema.sql`.
- Rotate **every** secret — MySQL root, OpenAI API key, Laravel `APP_KEY`, admin password.
- Cut DNS over, smoke-test, decommission old droplet (keep its DO snapshot for 30 days).

## Key paths

- This worktree (clean FastAPI scraper code): `/Users/emilshalamberidze/Desktop/divento-scrapper 2/.claude/worktrees/keen-hamilton-2a0db9`
- Pulled-down artifacts (audited): `/Users/emilshalamberidze/Desktop/divento-scrapper 2/untrusted-from-compromised-box/`
  - `code/` — three deployed trees from the droplet
  - `db/` — MySQL dumps (`schema.sql`, `data.sql`, `users.txt`)
  - `data/` — scraper outputs and logs
  - `quarantine/` — all known malware drops (11 files, dormant on macOS)
- Old droplet: `178.128.162.229` — still running with snapshot taken. Do not redeploy here.

## Safety notes

- Treat the OpenAI API key in any old `.env` as compromised — it must be rotated, not reused.
- Do not execute anything from `untrusted-from-compromised-box/quarantine/`.
- Do not blindly trust `untrusted-from-compromised-box/code/diventoscrapper/` for redeploy — it's audited clean but the `routes/web.php` should be visually compared to git history if one becomes available.
