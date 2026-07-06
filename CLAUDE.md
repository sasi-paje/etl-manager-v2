# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
cp .env.example .env        # fill in DB credentials
```

## Running

```bash
etl-manager --help                          # verify installation
etl-manager daemon start                    # start background daemon
etl-manager ps                              # list all ETLs and status
etl-manager logs <argus_id> --tail 100     # tail logs for a specific ETL
python -m etl_manager.daemon               # run daemon in foreground (useful for debugging)
```

## Architecture

The tool is a Docker-style process manager for syncing MySQL databases from `argus_{id}` (source) → `webapp_{id}` (target) on a per-ID basis.

**Data flow:**
1. `cli.py` — argparse CLI; reads/writes `state.json` for registration commands, spawns `python -m etl_manager.daemon` as a detached subprocess for `daemon start`
2. `daemon.py` — background loop (10s poll); reads `state.json` every tick, fires `run_etl()` for any enabled ETL whose interval has elapsed; writes PID to `daemon.pid` and logs to `logs/daemon.log`
3. `runner.py` — `ETLRunner` does the actual DB work: auto-creates `webapp_{id}` if missing, mirrors table schemas from source (stripping FK/unique/index constraints), then syncs each of the 9 tables
4. `state.py` — `ETLState` dataclass persisted to `state.json`; all reads/writes go through `load_state()`/`save_state()` on every call (no caching)

**Two sync strategies in `ETLRunner`:**
- `_sync_by_max_id`: Appends rows where `id > MAX(id)` in target — used by most tables
- `_sync_truncate_reload`: Full TRUNCATE + reload — used only by `channel_status`

**Runtime files** (repo root, not committed):
- `state.json` — registry of all ETLs
- `daemon.pid` — PID of the running daemon
- `logs/daemon.log` — structured log output

**Note:** The root-level `runner.py`, `daemon.py`, `cli.py`, and `state.py` files are stale copies; the active package is `etl_manager/`.

## Environment variables

All DB credentials are read from `.env` (loaded via `python-dotenv`) or from the environment at daemon startup. The daemon inherits the environment at `daemon start` time — restart the daemon after changing `.env`.

| Variable | Purpose |
|---|---|
| `SOURCE_DB_HOST` | Host for `argus_*` databases |
| `SOURCE_DB_USER` / `SOURCE_DB_PASSWORD` | Source credentials |
| `TARGET_DB_HOST` | Host for `webapp_*` databases |
| `TARGET_DB_USER` / `TARGET_DB_PASSWORD` | Target credentials |

Per-ETL host overrides can be set with `etl-manager add --source-host / --target-host`, stored in `ETLState.source_db` / `target_db`.
