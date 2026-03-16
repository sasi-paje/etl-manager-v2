"""
ETL Daemon — runs as a background process, scheduling all enabled ETLs.

Start with: python -m etl_manager.daemon
The CLI manages this process via a PID file.
"""

import os
import sys
import time
import logging
import signal
import json
from datetime import datetime
from pathlib import Path

from etl_manager.state import load_state, update_etl_fields, STATE_FILE
from etl_manager.runner import ETLRunner

DAEMON_PID_FILE = Path(__file__).parent.parent / "daemon.pid"
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "daemon.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Track last execution time per ETL
_last_run: dict[str, float] = {}
_running = True


def handle_shutdown(signum, frame):
    global _running
    logger.info("Daemon shutting down (signal %s)...", signum)
    _running = False


def build_db_configs(etl) -> tuple[dict, dict]:
    source_host = (etl.source_db or os.environ.get("SOURCE_DB_HOST", "")).strip()
    target_host = (etl.target_db or os.environ.get("TARGET_DB_HOST", "")).strip()

    source_db = f"argus_{etl.argus_id}"
    target_db = f"webapp_{etl.argus_id}"

    source_config = {
        "host": source_host,
        "user": os.environ["SOURCE_DB_USER"],
        "password": os.environ["SOURCE_DB_PASSWORD"],
        "db": source_db,
    }
    target_config = {
        "host": target_host,
        "user": os.environ["TARGET_DB_USER"],
        "password": os.environ["TARGET_DB_PASSWORD"],
        "db": target_db,
    }
    return source_config, target_config


def run_etl(argus_id: str) -> None:
    state = load_state()
    etl = state.get(argus_id)
    if not etl:
        logger.warning("ETL %s not found in state.", argus_id)
        return

    logger.info("[%s] Starting ETL run...", argus_id)
    update_etl_fields(argus_id, status="running")

    try:
        source_config, target_config = build_db_configs(etl)
        runner = ETLRunner(argus_id, source_config, target_config)
        results = runner.run()

        errors = {t: msg for t, msg in results.items() if msg != "ok"}
        now = datetime.now().isoformat()

        if errors:
            update_etl_fields(
                argus_id,
                status="error",
                last_run=now,
                last_run_status="partial",
                last_error=json.dumps(errors),
            )
            logger.warning("[%s] Finished with errors: %s", argus_id, errors)
        else:
            update_etl_fields(
                argus_id,
                status="stopped",
                last_run=now,
                last_run_status="success",
                last_error=None,
            )
            logger.info("[%s] Finished successfully.", argus_id)

    except Exception as e:
        update_etl_fields(
            argus_id,
            status="error",
            last_run=datetime.now().isoformat(),
            last_run_status="failed",
            last_error=str(e),
        )
        logger.error("[%s] Fatal error: %s", argus_id, e)


def write_pid() -> None:
    DAEMON_PID_FILE.write_text(str(os.getpid()))


def clear_pid() -> None:
    if DAEMON_PID_FILE.exists():
        DAEMON_PID_FILE.unlink()


def main() -> None:
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    write_pid()
    logger.info("ETL Daemon started (PID=%s).", os.getpid())

    try:
        while _running:
            state = load_state()
            now = time.time()

            for argus_id, etl in state.items():
                if not etl.enabled:
                    continue

                interval_seconds = etl.interval_minutes * 60
                last = _last_run.get(argus_id, 0)

                if now - last >= interval_seconds:
                    _last_run[argus_id] = now
                    run_etl(argus_id)

            time.sleep(10)  # poll every 10 seconds

    finally:
        clear_pid()
        logger.info("Daemon stopped.")


if __name__ == "__main__":
    main()
