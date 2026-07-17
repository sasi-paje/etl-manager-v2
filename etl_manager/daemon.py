"""ETL daemon package entrypoint."""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from etl_manager.runner import ETLRunner
from etl_manager.state import load_state, update_etl_fields

DAEMON_PID_FILE = Path(__file__).resolve().parent.parent / "daemon.pid"
DAEMON_LOCK_FILE = Path(__file__).resolve().parent.parent / "daemon.lock"
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5
DEFAULT_READ_TIMEOUT_SECONDS = 60
DEFAULT_WRITE_TIMEOUT_SECONDS = 60
DEFAULT_STATEMENT_TIMEOUT_SECONDS = 300
DEFAULT_BATCH_SIZE = 1000
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

_running = True
_lock_file = None


class MissingDatabaseConfigError(RuntimeError):
    pass


def handle_shutdown(signum, frame):
    global _running
    logger.info("Daemon shutting down (signal %s)...", signum)
    _running = False


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise MissingDatabaseConfigError(f"Missing required environment variable: {name}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise MissingDatabaseConfigError(f"Invalid integer environment variable: {name}") from exc
    if value <= 0:
        raise MissingDatabaseConfigError(f"Environment variable must be greater than zero: {name}")
    return value


def build_db_configs(etl) -> tuple[dict, dict]:
    source_host = (etl.source_db or os.environ.get("SOURCE_DB_HOST", "")).strip()
    target_host = (etl.target_db or os.environ.get("TARGET_DB_HOST", "")).strip()
    target_schema = os.environ.get("TARGET_DB_SCHEMA", "").strip() or f"webapp_{etl.argus_id}"
    if not source_host:
        raise MissingDatabaseConfigError("Missing required source database host: SOURCE_DB_HOST")
    if not target_host:
        raise MissingDatabaseConfigError("Missing required target database host: TARGET_DB_HOST")

    source_db = f"argus_{etl.argus_id}"

    source_config = {
        "host": source_host,
        "user": _required_env("SOURCE_DB_USER"),
        "password": _required_env("SOURCE_DB_PASSWORD"),
        "db": source_db,
        "connect_timeout": _env_int("SOURCE_DB_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT_SECONDS),
        "read_timeout": _env_int("SOURCE_DB_READ_TIMEOUT", DEFAULT_READ_TIMEOUT_SECONDS),
        "write_timeout": _env_int("SOURCE_DB_WRITE_TIMEOUT", DEFAULT_WRITE_TIMEOUT_SECONDS),
    }
    target_config = {
        "host": target_host,
        "port": int(os.environ.get("TARGET_DB_PORT", "5432")),
        "dbname": _required_env("TARGET_DB_NAME"),
        "user": _required_env("TARGET_DB_USER"),
        "password": _required_env("TARGET_DB_PASSWORD"),
        "schema": target_schema,
        "sslmode": os.environ.get("TARGET_DB_SSLMODE", "require").strip() or "require",
        "connect_timeout": _env_int("TARGET_DB_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT_SECONDS),
        "statement_timeout": _env_int("TARGET_DB_STATEMENT_TIMEOUT", DEFAULT_STATEMENT_TIMEOUT_SECONDS),
        "batch_size": _env_int("ETL_BATCH_SIZE", DEFAULT_BATCH_SIZE),
    }
    return source_config, target_config


def should_run_etl(etl, now: datetime | None = None) -> bool:
    if not etl.enabled:
        return False
    if not etl.last_run:
        return True

    now = now or datetime.now()
    try:
        last_run = datetime.fromisoformat(etl.last_run)
    except ValueError:
        return True

    elapsed_seconds = (now - last_run).total_seconds()
    return elapsed_seconds >= etl.interval_minutes * 60


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
        errors = {table: msg for table, msg in results.items() if msg != "ok"}
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
    except Exception as exc:
        update_etl_fields(
            argus_id,
            status="error",
            last_run=datetime.now().isoformat(),
            last_run_status="failed",
            last_error=str(exc),
        )
        logger.error("[%s] Fatal error: %s", argus_id, exc)


def write_pid() -> None:
    DAEMON_PID_FILE.write_text(str(os.getpid()), encoding="utf-8")


def clear_pid() -> None:
    if DAEMON_PID_FILE.exists():
        DAEMON_PID_FILE.unlink()


def acquire_daemon_lock() -> bool:
    global _lock_file
    _lock_file = DAEMON_LOCK_FILE.open("a+", encoding="utf-8")
    _lock_file.seek(0)
    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(_lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(_lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        _lock_file.close()
        _lock_file = None
        return False

    write_daemon_heartbeat()
    return True


def write_daemon_heartbeat() -> None:
    if _lock_file is None:
        return
    _lock_file.seek(0)
    _lock_file.truncate()
    _lock_file.write(f"{os.getpid()}\n{datetime.now().isoformat()}")
    _lock_file.flush()


def release_daemon_lock() -> None:
    global _lock_file
    if _lock_file is None:
        return
    try:
        if sys.platform == "win32":
            import msvcrt

            _lock_file.seek(0)
            msvcrt.locking(_lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(_lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        _lock_file.close()
        _lock_file = None
        DAEMON_LOCK_FILE.unlink(missing_ok=True)


def main() -> None:
    load_dotenv(ENV_FILE)
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    if not acquire_daemon_lock():
        logger.error("Another ETL daemon instance is already running.")
        return

    write_pid()
    logger.info("ETL Daemon started (PID=%s).", os.getpid())

    try:
        while _running:
            write_daemon_heartbeat()
            state = load_state()
            now = datetime.now()

            for argus_id, etl in state.items():
                if should_run_etl(etl, now):
                    run_etl(argus_id)

            time.sleep(10)
    except Exception:
        logger.exception("Daemon crashed with an unexpected error.")
        raise
    finally:
        clear_pid()
        release_daemon_lock()
        logger.info("Daemon stopped.")


if __name__ == "__main__":
    main()
