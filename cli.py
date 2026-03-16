"""
ETL Manager CLI — Docker-style interface for managing ETL processes.

Usage:
    etl-manager add argus_110760000549 --interval 60
    etl-manager ps
    etl-manager logs argus_110760000549
    etl-manager restart argus_110760000549
    etl-manager stop argus_110760000549
    etl-manager rm argus_110760000549
    etl-manager daemon start
    etl-manager daemon stop
    etl-manager daemon status
    etl-manager run argus_110760000549   # run once immediately
"""

import os
import sys
import subprocess
import signal
import time
import argparse
import json
import platform
from datetime import datetime
from pathlib import Path

from etl_manager.state import (
    ETLState, load_state, upsert_etl, remove_etl,
    update_etl_fields, get_etl,
)

DAEMON_PID_FILE = Path(__file__).parent.parent / "daemon.pid"
LOG_DIR = Path(__file__).parent.parent / "logs"

# ── ANSI colors ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

SYMBOL_OK = "+"
SYMBOL_STOPPED = "-"
SYMBOL_ERROR = "x"


def colored(text: str, color: str) -> str:
    return f"{color}{text}{RESET}"


def status_badge(status: str) -> str:
    badges = {
        "running": colored("* running", GREEN),
        "stopped": colored(f"{SYMBOL_STOPPED} stopped", YELLOW),
        "error":   colored(f"{SYMBOL_ERROR} error",   RED),
    }
    return badges.get(status, status)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize_id(raw: str) -> str:
    """Accept '110760000549' or 'argus_110760000549' → returns just the numeric part."""
    return raw.replace("argus_", "").replace("webapp_", "").strip()


def _get_daemon_pid() -> int | None:
    if not DAEMON_PID_FILE.exists():
        return None
    try:
        return int(DAEMON_PID_FILE.read_text().strip())
    except ValueError:
        return None


def _is_daemon_running() -> bool:
    pid = _get_daemon_pid()
    if pid is None:
        return False
    if platform.system() == "Windows":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        output = result.stdout.strip()
        is_running = bool(output) and "No tasks are running" not in output
        if not is_running:
            DAEMON_PID_FILE.unlink(missing_ok=True)
        return is_running

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        DAEMON_PID_FILE.unlink(missing_ok=True)
        return False


def _time_ago(iso: str | None) -> str:
    if iso is None:
        return "never"
    try:
        delta = datetime.now() - datetime.fromisoformat(iso)
        seconds = int(delta.total_seconds())
        if seconds < 60:
            return f"{seconds}s ago"
        if seconds < 3600:
            return f"{seconds // 60}m ago"
        if seconds < 86400:
            return f"{seconds // 3600}h ago"
        return f"{seconds // 86400}d ago"
    except Exception:
        return iso


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_add(args) -> None:
    numeric_id = _normalize_id(args.argus_id)
    argus_id   = numeric_id
    source_db  = args.source_host or os.environ.get("SOURCE_DB_HOST", "")
    target_db  = args.target_host or os.environ.get("TARGET_DB_HOST", "")

    existing = get_etl(argus_id)
    if existing and not args.force:
        print(colored(f"ETL '{argus_id}' already exists. Use --force to overwrite.", YELLOW))
        return

    etl = ETLState(
        argus_id=argus_id,
        source_db=source_db,
        target_db=target_db,
        interval_minutes=args.interval,
        enabled=True,
        status="stopped",
    )
    upsert_etl(etl)
    print(colored(f"{SYMBOL_OK} ETL '{argus_id}' added (interval: {args.interval}min).", GREEN))
    print(f"  Source DB : argus_{argus_id}  @ {source_db or '(SOURCE_DB_HOST env)'}")
    print(f"  Target DB : webapp_{argus_id} @ {target_db or '(TARGET_DB_HOST env)'}")


def cmd_ps(args) -> None:
    state = load_state()
    if not state:
        print("No ETLs registered. Use: etl-manager add <argus_id>")
        return

    header = f"{'ARGUS ID':<20} {'STATUS':<20} {'INTERVAL':>10} {'LAST RUN':<15} {'LAST STATUS':<12}"
    print(colored(BOLD + header + RESET, BOLD))
    print("─" * 80)

    for argus_id, etl in state.items():
        interval_str = f"{etl.interval_minutes}min"
        last_run     = _time_ago(etl.last_run)
        last_status  = etl.last_run_status or "—"
        status       = status_badge(etl.status)
        enabled_mark = "" if etl.enabled else colored(" [disabled]", YELLOW)

        print(f"{argus_id:<20} {status:<30} {interval_str:>10} {last_run:<15} {last_status:<12}{enabled_mark}")


def cmd_logs(args) -> None:
    argus_id = _normalize_id(args.argus_id)
    log_file = LOG_DIR / "daemon.log"

    if not log_file.exists():
        print("No logs found yet.")
        return

    lines = log_file.read_text(encoding="utf-8").splitlines()
    # Filter by argus_id if it's in the line
    filtered = [l for l in lines if argus_id in l or args.all]

    tail = filtered[-args.tail:]
    for line in tail:
        if "ERROR" in line:
            print(colored(line, RED))
        elif "WARNING" in line:
            print(colored(line, YELLOW))
        else:
            print(line)


def cmd_restart(args) -> None:
    argus_id = _normalize_id(args.argus_id)
    etl = get_etl(argus_id)
    if not etl:
        print(colored(f"ETL '{argus_id}' not found.", RED))
        return

    update_etl_fields(argus_id, enabled=True, status="stopped", last_run=None)
    # Signal the daemon to pick it up immediately by resetting last_run
    print(colored(f"{SYMBOL_OK} ETL '{argus_id}' will run on next daemon cycle.", GREEN))


def cmd_stop(args) -> None:
    argus_id = _normalize_id(args.argus_id)
    if not get_etl(argus_id):
        print(colored(f"ETL '{argus_id}' not found.", RED))
        return
    update_etl_fields(argus_id, enabled=False, status="stopped")
    print(colored(f"{SYMBOL_OK} ETL '{argus_id}' disabled.", YELLOW))


def cmd_enable(args) -> None:
    argus_id = _normalize_id(args.argus_id)
    if not get_etl(argus_id):
        print(colored(f"ETL '{argus_id}' not found.", RED))
        return
    update_etl_fields(argus_id, enabled=True, status="stopped")
    print(colored(f"{SYMBOL_OK} ETL '{argus_id}' enabled.", GREEN))


def cmd_rm(args) -> None:
    argus_id = _normalize_id(args.argus_id)
    if remove_etl(argus_id):
        print(colored(f"{SYMBOL_OK} ETL '{argus_id}' removed.", GREEN))
    else:
        print(colored(f"ETL '{argus_id}' not found.", RED))


def cmd_set_interval(args) -> None:
    argus_id = _normalize_id(args.argus_id)
    if not get_etl(argus_id):
        print(colored(f"ETL '{argus_id}' not found.", RED))
        return
    update_etl_fields(argus_id, interval_minutes=args.interval)
    print(colored(f"{SYMBOL_OK} ETL '{argus_id}' interval set to {args.interval}min.", GREEN))


def cmd_run_once(args) -> None:
    """Run an ETL immediately in the foreground (for testing)."""
    argus_id = _normalize_id(args.argus_id)
    etl = get_etl(argus_id)
    if not etl:
        print(colored(f"ETL '{argus_id}' not found.", RED))
        return

    print(colored(f"Running ETL '{argus_id}' now...", CYAN))

    from etl_manager.daemon import run_etl, build_db_configs
    run_etl(argus_id)
    etl = get_etl(argus_id)
    if etl and etl.last_run_status == "success":
        print(colored(f"{SYMBOL_OK} Done - success!", GREEN))
    else:
        print(colored(f"{SYMBOL_ERROR} Finished with status: {etl.last_run_status}. Check logs.", RED))


def cmd_inspect(args) -> None:
    argus_id = _normalize_id(args.argus_id)
    etl = get_etl(argus_id)
    if not etl:
        print(colored(f"ETL '{argus_id}' not found.", RED))
        return

    from dataclasses import asdict
    data = asdict(etl)
    for key, value in data.items():
        print(f"  {colored(key, CYAN)}: {value}")


# ── Daemon commands ────────────────────────────────────────────────────────────

def cmd_daemon_start(args) -> None:
    if _is_daemon_running():
        print(colored(f"Daemon is already running (PID={_get_daemon_pid()}).", YELLOW))
        return

    python = sys.executable
    module = "etl_manager.daemon"

    proc = subprocess.Popen(
        [python, "-m", module],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        start_new_session=True,
    )
    time.sleep(1)
    if _is_daemon_running():
        print(colored(f"{SYMBOL_OK} Daemon started (PID={_get_daemon_pid()}).", GREEN))
    else:
        print(colored(f"{SYMBOL_ERROR} Daemon failed to start. Check logs/daemon.log", RED))


def cmd_daemon_stop(args) -> None:
    pid = _get_daemon_pid()
    if not pid or not _is_daemon_running():
        print(colored("Daemon is not running.", YELLOW))
        return
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
        print(colored(f"{SYMBOL_OK} Daemon stopped.", GREEN))
    except OSError as e:
        print(colored(f"Failed to stop daemon: {e}", RED))


def cmd_daemon_status(args) -> None:
    if _is_daemon_running():
        print(colored(f"{SYMBOL_OK} Daemon running (PID={_get_daemon_pid()}).", GREEN))
    else:
        print(colored(f"{SYMBOL_STOPPED} Daemon is not running.", YELLOW))


# ── Parser ─────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl-manager",
        description="Docker-style ETL process manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  etl-manager daemon start
  etl-manager add 110760000549 --interval 60
  etl-manager ps
  etl-manager logs 110760000549 --tail 50
  etl-manager restart 110760000549
  etl-manager stop 110760000549
  etl-manager interval 110760000549 30
  etl-manager run 110760000549
  etl-manager rm 110760000549
  etl-manager daemon stop
""",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # add
    p_add = sub.add_parser("add", help="Register a new ETL")
    p_add.add_argument("argus_id", help="e.g. 110760000549 or argus_110760000549")
    p_add.add_argument("--interval", type=int, default=60, help="Interval in minutes (default: 60)")
    p_add.add_argument("--source-host", help="Source DB host (overrides env var)")
    p_add.add_argument("--target-host", help="Target DB host (overrides env var)")
    p_add.add_argument("--force", action="store_true", help="Overwrite existing ETL")

    # ps
    p_ps = sub.add_parser("ps", help="List all ETLs and their status")

    # logs
    p_logs = sub.add_parser("logs", help="Show logs for an ETL (or all)")
    p_logs.add_argument("argus_id", nargs="?", default="", help="ETL ID (omit for all)")
    p_logs.add_argument("--tail", type=int, default=30, help="Number of lines to show")
    p_logs.add_argument("--all", action="store_true", help="Show all ETLs in log")

    # restart
    p_restart = sub.add_parser("restart", help="Restart (re-enable) an ETL")
    p_restart.add_argument("argus_id")

    # stop
    p_stop = sub.add_parser("stop", help="Stop (disable) an ETL")
    p_stop.add_argument("argus_id")

    # enable
    p_enable = sub.add_parser("enable", help="Re-enable a stopped ETL")
    p_enable.add_argument("argus_id")

    # rm
    p_rm = sub.add_parser("rm", help="Remove an ETL from the registry")
    p_rm.add_argument("argus_id")

    # interval
    p_interval = sub.add_parser("interval", help="Change the run interval of an ETL")
    p_interval.add_argument("argus_id")
    p_interval.add_argument("interval", type=int, help="New interval in minutes")

    # run
    p_run = sub.add_parser("run", help="Run an ETL immediately (foreground)")
    p_run.add_argument("argus_id")

    # inspect
    p_inspect = sub.add_parser("inspect", help="Show full details of an ETL")
    p_inspect.add_argument("argus_id")

    # daemon
    p_daemon = sub.add_parser("daemon", help="Control the background daemon")
    daemon_sub = p_daemon.add_subparsers(dest="daemon_command", required=True)
    daemon_sub.add_parser("start",  help="Start the daemon")
    daemon_sub.add_parser("stop",   help="Stop the daemon")
    daemon_sub.add_parser("status", help="Check daemon status")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "add":      cmd_add,
        "ps":       cmd_ps,
        "logs":     cmd_logs,
        "restart":  cmd_restart,
        "stop":     cmd_stop,
        "enable":   cmd_enable,
        "rm":       cmd_rm,
        "interval": cmd_set_interval,
        "run":      cmd_run_once,
        "inspect":  cmd_inspect,
    }

    if args.command == "daemon":
        daemon_dispatch = {
            "start":  cmd_daemon_start,
            "stop":   cmd_daemon_stop,
            "status": cmd_daemon_status,
        }
        daemon_dispatch[args.daemon_command](args)
    else:
        dispatch[args.command](args)


if __name__ == "__main__":
    main()
