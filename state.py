"""Persistent state manager for ETL processes."""

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

STATE_FILE = Path(__file__).parent.parent / "state.json"


@dataclass
class ETLState:
    argus_id: str
    source_db: str
    target_db: str
    interval_minutes: int
    enabled: bool = True
    status: str = "stopped"  # running | stopped | error
    last_run: Optional[str] = None
    last_run_status: Optional[str] = None
    last_error: Optional[str] = None
    pid: Optional[int] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


def load_state() -> dict[str, ETLState]:
    if not STATE_FILE.exists():
        return {}
    with open(STATE_FILE, encoding="utf-8") as f:
        raw = json.load(f)
    return {k: ETLState(**v) for k, v in raw.items()}


def save_state(state: dict[str, ETLState]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({k: asdict(v) for k, v in state.items()}, f, indent=2)


def get_etl(argus_id: str) -> Optional[ETLState]:
    return load_state().get(argus_id)


def upsert_etl(etl: ETLState) -> None:
    state = load_state()
    state[etl.argus_id] = etl
    save_state(state)


def remove_etl(argus_id: str) -> bool:
    state = load_state()
    if argus_id not in state:
        return False
    del state[argus_id]
    save_state(state)
    return True


def update_etl_fields(argus_id: str, **kwargs) -> bool:
    state = load_state()
    if argus_id not in state:
        return False
    etl = state[argus_id]
    for key, value in kwargs.items():
        setattr(etl, key, value)
    save_state(state)
    return True
