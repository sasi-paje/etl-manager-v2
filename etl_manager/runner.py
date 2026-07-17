"""ETL runner package module."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

import pymysql

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Column, ...]
    strategy: str = "incremental"


def _q_mysql(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _q_pg(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) + chr(34))}"'


def _pg_table_ref(schema: str, table: str) -> str:
    return f"{_q_pg(schema)}.{_q_pg(table)}"


TABLE_SPECS: dict[str, TableSpec] = {
    "alerts": TableSpec(
        name="alerts",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("uuid", "TEXT"),
            Column("sasiAPIId", "TEXT"),
            Column("status", "TEXT"),
            Column("text", "TEXT"),
            Column("priority", "INTEGER"),
            Column("test", "INTEGER"),
            Column("anonymous", "INTEGER"),
            Column("location", "TEXT"),
            Column("channel", "TEXT"),
            Column("groupId", "BIGINT"),
            Column("mobileAlertPanel", "TEXT"),
            Column("client", "TEXT"),
            Column("site", "TEXT"),
            Column("meta", "TEXT"),
            Column("generatedAt", "TIMESTAMPTZ"),
            Column("sasiUpdatedAt", "TIMESTAMPTZ"),
            Column("sasiCreateddAt", "TIMESTAMPTZ"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
            Column("schema_version", "TEXT"),
            Column("nearSite", "TEXT"),
        ),
    ),
    "channels": TableSpec(
        name="channels",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("sasiAPIId", "TEXT"),
            Column("name", "TEXT"),
            Column("channelType", "TEXT"),
            Column("groupid", "BIGINT"),
            Column("fsm_id", "BIGINT"),
            Column("primary", "INTEGER"),
            Column("clientId", "BIGINT"),
            Column("sasiUpdatedAt", "TIMESTAMPTZ"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
            Column("channel_priority", "INTEGER"),
        ),
    ),
    "channel_status": TableSpec(
        name="channel_status",
        strategy="truncate_reload",
        columns=(
            Column("channelID", "BIGINT"),
            Column("status", "TEXT"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
        ),
    ),
    "flow": TableSpec(
        name="flow",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("default", "INTEGER"),
            Column("name", "TEXT"),
            Column("nicename", "TEXT"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
            Column("mode", "TEXT"),
            Column("description", "TEXT"),
        ),
    ),
    "flow_states": TableSpec(
        name="flow_states",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("default", "INTEGER"),
            Column("name", "TEXT"),
            Column("nicename", "TEXT"),
            Column("icon", "TEXT"),
            Column("color", "TEXT"),
            Column("flow_id", "BIGINT"),
            Column("message_id", "BIGINT"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
            Column("active", "INTEGER"),
        ),
    ),
    "sites": TableSpec(
        name="sites",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("sasiAPIId", "TEXT"),
            Column("name", "TEXT"),
            Column("siteType", "TEXT"),
            Column("code", "TEXT"),
            Column("tag", "TEXT"),
            Column("referencePoint", "TEXT"),
            Column("radius", "DOUBLE PRECISION"),
            Column("clientId", "BIGINT"),
            Column("lat", "DOUBLE PRECISION"),
            Column("lon", "DOUBLE PRECISION"),
            Column("sasiUpdatedAt", "TIMESTAMPTZ"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
            Column("location", "TEXT"),
        ),
    ),
    "clients": TableSpec(
        name="clients",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("sasiAPIId", "TEXT"),
            Column("name", "TEXT"),
            Column("active", "INTEGER"),
            Column("nickname", "TEXT"),
            Column("sasiUpdatedAt", "TIMESTAMPTZ"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
        ),
    ),
    "groups": TableSpec(
        name="groups",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("name", "TEXT"),
            Column("sasiAPIId", "TEXT"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
        ),
    ),
    "maps": TableSpec(
        name="maps",
        columns=(
            Column("id", "BIGINT PRIMARY KEY"),
            Column("sasiAPIId", "TEXT"),
            Column("name", "TEXT"),
            Column("email", "TEXT"),
            Column("phone", "TEXT"),
            Column("profileProps", "TEXT"),
            Column("customProps", "TEXT"),
            Column("clientId", "BIGINT"),
            Column("siteId", "BIGINT"),
            Column("appVersion", "TEXT"),
            Column("sasiUpdatedAt", "TIMESTAMPTZ"),
            Column("createdAt", "TIMESTAMPTZ"),
            Column("updatedAt", "TIMESTAMPTZ"),
        ),
    ),
}


class ETLRunner:
    TABLES = list(TABLE_SPECS)

    def __init__(self, argus_id: str, source_db_config: dict, target_db_config: dict) -> None:
        self.argus_id = argus_id
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config
        self.target_schema = target_db_config.get("schema", "public")
        self.batch_size = int(target_db_config.get("batch_size", 1000))
        self.target_column_types: dict[str, dict[str, str]] = {}
        self.source_conn: Optional[pymysql.Connection] = None
        self.target_conn: Optional[Any] = None

    def connect(self) -> None:
        import psycopg

        self.source_conn = pymysql.connect(**self.source_db_config)
        pg_config = {
            key: value
            for key, value in self.target_db_config.items()
            if key not in {"schema", "batch_size", "statement_timeout"} and value is not None
        }
        pg_config.setdefault("prepare_threshold", None)
        self.target_conn = psycopg.connect(**pg_config)
        self._set_target_statement_timeout()
        self._ensure_target_schema_exists()
        self._ensure_target_tables_exist()

    def _set_target_statement_timeout(self) -> None:
        assert self.target_conn is not None
        timeout_seconds = int(self.target_db_config.get("statement_timeout", 300))
        with self.target_conn.cursor() as cursor:
            cursor.execute("SELECT set_config('statement_timeout', %s, false)", (f"{timeout_seconds}s",))

    def _ensure_target_schema_exists(self) -> None:
        assert self.target_conn is not None
        with self.target_conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_q_pg(self.target_schema)}")
        self.target_conn.commit()

    def _ensure_target_tables_exist(self) -> None:
        assert self.target_conn is not None
        for table_name in self.TABLES:
            table_spec = TABLE_SPECS[table_name]
            if self._table_exists(self.target_conn, self.target_schema, table_name):
                self._ensure_target_columns_exist(table_spec)
            else:
                with self.target_conn.cursor() as cursor:
                    cursor.execute(self._create_table_sql(table_spec))
                self.target_conn.commit()
            self.target_column_types[table_name] = self._existing_target_column_types(table_name)

    def _table_exists(self, connection, schema_name: str, table_name: str) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                LIMIT 1
                """,
                (schema_name, table_name),
            )
            return cursor.fetchone() is not None

    def _create_table_sql(self, table_spec: TableSpec) -> str:
        columns_sql = ",\n    ".join(f"{_q_pg(column.name)} {column.pg_type}" for column in table_spec.columns)
        return f"CREATE TABLE {_pg_table_ref(self.target_schema, table_spec.name)} (\n    {columns_sql}\n)"

    def _ensure_target_columns_exist(self, table_spec: TableSpec) -> None:
        assert self.target_conn is not None
        existing_columns = self._existing_target_columns(table_spec.name)
        changed = False

        for column in table_spec.columns:
            if column.name in existing_columns:
                continue

            existing_by_lower = {name.lower(): name for name in existing_columns}
            case_match = existing_by_lower.get(column.name.lower())
            table_ref = _pg_table_ref(self.target_schema, table_spec.name)
            with self.target_conn.cursor() as cursor:
                if case_match:
                    cursor.execute(f"ALTER TABLE {table_ref} RENAME COLUMN {_q_pg(case_match)} TO {_q_pg(column.name)}")
                    existing_columns.pop(case_match, None)
                    existing_columns[column.name] = column.name
                else:
                    cursor.execute(f"ALTER TABLE {table_ref} ADD COLUMN {_q_pg(column.name)} {self._column_type_for_add(column)}")
                    existing_columns[column.name] = column.name
            changed = True

        if changed:
            self.target_conn.commit()

    def _existing_target_columns(self, table_name: str) -> dict[str, str]:
        return {name: name for name in self._existing_target_column_types(table_name)}

    def _existing_target_column_types(self, table_name: str) -> dict[str, str]:
        assert self.target_conn is not None
        with self.target_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (self.target_schema, table_name),
            )
            return {row[0]: row[1] for row in cursor.fetchall()}

    def _column_type_for_add(self, column: Column) -> str:
        return column.pg_type.replace(" PRIMARY KEY", "")

    def disconnect(self) -> None:
        for conn in [self.source_conn, self.target_conn]:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def run(self) -> dict[str, str]:
        results = {}
        try:
            self.connect()
            assert self.source_conn is not None
            assert self.target_conn is not None
            source_cursor = self.source_conn.cursor()
            target_cursor = self.target_conn.cursor()
            source_db = self.source_db_config["db"]

            for table in self.TABLES:
                try:
                    logger.info("[%s] Syncing table %s...", self.argus_id, table)
                    self._sync_table(source_cursor, target_cursor, source_db, TABLE_SPECS[table])
                    self.target_conn.commit()
                    results[table] = "ok"
                    logger.info("[%s] Synced table %s.", self.argus_id, table)
                except Exception as exc:
                    self.target_conn.rollback()
                    results[table] = str(exc)
                    logger.error("[%s] Error syncing %s: %s", self.argus_id, table, exc)
        finally:
            self.disconnect()
        return results

    def _sync_table(self, src, tgt, source_db: str, table_spec: TableSpec) -> None:
        if table_spec.strategy == "truncate_reload":
            self._sync_truncate_reload(src, tgt, source_db, table_spec)
        else:
            self._sync_upsert_by_id(src, tgt, source_db, table_spec)

    def _source_columns_sql(self, table_spec: TableSpec) -> str:
        return ", ".join(_q_mysql(column.name) for column in table_spec.columns)

    def _target_columns_sql(self, table_spec: TableSpec) -> str:
        return ", ".join(_q_pg(column.name) for column in table_spec.columns)

    def _upsert_sql(self, table_spec: TableSpec) -> str:
        target_table = _pg_table_ref(self.target_schema, table_spec.name)
        target_columns = self._target_columns_sql(table_spec)
        placeholders = ", ".join(["%s"] * len(table_spec.columns))
        update_columns = [column for column in table_spec.columns if column.name != "id"]
        update_sql = ", ".join(f"{_q_pg(column.name)} = EXCLUDED.{_q_pg(column.name)}" for column in update_columns)
        return (
            f"INSERT INTO {target_table} ({target_columns}) VALUES ({placeholders}) "
            f"ON CONFLICT ({_q_pg('id')}) DO UPDATE SET {update_sql}"
        )

    def _target_max_id(self, tgt, table_spec: TableSpec) -> int:
        target_table = _pg_table_ref(self.target_schema, table_spec.name)
        tgt.execute(f"SELECT COALESCE(MAX({_q_pg('id')}), 0) FROM {target_table}")
        row = tgt.fetchone()
        return int(row[0] or 0) if row else 0

    def _sync_upsert_by_id(self, src, tgt, source_db: str, table_spec: TableSpec) -> None:
        source_table = f"{_q_mysql(source_db)}.{_q_mysql(table_spec.name)}"
        columns = self._source_columns_sql(table_spec)
        upsert_sql = self._upsert_sql(table_spec)
        last_id = self._target_max_id(tgt, table_spec)
        total_rows = 0
        batches = 0
        logger.info("[%s] %s: starting after target id %s.", self.argus_id, table_spec.name, last_id)

        while True:
            src.execute(
                f"SELECT {columns} FROM {source_table} WHERE {_q_mysql('id')} > %s ORDER BY {_q_mysql('id')} LIMIT %s",
                (last_id, self.batch_size),
            )
            rows = src.fetchall()
            if not rows:
                break
            tgt.executemany(
                upsert_sql,
                [self._normalize_row(row, table_spec) for row in rows],
            )
            last_id = rows[-1][0]
            total_rows += len(rows)
            batches += 1
            if batches == 1 or batches % 10 == 0:
                logger.info(
                    "[%s] %s: processed %s rows through id %s.",
                    self.argus_id,
                    table_spec.name,
                    total_rows,
                    last_id,
                )

    def _sync_truncate_reload(self, src, tgt, source_db: str, table_spec: TableSpec) -> None:
        target_table = _pg_table_ref(self.target_schema, table_spec.name)
        source_table = f"{_q_mysql(source_db)}.{_q_mysql(table_spec.name)}"
        columns = self._source_columns_sql(table_spec)
        target_columns = self._target_columns_sql(table_spec)
        placeholders = ", ".join(["%s"] * len(table_spec.columns))

        tgt.execute(f"TRUNCATE TABLE {target_table}")
        src.execute(f"SELECT {columns} FROM {source_table}")
        rows = src.fetchall()
        if rows:
            tgt.executemany(
                f"INSERT INTO {target_table} ({target_columns}) VALUES ({placeholders})",
                [self._normalize_row(row, table_spec) for row in rows],
            )
        logger.info("[%s] %s: reloaded %s rows.", self.argus_id, table_spec.name, len(rows))

    def _normalize_row(self, row, table_spec: TableSpec):
        return tuple(self._normalize_value(value, column, table_spec.name) for value, column in zip(row, table_spec.columns))

    def _normalize_value(self, value, column: Column, table_name: str):
        target_type = self.target_column_types.get(table_name, {}).get(column.name)
        if not column.pg_type.startswith(("TIMESTAMP", "TIMESTAMPTZ")):
            if target_type == "boolean" and value in (0, 1):
                return bool(value)
            return value
        if value in ("0000-00-00", "0000-00-00 00:00:00"):
            return None
        if isinstance(value, (date, datetime)) and value.year == 1:
            return None
        return value
