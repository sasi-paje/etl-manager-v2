"""ETL runner package module."""

import logging
import re
from typing import Optional

import pymysql

logger = logging.getLogger(__name__)


class ETLRunner:
    TABLES = [
        "alerts",
        "channels",
        "channel_status",
        "flow",
        "flow_states",
        "sites",
        "clients",
        "groups",
        "maps",
    ]

    def __init__(self, argus_id: str, source_db_config: dict, target_db_config: dict) -> None:
        self.argus_id = argus_id
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config
        self.source_conn: Optional[pymysql.Connection] = None
        self.target_conn: Optional[pymysql.Connection] = None

    def connect(self) -> None:
        self._ensure_database_exists(self.target_db_config)
        self.source_conn = pymysql.connect(**self.source_db_config)
        self.target_conn = pymysql.connect(**self.target_db_config)
        self._ensure_target_tables_exist()

    def _ensure_database_exists(self, db_config: dict) -> None:
        admin_config = {key: value for key, value in db_config.items() if key != "db"}
        connection = pymysql.connect(**admin_config)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_config['db']}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            connection.commit()
        finally:
            connection.close()

    def _ensure_target_tables_exist(self) -> None:
        assert self.source_conn is not None
        assert self.target_conn is not None

        source_db = self.source_db_config["db"]
        target_db = self.target_db_config["db"]

        for table in self.TABLES:
            target_table = "groups" if table == "groups" else table
            if self._table_exists(self.target_conn, target_db, target_table):
                continue

            create_sql = self._get_create_table_sql(source_db, table)
            sanitized_sql = self._sanitize_create_table_sql(create_sql, source_db, target_db, table, target_table)

            with self.target_conn.cursor() as cursor:
                cursor.execute(sanitized_sql)
            self.target_conn.commit()

    def _table_exists(self, connection, db_name: str, table_name: str) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s LIMIT 1",
                (db_name, table_name),
            )
            return cursor.fetchone() is not None

    def _get_create_table_sql(self, db_name: str, table_name: str) -> str:
        with self.source_conn.cursor() as cursor:
            cursor.execute(f"SHOW CREATE TABLE `{db_name}`.`{table_name}`")
            row = cursor.fetchone()
            return row[1]

    def _sanitize_create_table_sql(
        self,
        create_sql: str,
        source_db: str,
        target_db: str,
        source_table: str,
        target_table: str,
    ) -> str:
        sql = create_sql
        sql = sql.replace(f"CREATE TABLE `{source_table}`", f"CREATE TABLE `{target_db}`.`{target_table}`")
        sql = sql.replace(f"CREATE TABLE `{source_db}`.`{source_table}`", f"CREATE TABLE `{target_db}`.`{target_table}`")

        lines = []
        for raw_line in sql.splitlines():
            line = raw_line.rstrip()
            stripped = line.strip()

            if stripped.startswith("CONSTRAINT "):
                continue
            if " FOREIGN KEY " in stripped:
                continue
            if stripped.startswith("UNIQUE KEY"):
                continue
            if stripped.startswith("KEY "):
                continue
            if stripped.startswith("FULLTEXT KEY"):
                continue
            if stripped.startswith("SPATIAL KEY"):
                continue

            lines.append(line)

        cleaned_lines = []
        for index, line in enumerate(lines):
            if index < len(lines) - 1:
                next_nonempty_exists = any(next_line.strip() for next_line in lines[index + 1 :])
                if next_nonempty_exists and not line.strip().endswith(",") and line.strip() != ")":
                    if line.strip().startswith("`"):
                        line = f"{line},"
            cleaned_lines.append(line)

        sql = "\n".join(cleaned_lines)
        sql = re.sub(r",\s*\n\)", "\n)", sql)
        return sql

    def disconnect(self) -> None:
        for conn in [self.source_conn, self.target_conn]:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def run(self) -> dict[str, str]:
        results = {}
        self.connect()
        try:
            source_cursor = self.source_conn.cursor()
            target_cursor = self.target_conn.cursor()
            source_db = self.source_db_config["db"]
            target_db = self.target_db_config["db"]

            for table in self.TABLES:
                try:
                    self._sync_table(source_cursor, target_cursor, source_db, target_db, table)
                    self.target_conn.commit()
                    results[table] = "ok"
                except Exception as exc:
                    results[table] = str(exc)
                    logger.error("[%s] Error syncing %s: %s", self.argus_id, table, exc)
        finally:
            self.disconnect()
        return results

    def _sync_table(self, src, tgt, source_db: str, target_db: str, table: str) -> None:
        if table == "alerts":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, uuid, sasiAPIId, status, `text`, priority, test, anonymous, location, channel, groupId, mobileAlertPanel, client, site, meta, generatedAt, sasiUpdatedAt, sasiCreateddAt, createdAt, updatedAt, schema_version, nearSite")
        elif table == "channels":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, sasiAPIId, name, channelType, groupid, fsm_id, `primary`, clientId, sasiUpdatedAt, createdAt, updatedAt, channel_priority")
        elif table == "channel_status":
            self._sync_truncate_reload(src, tgt, source_db, target_db, table, columns="channelID, status, createdAt, updatedAt")
        elif table == "flow":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, `default`, name, nicename, createdAt, updatedAt, mode, description")
        elif table == "flow_states":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, `default`, name, nicename, icon, color, flow_id, message_id, createdAt, updatedAt, active")
        elif table == "sites":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, sasiAPIId, name, siteType, code, tag, referencePoint, radius, clientId, lat, lon, sasiUpdatedAt, createdAt, updatedAt, location")
        elif table == "clients":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, sasiAPIId, name, active, nickname, sasiUpdatedAt, createdAt, updatedAt")
        elif table == "groups":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, name, sasiAPIId, createdAt, updatedAt", target_table="`groups`")
        elif table == "maps":
            self._sync_by_max_id(src, tgt, source_db, target_db, table, columns="id, sasiAPIId, name, email, phone, profileProps, customProps, clientId, siteId, appVersion, sasiUpdatedAt, createdAt, updatedAt")

    def _sync_by_max_id(self, src, tgt, source_db: str, target_db: str, table: str, columns: str, target_table: Optional[str] = None) -> None:
        target_table = target_table or table
        placeholders = ", ".join(["%s"] * len(columns.split(",")))
        tgt.execute(f"SELECT MAX(id) FROM {target_db}.{target_table}")
        max_id = tgt.fetchone()[0] or 0
        src.execute(f"SELECT {columns} FROM {source_db}.{table} WHERE id > %s", (max_id,))
        rows = src.fetchall()
        for row in rows:
            tgt.execute(f"INSERT INTO {target_db}.{target_table} ({columns}) VALUES ({placeholders})", row)

    def _sync_truncate_reload(self, src, tgt, source_db: str, target_db: str, table: str, columns: str) -> None:
        placeholders = ", ".join(["%s"] * len(columns.split(",")))
        tgt.execute(f"TRUNCATE TABLE {target_db}.{table}")
        src.execute(f"SELECT {columns} FROM {source_db}.{table}")
        rows = src.fetchall()
        for row in rows:
            tgt.execute(f"INSERT INTO {target_db}.{table} ({columns}) VALUES ({placeholders})", row)
