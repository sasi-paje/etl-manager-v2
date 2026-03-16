"""ETL runner: executes the database sync for a given argus_id."""

import os
import sys
import logging
from datetime import datetime
from typing import Optional

import pymysql

logger = logging.getLogger(__name__)


class ETLRunner:
    """Runs the full ETL sync from argus_* to webapp_* database."""

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
        self.source_conn = pymysql.connect(**self.source_db_config)
        self.target_conn = pymysql.connect(**self.target_db_config)

    def disconnect(self) -> None:
        for conn in [self.source_conn, self.target_conn]:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def run(self) -> dict[str, str]:
        """Run all table syncs. Returns a dict of table -> 'ok' | error message."""
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
                except Exception as e:
                    results[table] = str(e)
                    logger.error("[%s] Error syncing %s: %s", self.argus_id, table, e)

        finally:
            self.disconnect()

        return results

    def _sync_table(self, src, tgt, source_db: str, target_db: str, table: str) -> None:
        """Dispatch to the correct sync strategy for each table."""
        if table == "alerts":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, uuid, sasiAPIId, status, `text`, priority, test, anonymous, location, channel, groupId, mobileAlertPanel, client, site, meta, generatedAt, sasiUpdatedAt, sasiCreateddAt, createdAt, updatedAt, schema_version, nearSite")

        elif table == "channels":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, sasiAPIId, name, channelType, groupid, fsm_id, `primary`, clientId, sasiUpdatedAt, createdAt, updatedAt, channel_priority")

        elif table == "channel_status":
            self._sync_truncate_reload(src, tgt, source_db, target_db, table,
                columns="channelID, status, createdAt, updatedAt")

        elif table == "flow":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, `default`, name, nicename, createdAt, updatedAt, mode, description")

        elif table == "flow_states":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, `default`, name, nicename, icon, color, flow_id, message_id, createdAt, updatedAt, active")

        elif table == "sites":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, sasiAPIId, name, siteType, code, tag, referencePoint, radius, clientId, lat, lon, sasiUpdatedAt, createdAt, updatedAt, location")

        elif table == "clients":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, sasiAPIId, name, active, nickname, sasiUpdatedAt, createdAt, updatedAt")

        elif table == "groups":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, name, sasiAPIId, createdAt, updatedAt",
                target_table="`groups`")

        elif table == "maps":
            self._sync_by_max_id(src, tgt, source_db, target_db, table,
                columns="id, sasiAPIId, name, email, phone, profileProps, customProps, clientId, siteId, appVersion, sasiUpdatedAt, createdAt, updatedAt")

    def _sync_by_max_id(
        self, src, tgt, source_db: str, target_db: str,
        table: str, columns: str, target_table: Optional[str] = None,
    ) -> None:
        target_table = target_table or table
        placeholders = ", ".join(["%s"] * len(columns.split(",")))

        tgt.execute(f"SELECT MAX(id) FROM {target_db}.{target_table}")
        max_id = tgt.fetchone()[0] or 0

        src.execute(f"SELECT {columns} FROM {source_db}.{table} WHERE id > %s", (max_id,))
        rows = src.fetchall()

        for row in rows:
            tgt.execute(
                f"INSERT INTO {target_db}.{target_table} ({columns}) VALUES ({placeholders})",
                row,
            )

    def _sync_truncate_reload(
        self, src, tgt, source_db: str, target_db: str, table: str, columns: str,
    ) -> None:
        placeholders = ", ".join(["%s"] * len(columns.split(",")))
        tgt.execute(f"TRUNCATE TABLE {target_db}.{table}")

        src.execute(f"SELECT {columns} FROM {source_db}.{table}")
        rows = src.fetchall()

        for row in rows:
            tgt.execute(
                f"INSERT INTO {target_db}.{table} ({columns}) VALUES ({placeholders})",
                row,
            )
