import unittest

from etl_manager.runner import ETLRunner, TABLE_SPECS


class FakeCursor:
    def __init__(self, fetchone_result=None, fetchall_result=None):
        self.fetchone_result = fetchone_result
        self.fetchall_result = fetchall_result or []
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, params_seq):
        self.executed.append((sql, params_seq))

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.commits = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1


class RunnerSqlTest(unittest.TestCase):
    def make_runner(self):
        return ETLRunner(
            "110760000549",
            {"host": "mysql", "user": "u", "password": "p", "db": "argus_110760000549"},
            {"host": "pg", "user": "u", "password": "p", "dbname": "postgres", "schema": "public"},
        )

    def test_incremental_sync_renders_mysql_source_and_postgres_target_sql(self):
        runner = self.make_runner()
        src = FakeCursor(fetchall_result=[(2, False, "name", "nice", None, None, "mode", "desc")])
        tgt = FakeCursor(fetchone_result=(1,))

        runner._sync_by_max_id(src, tgt, "argus_110760000549", TABLE_SPECS["flow"])

        self.assertIn('SELECT MAX("id") FROM "public"."flow"', tgt.executed[0][0])
        self.assertIn("`default`", src.executed[0][0])
        self.assertIn("`argus_110760000549`.`flow`", src.executed[0][0])
        self.assertIn('"default"', tgt.executed[1][0])
        self.assertIn('"createdAt"', tgt.executed[1][0])

    def test_truncate_reload_uses_postgres_schema_qualified_table(self):
        runner = self.make_runner()
        src = FakeCursor(fetchall_result=[])
        tgt = FakeCursor()

        runner._sync_truncate_reload(src, tgt, "argus_110760000549", TABLE_SPECS["channel_status"])

        self.assertEqual(tgt.executed[0][0], 'TRUNCATE TABLE "public"."channel_status"')
        self.assertIn("`channelID`", src.executed[0][0])

    def test_table_preparation_creates_missing_postgres_table(self):
        runner = self.make_runner()
        cursor = FakeCursor(fetchone_result=None)
        conn = FakeConnection(cursor)
        runner.target_conn = conn

        runner._ensure_target_tables_exist()

        create_statements = [sql for sql, _ in cursor.executed if sql.startswith("CREATE TABLE")]
        self.assertTrue(any('"public"."alerts"' in sql for sql in create_statements))
        self.assertTrue(any('"sasiAPIId" TEXT' in sql for sql in create_statements))
        self.assertGreater(conn.commits, 0)

    def test_existing_lowercase_columns_are_renamed_to_canonical_case(self):
        runner = self.make_runner()
        cursor = FakeCursor(fetchall_result=[("channelid", "bigint"), ("status", "text"), ("createdat", "timestamp with time zone"), ("updatedat", "timestamp with time zone")])
        conn = FakeConnection(cursor)
        runner.target_conn = conn

        runner._ensure_target_columns_exist(TABLE_SPECS["channel_status"])

        statements = [sql for sql, _ in cursor.executed]
        self.assertIn('ALTER TABLE "public"."channel_status" RENAME COLUMN "channelid" TO "channelID"', statements)
        self.assertIn('ALTER TABLE "public"."channel_status" RENAME COLUMN "createdat" TO "createdAt"', statements)
        self.assertIn('ALTER TABLE "public"."channel_status" RENAME COLUMN "updatedat" TO "updatedAt"', statements)
        self.assertEqual(conn.commits, 1)

    def test_zero_mysql_dates_are_normalized_to_null_for_postgres_timestamps(self):
        runner = self.make_runner()
        table_spec = TABLE_SPECS["clients"]
        row = (1, "api", "client", 1, "nick", "0000-00-00 00:00:00", "0000-00-00", None)

        normalized = runner._normalize_row(row, table_spec)

        self.assertEqual(normalized[:5], row[:5])
        self.assertIsNone(normalized[5])
        self.assertIsNone(normalized[6])
        self.assertIsNone(normalized[7])

    def test_smallint_values_are_normalized_for_existing_postgres_boolean_columns(self):
        runner = self.make_runner()
        runner.target_column_types = {"alerts": {"test": "boolean", "anonymous": "boolean"}}
        table_spec = TABLE_SPECS["alerts"]
        row = (
            1,
            "uuid",
            "api",
            "status",
            "text",
            1,
            1,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )

        normalized = runner._normalize_row(row, table_spec)

        self.assertIs(normalized[6], True)
        self.assertIs(normalized[7], False)


if __name__ == "__main__":
    unittest.main()
