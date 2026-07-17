import os
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from etl_manager.daemon import MissingDatabaseConfigError, build_db_configs, should_run_etl


class BuildDbConfigsTest(unittest.TestCase):
    def test_builds_mysql_source_and_postgres_target_configs(self):
        env = {
            "SOURCE_DB_HOST": "mysql.example.test",
            "SOURCE_DB_USER": "source_user",
            "SOURCE_DB_PASSWORD": "source_secret",
            "TARGET_DB_HOST": "db.supabase.test",
            "TARGET_DB_PORT": "6543",
            "TARGET_DB_NAME": "postgres",
            "TARGET_DB_USER": "postgres",
            "TARGET_DB_PASSWORD": "target_secret",
            "TARGET_DB_SSLMODE": "require",
        }
        etl = SimpleNamespace(argus_id="110760000549", source_db="", target_db="")

        with patch.dict(os.environ, env, clear=True):
            source_config, target_config = build_db_configs(etl)

        self.assertEqual(source_config["db"], "argus_110760000549")
        self.assertEqual(source_config["host"], "mysql.example.test")
        self.assertEqual(source_config["connect_timeout"], 5)
        self.assertEqual(target_config["host"], "db.supabase.test")
        self.assertEqual(target_config["port"], 6543)
        self.assertEqual(target_config["dbname"], "postgres")
        self.assertEqual(target_config["schema"], "webapp_110760000549")
        self.assertEqual(target_config["sslmode"], "require")
        self.assertEqual(target_config["connect_timeout"], 5)
        self.assertEqual(target_config["batch_size"], 1000)

    def test_connect_timeout_can_be_overridden_by_env(self):
        env = {
            "SOURCE_DB_HOST": "mysql.example.test",
            "SOURCE_DB_USER": "source_user",
            "SOURCE_DB_PASSWORD": "source_secret",
            "SOURCE_DB_CONNECT_TIMEOUT": "3",
            "TARGET_DB_HOST": "db.supabase.test",
            "TARGET_DB_NAME": "postgres",
            "TARGET_DB_USER": "postgres",
            "TARGET_DB_PASSWORD": "target_secret",
            "TARGET_DB_CONNECT_TIMEOUT": "4",
            "ETL_BATCH_SIZE": "500",
        }
        etl = SimpleNamespace(argus_id="110760000549", source_db="", target_db="")

        with patch.dict(os.environ, env, clear=True):
            source_config, target_config = build_db_configs(etl)

        self.assertEqual(source_config["connect_timeout"], 3)
        self.assertEqual(target_config["connect_timeout"], 4)
        self.assertEqual(target_config["batch_size"], 500)

    def test_target_schema_can_be_overridden_by_env(self):
        env = {
            "SOURCE_DB_HOST": "mysql.example.test",
            "SOURCE_DB_USER": "source_user",
            "SOURCE_DB_PASSWORD": "source_secret",
            "TARGET_DB_HOST": "db.supabase.test",
            "TARGET_DB_NAME": "postgres",
            "TARGET_DB_USER": "postgres",
            "TARGET_DB_PASSWORD": "target_secret",
            "TARGET_DB_SCHEMA": "public",
        }
        etl = SimpleNamespace(argus_id="110760000549", source_db="", target_db="")

        with patch.dict(os.environ, env, clear=True):
            _, target_config = build_db_configs(etl)

        self.assertEqual(target_config["schema"], "public")

    def test_existing_state_target_db_overrides_target_host_env(self):
        env = {
            "SOURCE_DB_HOST": "mysql.example.test",
            "SOURCE_DB_USER": "source_user",
            "SOURCE_DB_PASSWORD": "source_secret",
            "TARGET_DB_HOST": "db.supabase.test",
            "TARGET_DB_NAME": "postgres",
            "TARGET_DB_USER": "postgres",
            "TARGET_DB_PASSWORD": "target_secret",
        }
        etl = SimpleNamespace(argus_id="110760000549", source_db="", target_db="saved-target.test")

        with patch.dict(os.environ, env, clear=True):
            _, target_config = build_db_configs(etl)

        self.assertEqual(target_config["host"], "saved-target.test")

    def test_missing_target_env_fails_before_sync(self):
        env = {
            "SOURCE_DB_HOST": "mysql.example.test",
            "SOURCE_DB_USER": "source_user",
            "SOURCE_DB_PASSWORD": "source_secret",
            "TARGET_DB_HOST": "db.supabase.test",
            "TARGET_DB_USER": "postgres",
            "TARGET_DB_PASSWORD": "target_secret",
        }
        etl = SimpleNamespace(argus_id="110760000549", source_db="", target_db="")

        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(MissingDatabaseConfigError, "TARGET_DB_NAME"):
                build_db_configs(etl)


class ShouldRunEtlTest(unittest.TestCase):
    def test_runs_when_etl_has_never_run(self):
        etl = SimpleNamespace(enabled=True, last_run=None, interval_minutes=60)

        self.assertTrue(should_run_etl(etl, datetime(2026, 7, 13, 13, 0, 0)))

    def test_does_not_run_again_before_persisted_interval_elapsed(self):
        now = datetime(2026, 7, 13, 13, 30, 0)
        etl = SimpleNamespace(
            enabled=True,
            last_run=(now - timedelta(minutes=30)).isoformat(),
            interval_minutes=60,
        )

        self.assertFalse(should_run_etl(etl, now))

    def test_runs_after_persisted_interval_elapsed(self):
        now = datetime(2026, 7, 13, 14, 1, 0)
        etl = SimpleNamespace(
            enabled=True,
            last_run=(now - timedelta(minutes=61)).isoformat(),
            interval_minutes=60,
        )

        self.assertTrue(should_run_etl(etl, now))

    def test_disabled_etl_does_not_run(self):
        etl = SimpleNamespace(enabled=False, last_run=None, interval_minutes=60)

        self.assertFalse(should_run_etl(etl, datetime(2026, 7, 13, 13, 0, 0)))


if __name__ == "__main__":
    unittest.main()
