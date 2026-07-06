import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from etl_manager.daemon import MissingDatabaseConfigError, build_db_configs


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
            "TARGET_DB_SCHEMA": "public",
            "TARGET_DB_SSLMODE": "require",
        }
        etl = SimpleNamespace(argus_id="110760000549", source_db="", target_db="")

        with patch.dict(os.environ, env, clear=True):
            source_config, target_config = build_db_configs(etl)

        self.assertEqual(source_config["db"], "argus_110760000549")
        self.assertEqual(source_config["host"], "mysql.example.test")
        self.assertEqual(target_config["host"], "db.supabase.test")
        self.assertEqual(target_config["port"], 6543)
        self.assertEqual(target_config["dbname"], "postgres")
        self.assertEqual(target_config["schema"], "public")
        self.assertEqual(target_config["sslmode"], "require")

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


if __name__ == "__main__":
    unittest.main()
