## Why

The ETL manager currently treats both source and target as MySQL/MariaDB databases, which prevents writing synchronized `webapp_*` data into Supabase PostgreSQL. The target must move to Supabase while preserving the existing Argus MySQL source, daemon scheduling, CLI workflow, and table synchronization behavior.

## What Changes

- **BREAKING**: The ETL target database changes from MySQL/MariaDB to PostgreSQL on Supabase.
- Keep the source database as MySQL/MariaDB using the existing `argus_<id>` naming convention.
- Replace target-side MySQL connection, schema creation, table existence checks, DDL execution, identifiers, and write SQL with PostgreSQL-compatible behavior.
- Add Supabase/PostgreSQL target configuration through environment variables and per-ETL state where appropriate.
- Preserve current sync strategies: incremental `id > MAX(id)` loads for most tables and truncate/reload for `channel_status`.
- Update documentation and examples so operators configure Supabase target credentials instead of MySQL target credentials.

## Capabilities

### New Capabilities

- `supabase-postgres-target`: Defines how ETLs connect to, prepare, and synchronize data into a Supabase PostgreSQL target.

### Modified Capabilities

- None.

## Impact

- Affected code: `etl_manager/runner.py`, `etl_manager/daemon.py`, `etl_manager/cli.py`, `etl_manager/state.py`, `.env.example`, and `README.md`.
- Dependencies: add a PostgreSQL driver such as `psycopg` or `psycopg2`; keep `pymysql` for the MySQL source.
- Data systems: source remains Argus MySQL/MariaDB; target becomes Supabase PostgreSQL.
- Operational impact: existing MySQL target environment variables and documentation must be replaced or mapped to Supabase PostgreSQL connection settings.
