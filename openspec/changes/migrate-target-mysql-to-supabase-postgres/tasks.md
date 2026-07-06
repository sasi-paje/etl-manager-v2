## 1. Configuration and Dependencies

- [x] 1.1 Add a PostgreSQL driver dependency while keeping `pymysql` for source MySQL reads.
- [x] 1.2 Define Supabase/PostgreSQL target environment variables in `.env.example`.
- [x] 1.3 Update daemon target config building to read Supabase/PostgreSQL host, port, database, user, password, schema, and SSL settings.
- [x] 1.4 Preserve compatibility with existing ETL state fields or migrate unclear field names such as `target_db` without losing registered ETLs.

## 2. Runner Target Refactor

- [x] 2.1 Refactor `ETLRunner` connection state so source uses a MySQL connection and target uses a PostgreSQL connection.
- [x] 2.2 Remove target-side MySQL database creation and replace it with PostgreSQL schema/table preparation.
- [x] 2.3 Add explicit PostgreSQL table definitions for all synchronized tables.
- [x] 2.4 Implement target table existence checks against PostgreSQL metadata.

## 3. SQL Dialect Handling

- [x] 3.1 Replace shared column strings with table metadata that can render MySQL source columns and PostgreSQL target columns separately.
- [x] 3.2 Update incremental target `MAX(id)` queries to use PostgreSQL schema-qualified table references.
- [x] 3.3 Update target inserts to use PostgreSQL-compatible placeholders and quoted identifiers.
- [x] 3.4 Update `channel_status` truncate/reload to use PostgreSQL-compatible truncate syntax.
- [x] 3.5 Add commit and rollback handling so per-table target failures leave the connection usable for later tables.

## 4. Documentation

- [x] 4.1 Update README prerequisites to describe MySQL source and Supabase PostgreSQL target.
- [x] 4.2 Update README configuration examples with the new target variables.
- [x] 4.3 Update CLI help or displayed text that still describes the target as `webapp_<id>` on MySQL.

## 5. Verification

- [x] 5.1 Add or update unit tests for target config construction and missing target environment validation.
- [x] 5.2 Add or update unit tests for SQL rendering of reserved and mixed-case identifiers.
- [x] 5.3 Add or update tests for PostgreSQL table preparation behavior using a mocked target connection.
- [x] 5.4 Run the test suite or the closest available validation command.
- [x] 5.5 Perform a foreground dry validation or documented manual run plan for `etl-manager run <argus_id>` against a non-production Supabase database.
