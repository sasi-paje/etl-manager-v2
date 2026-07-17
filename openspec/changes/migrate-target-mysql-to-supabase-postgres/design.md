## Context

The current ETL runner uses `pymysql` for both ends of the sync. It creates the target MySQL database if needed, copies table definitions with `SHOW CREATE TABLE`, sanitizes MySQL DDL, checks `information_schema.tables`, and writes rows with MySQL identifiers and `%s` placeholders.

The requested target is Supabase PostgreSQL. The source remains Argus MySQL/MariaDB, so the implementation must support two database dialects in one run: MySQL reads and PostgreSQL writes. Existing daemon, state, and CLI commands should continue to identify ETLs by Argus ID and run on the configured interval.

## Goals / Non-Goals

**Goals:**

- Connect to the source with MySQL credentials and to the target with Supabase PostgreSQL credentials.
- Create or verify the required target tables in PostgreSQL before syncing.
- Preserve the existing table list and sync strategies.
- Convert current column lists and target SQL to PostgreSQL-safe identifiers and statements.
- Keep the operator workflow simple: `etl-manager-v2 add`, daemon scheduling, foreground `run`, logs, and inspection remain usable.
- Document the new required environment variables.

**Non-Goals:**

- Migrating the source database away from MySQL/MariaDB.
- Replacing the local JSON state store.
- Adding bidirectional sync, deletes, updates, conflict resolution, or CDC.
- Managing Supabase project creation, network allowlisting, or database user provisioning.
- Guaranteeing automatic conversion of arbitrary MySQL DDL beyond the tables synchronized by this project.

## Decisions

1. Use separate source and target connection paths.

   Keep `pymysql` for source reads and add a PostgreSQL driver for target writes. Prefer `psycopg` v3 unless compatibility testing shows the environment requires `psycopg2-binary`.

   Alternative considered: use SQLAlchemy for both databases. That would add a larger abstraction and migration surface than this focused ETL needs.

2. Treat the Supabase database as an existing PostgreSQL database and create schemas/tables inside it.

   Supabase exposes a PostgreSQL database that already exists. The runner must not try to create the database itself. It should create target tables in a configured schema, defaulting to `public`, or verify that they exist.

   Alternative considered: one PostgreSQL schema per `webapp_<argus_id>`. This is useful for isolation but changes how downstream consumers query data. The safer first implementation is a configurable schema with table names matching the existing tables unless the project chooses a per-ETL schema during implementation.

3. Define PostgreSQL target DDL for the managed tables instead of executing sanitized MySQL `SHOW CREATE TABLE` output.

   MySQL DDL is not reliably portable to PostgreSQL because of types, backticks, indexes, collations, engine clauses, and reserved words. The implementation should centralize target table definitions or a small table/column model for the known synchronized tables.

   Alternative considered: build a generic MySQL-to-PostgreSQL DDL converter. That is higher risk and unnecessary for the known table set.

4. Represent table and column identifiers explicitly per dialect.

   Source queries can continue using MySQL backticks for reserved words such as `default`, `primary`, and `text`. Target queries must use PostgreSQL double quotes where needed and avoid `database.table` qualification. A helper should produce target column lists, table references, and insert statements.

   Alternative considered: leave existing column strings and replace backticks with double quotes inline. That is fragile because the same string is used for both source and target dialects.

5. Keep sync semantics append-only except for `channel_status`.

   Existing behavior inserts rows with `id > MAX(id)` for most tables and truncates/reloads `channel_status`. The PostgreSQL implementation should preserve that behavior and commit per table, with rollback on each table failure.

   Alternative considered: use PostgreSQL upsert for all tables. That would be a behavior change because existing rows are not updated today.

## Risks / Trade-offs

- PostgreSQL table definitions may not perfectly match existing MySQL schemas -> mitigate by defining explicit mappings for every synced column and validating against a staging Supabase database.
- Identifier case can drift because PostgreSQL folds unquoted identifiers to lowercase -> mitigate by consistently quoting camelCase and reserved columns in target SQL.
- Large truncate/reload or insert batches may be slow over Supabase connections -> mitigate by batching inserts and keeping current per-table transaction boundaries.
- Existing state uses `target_db` as a host field -> mitigate by preserving backward-compatible state loading while documenting the new meaning or introducing a clearer target host field.
- Supabase SSL requirements may differ by deployment -> mitigate by supporting SSL mode in target config and documenting the expected connection string fields.

## Migration Plan

1. Add the PostgreSQL dependency and target configuration fields.
2. Refactor runner connection setup so MySQL source and PostgreSQL target are separate.
3. Implement target table preparation with PostgreSQL DDL for the supported tables.
4. Update sync SQL generation for PostgreSQL target reads, inserts, truncates, commits, and rollbacks.
5. Update `.env.example` and `README.md` with Supabase target variables.
6. Validate with a foreground `etl-manager-v2 run <argus_id>` against a non-production Supabase database.
7. Run the daemon after foreground validation succeeds.

Rollback is to stop the daemon, restore the prior MySQL-target code/configuration, and point ETLs back to the existing MySQL target. PostgreSQL target tables can remain in Supabase until manually removed.

## Open Questions

- Should each Argus ID write to a separate PostgreSQL schema named `webapp_<argus_id>`, or should all ETLs write to `public` with shared tables?
- Should the implementation preserve mixed-case column names such as `sasiAPIId` and `createdAt`, or normalize target columns to snake_case?
- Should target table creation be automatic in production, or should it run only through an explicit setup command/migration?
