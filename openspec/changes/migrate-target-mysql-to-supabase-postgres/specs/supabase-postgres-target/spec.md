## ADDED Requirements

### Requirement: Supabase PostgreSQL target configuration
The system SHALL configure the ETL target as a Supabase PostgreSQL connection while keeping the source configured as MySQL/MariaDB.

#### Scenario: Build source and target configs
- **WHEN** an ETL run starts for an Argus ID
- **THEN** the source config uses MySQL/MariaDB credentials and database `argus_<id>`
- **AND** the target config uses Supabase PostgreSQL credentials for the configured target database and schema

#### Scenario: Missing target configuration
- **WHEN** required Supabase PostgreSQL target environment variables are missing
- **THEN** the ETL run fails before syncing any table
- **AND** the failure is recorded in the ETL state and daemon logs

### Requirement: PostgreSQL target connection
The system SHALL connect to the target with a PostgreSQL driver and SHALL NOT use the MySQL driver for target operations.

#### Scenario: Connect to target
- **WHEN** the runner opens database connections
- **THEN** it opens the source connection with the MySQL driver
- **AND** it opens the target connection with the PostgreSQL driver

#### Scenario: Target database creation
- **WHEN** the runner prepares the Supabase target
- **THEN** it does not attempt to create the PostgreSQL database itself
- **AND** it prepares only the configured schema and tables inside the existing database

### Requirement: PostgreSQL target table preparation
The system SHALL ensure every supported target table exists in PostgreSQL before table synchronization begins.

#### Scenario: Missing supported table
- **WHEN** a supported target table does not exist
- **THEN** the runner creates the table using PostgreSQL-compatible DDL for the known synchronized columns

#### Scenario: Existing supported table
- **WHEN** a supported target table already exists
- **THEN** the runner reuses the table without dropping existing rows

### Requirement: Dialect-specific SQL generation
The system SHALL generate MySQL-compatible SQL for source reads and PostgreSQL-compatible SQL for target reads and writes.

#### Scenario: Reserved source columns
- **WHEN** source SQL selects columns with MySQL reserved names such as `default`, `primary`, or `text`
- **THEN** the source query uses MySQL-compatible identifier quoting

#### Scenario: Reserved target columns
- **WHEN** target SQL reads or writes columns with PostgreSQL reserved names or mixed-case names
- **THEN** the target query uses PostgreSQL-compatible identifier quoting

#### Scenario: Target table reference
- **WHEN** target SQL references a table
- **THEN** it uses the configured PostgreSQL schema and table name
- **AND** it does not use MySQL-style `database.table` qualification

### Requirement: Existing sync semantics
The system SHALL preserve the current table sync behavior when writing to Supabase PostgreSQL.

#### Scenario: Incremental table sync
- **WHEN** the runner syncs a table that currently uses incremental loading
- **THEN** it reads the current maximum target `id`
- **AND** it inserts source rows with `id` greater than that value into the PostgreSQL target

#### Scenario: Truncate reload table sync
- **WHEN** the runner syncs `channel_status`
- **THEN** it truncates the PostgreSQL target table
- **AND** it reloads all selected source rows

#### Scenario: Per-table failure
- **WHEN** a table sync fails
- **THEN** the runner records the table error
- **AND** it does not report that table as successful

### Requirement: Operator documentation
The system SHALL document how to configure and run ETLs with a Supabase PostgreSQL target.

#### Scenario: Environment example
- **WHEN** an operator opens the environment example file
- **THEN** it shows MySQL source variables and Supabase PostgreSQL target variables

#### Scenario: README usage
- **WHEN** an operator reads the README setup instructions
- **THEN** the target prerequisites and examples describe Supabase PostgreSQL instead of a MySQL target
