# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.2-beta.0] - 2025-04-01

### Added

- **SQL Recorder** — transparent query capture across web requests and background jobs via `frappe.db.sql` patching, with Redis-based aggregation
- **Index Manager** — automated index optimization pipeline: candidate generation from WHERE/ORDER BY clauses, index creation, backtesting via `ANALYZE`, and removal of ineffective indexes
- **DocType Flow Tracer** — traces document lifecycle chains to visualize DocType creation cascades
- **MariaDB Query** DocType with `EXPLAIN EXTENDED` results, parameterized query deduplication, and occurrence tracking
- **MariaDB Table** DocType with read/write categorization, table existence checks, and `ANALYZE`/`OPTIMIZE` table actions
- **MariaDB Index** virtual DocType providing live views of database indexes via `INFORMATION_SCHEMA`
- **SQL Record Summary** DocType for tracking query processing batches
- **ToolBox Settings** with toggle controls for SQL Recorder and Index Manager, configurable processing intervals (Hourly/Daily)
- **Scheduled jobs** for automated SQL processing and index optimization
- **Unused MariaDB Indexes** report
- **Index Manager** and **Site Manager** dashboards
- **CLI commands**: `sql-recorder` (start/stop/drop), `sql-manager` (process/cleanup), `index-manager` (optimize/show/drop), `doctype-manager` (trace on/off/draw/status/purge)
- **Bulk insert** for efficient batch recording of query metadata
- **CI pipeline** testing against Frappe v15, v16, and develop branches

### Fixed

- **SQL injection prevention** — parameterized filter clauses and validated identifiers in all DDL operations
- **Table name validation** — regex-based validation before `ANALYZE`/`OPTIMIZE` SQL execution
- **Typo fix** — renamed `occurence` to `occurrence` across the codebase
- `process_sql_metadata_chunk` always returns a summary document
- `MariaDBIndex.get_count` extra kwarg error
- Scheduled jobs management reliability
- Shared list reference bug in query processing
- Query building/ordering strip comma fix

### Changed

- Standardized Redis API usage to `frappe.cache` property access
- Decomposed `process_sql_metadata_chunk` into smaller, testable functions
- Moved `process_index_manager` to dedicated module
- Replaced `print()` calls with `frappe.logger("toolbox")` throughout pipeline

[0.0.2-beta.0]: https://github.com/gavindsouza/toolbox/releases/tag/v0.0.2-beta.0
