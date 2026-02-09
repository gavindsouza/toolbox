<div align="center">

# Toolbox

**Automated database optimization for Frappe sites**

Record SQL queries, analyze access patterns, and generate optimized indexes — all on autopilot.

[![CI](https://github.com/gavindsouza/toolbox/actions/workflows/ci.yml/badge.svg)](https://github.com/gavindsouza/toolbox/actions/workflows/ci.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Frappe](https://img.shields.io/badge/frappe-v15%20|%20v16-blue.svg)](https://frappeframework.com)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

</div>

---

## Key Features

- **SQL Recorder** — Transparently captures all SQL queries across web requests and background jobs with zero application changes
- **Index Manager** — Analyzes recorded queries, generates index candidates, backtests them against real workloads, and drops indexes that don't help
- **DocType Flow Tracer** — Traces document lifecycle chains to visualize which DocTypes trigger creation of other DocTypes
- **Dashboards** — Built-in dashboards for index management and site overview
- **Unused Indexes Report** — Identifies indexes that aren't being used by your workload
- **CLI Tools** — Full CLI for recording, processing, optimizing, and managing indexes

## How It Works

```
Record  →  Analyze  →  Optimize
```

**1. Record** — When enabled, the SQL Recorder monkey-patches `frappe.db.sql` to capture every query executed during web requests and background jobs. Queries are aggregated in Redis with occurrence counts.

**2. Analyze** — A scheduled job processes recorded queries: runs `EXPLAIN EXTENDED` on each, extracts table access patterns, and stores structured results as MariaDB Query / Table / Index documents.

**3. Optimize** — The Index Manager parses WHERE clauses and ORDER BY expressions to generate index candidates, creates them, backtests with `ANALYZE` to measure actual improvement, and drops indexes that don't reduce rows scanned.

## Dashboards

### Workspace
![toolbox-workspace](https://github.com/user-attachments/assets/1b7706d8-38cc-4028-85c2-7cdc03ede581)

### Index Manager
![toolbox-index-manager-dashboard](https://github.com/user-attachments/assets/23aac030-5379-497e-95fa-d1058ade6a98)

### Site Manager
![toolbox-site-manager-dashboard](https://github.com/user-attachments/assets/a2d41b84-068b-441e-91e0-e3640030e2d2)

### Settings
![toolbox-settings](https://github.com/user-attachments/assets/7a592a26-db82-4eb6-8cbd-8c1298d0447b)

## Installation

```bash
bench get-app https://github.com/gavindsouza/toolbox.git
bench --site your-site install-app toolbox
```

> **Note:** Toolbox currently supports **MariaDB** only.

## Quick Start

1. Navigate to **ToolBox Settings**
2. Enable **SQL Recorder** — starts capturing queries immediately
3. Enable **Index Manager** — enables automatic index optimization
4. Set processing intervals (Hourly or Daily)
5. Save — scheduled jobs are created automatically

Toolbox will begin recording queries and periodically process them to generate optimized indexes.

## CLI Reference

### SQL Recorder

```bash
# Start/stop recording
bench --site your-site sql-recorder start
bench --site your-site sql-recorder stop

# Discard recorded data
bench --site your-site sql-recorder drop
```

### SQL Manager

```bash
# Process recorded queries (run EXPLAIN, store metadata)
bench --site your-site sql-manager process

# Deduplicate stored query records
bench --site your-site sql-manager cleanup
```

### Index Manager

```bash
# Generate and apply optimized indexes (with backtesting)
bench --site your-site index-manager optimize

# Optimize a specific table
bench --site your-site index-manager optimize --table tabSales\ Invoice

# Skip backtesting (faster, but keeps all generated indexes)
bench --site your-site index-manager optimize --skip-backtest

# Filter by minimum query occurrence
bench --site your-site index-manager optimize --sql-occurrence 100

# Show all toolbox-generated indexes
bench --site your-site index-manager show-toolbox-indexes

# Remove all toolbox-generated indexes
bench --site your-site index-manager drop-toolbox-indexes
```

### DocType Flow Tracer

```bash
# Start tracing specific DocTypes
bench --site your-site doctype-manager trace on -d "Sales Invoice,Payment Entry"

# Stop tracing
bench --site your-site doctype-manager trace off -d "Sales Invoice"

# View traced flows
bench --site your-site doctype-manager trace draw

# Check tracing status
bench --site your-site doctype-manager trace status

# Purge trace data
bench --site your-site doctype-manager trace purge -d "Sales Invoice"
```

## DocTypes

| DocType | Purpose |
|---|---|
| **ToolBox Settings** | Global configuration — enable/disable features, set processing intervals |
| **MariaDB Query** | Stores captured queries with EXPLAIN results and occurrence counts |
| **MariaDB Table** | Represents database tables with query statistics and read/write categorization |
| **MariaDB Index** | Virtual DocType — live view of database indexes via `INFORMATION_SCHEMA` |
| **SQL Record Summary** | Tracks processing batches with total and unique query counts |

## Supported Versions

| Component | Version |
|---|---|
| Frappe | v15, v16 |
| Python | 3.8+ |
| Database | MariaDB |

## Planned Features

- [ ] Table health checks — ghost data, dangling columns (via `after_migrate` hooks + on-demand)
- [ ] Backup quality auditing
- [ ] Resource utilization analysis — queue utilization, CPU usage, configuration recommendations
- [ ] Security auditing — semgrep rules, whitelisted API checks, guest-accessible endpoints
- [ ] Suspicious activity tracking — permission errors, access log summaries
- [ ] Permission gap detection — data access vs. list view permission mismatches

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Write tests for new functionality
4. Submit a pull request

```bash
# Run tests locally
bench --site your-site run-tests --app toolbox
```

## License

GNU Affero General Public License v3.0 — see [LICENSE](LICENSE) for details.

Copyright &copy; 2023, [Gavin D'souza](https://github.com/gavindsouza)
