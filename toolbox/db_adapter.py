# Copyright (c) 2025, Gavin D'souza and contributors
# For license information, please see license.txt
#
# Database abstraction layer for MariaDB and PostgreSQL support.
# All functions dispatch based on frappe.conf.db_type.

from textwrap import dedent

import frappe


def get_db_type() -> str:
    try:
        return getattr(frappe.conf, "db_type", "mariadb")
    except RuntimeError:
        # frappe.conf not bound (e.g. unit tests without site context)
        return "mariadb"


def is_mariadb() -> bool:
    return get_db_type() == "mariadb"


def is_postgres() -> bool:
    return get_db_type() == "postgres"


def quote_identifier(name: str) -> str:
    if is_postgres():
        return f'"{name}"'
    return f"`{name}`"


def table_exists(name: str) -> bool:
    if is_postgres():
        return bool(
            frappe.db.sql(
                "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s",
                (name,),
            )
        )
    return bool(frappe.db.sql("SHOW TABLES LIKE %s", (name,)))


def get_explain_sql(query: str) -> str:
    if is_postgres():
        return f"EXPLAIN {query}"
    return f"EXPLAIN EXTENDED {query}"


def get_analyze_sql(query: str) -> str:
    if is_postgres():
        return f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
    return f"ANALYZE {query}"


def get_optimize_sql(table: str) -> str:
    if is_postgres():
        return f"VACUUM ANALYZE {quote_identifier(table)}"
    return f"OPTIMIZE TABLE {quote_identifier(table)}"


def get_analyze_table_sql(table: str) -> str:
    if is_postgres():
        return f"ANALYZE {quote_identifier(table)}"
    return f"ANALYZE TABLE {quote_identifier(table)}"


def get_create_index_ddl(table: str, name: str, columns: list[str]) -> str:
    q = quote_identifier
    cols = ", ".join(q(col) for col in columns)
    return f"CREATE INDEX {q(name)} ON {q(table)} ({cols})"


def get_drop_index_ddl(table: str, name: str) -> str:
    q = quote_identifier
    if is_postgres():
        return f"DROP INDEX {q(name)}"
    return f"DROP INDEX {q(name)} ON {q(table)}"


def get_drop_index_if_exists_ddl(table: str, name: str) -> str:
    q = quote_identifier
    if is_postgres():
        return f"DROP INDEX IF EXISTS {q(name)}"
    return f"DROP INDEX IF EXISTS {q(name)} ON {q(table)}"


def get_active_connections() -> list[dict]:
    if is_postgres():
        return frappe.db.sql(
            "SELECT count(*) AS \"Value\", 'Connections_active' AS \"Variable_name\""
            " FROM pg_stat_activity WHERE state != 'idle'",
            as_dict=True,
        )
    return frappe.db.sql(
        "SHOW STATUS WHERE `variable_name` = 'Threads_connected'", as_dict=True
    )


def get_index_query() -> str:
    if is_postgres():
        return dedent(
            """\
            SELECT
                i.tablename AS "table",
                f.name AS frappe_table_id,
                i.indexname AS key_name,
                a.attnum AS seq_id,
                a.attname AS column_name,
                NOT idx.indisunique AS non_unique,
                am.amname AS index_type,
                NULL AS cardinality,
                NULL AS collation,
                i.indexname || '--' || a.attname || '--' || i.tablename AS name,
                'Administrator' AS owner,
                'Administrator' AS modified_by,
                NULL AS creation,
                NULL AS modified
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.indexname
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
            JOIN pg_index idx ON idx.indexrelid = c.oid
            JOIN pg_attribute a ON a.attrelid = idx.indrelid
                AND a.attnum = ANY(idx.indkey)
            JOIN pg_am am ON am.oid = c.relam
            LEFT JOIN "tabMariaDB Table" f ON i.tablename = f._table_name
            WHERE i.schemaname = 'public'"""
        )
    return dedent(
        """\
        SELECT
            TABLE_NAME `table`,
            f.name `frappe_table_id`,
            INDEX_NAME key_name,
            SEQ_IN_INDEX seq_id,
            COLUMN_NAME column_name,
            NON_UNIQUE non_unique,
            INDEX_TYPE index_type,
            CARDINALITY cardinality,
            COLLATION collation,
            CONCAT(INDEX_NAME, '--', COLUMN_NAME, '--', TABLE_NAME) name,
            "Administrator" owner,
            "Administrator" modified_by,
            NULL creation,
            NULL modified
        FROM
            INFORMATION_SCHEMA.STATISTICS s LEFT JOIN `tabMariaDB Table` f
        ON
            s.TABLE_NAME = f._table_name"""
    )


# Postgres data type -> max value for PK exhaustion detection
_PG_MAX_VALUES = {
    "smallint": 2**15 - 1,
    "integer": 2**31 - 1,
    "bigint": 2**63 - 1,
}


def get_pk_exhaustion_data() -> list[dict]:
    """Return PK exhaustion data normalized across both DB backends.

    Returns list of dicts with keys: table_name, auto_increment, column_type.
    """
    if is_postgres():
        rows = frappe.db.sql(
            """\
            SELECT
                s.sequencename,
                s.data_type,
                s.last_value,
                s.max_value
            FROM pg_sequences s
            WHERE s.schemaname = 'public'
                AND s.last_value IS NOT NULL""",
            as_dict=True,
        )
        results = []
        for row in rows:
            # pg_sequences.sequencename is typically "<table>_<col>_seq"
            # Strip the "_<col>_seq" suffix to get the table name
            seq_name = row["sequencename"]
            # Try to derive table name: common pattern is <table>_id_seq
            table_name = seq_name
            for suffix in ("_id_seq", "_seq"):
                if seq_name.endswith(suffix):
                    table_name = seq_name[: -len(suffix)]
                    break

            results.append({
                "table_name": table_name,
                "auto_increment": row["last_value"],
                "column_type": row["data_type"],
            })
        return results

    # MariaDB path
    return frappe.db.sql(
        """\
        SELECT
            t.TABLE_NAME AS table_name,
            t.AUTO_INCREMENT AS auto_increment,
            c.COLUMN_TYPE AS column_type
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c
            ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND t.TABLE_NAME = c.TABLE_NAME
            AND c.COLUMN_KEY = 'PRI'
            AND c.EXTRA LIKE '%%auto_increment%%'
        WHERE t.TABLE_SCHEMA = DATABASE()
            AND t.AUTO_INCREMENT IS NOT NULL""",
        as_dict=True,
    )


def get_max_value_for_type_pg(data_type: str) -> int | None:
    return _PG_MAX_VALUES.get(data_type.lower())


def get_rename_column_sql(table: str, old_col: str, new_col: str, col_type: str) -> str:
    q = quote_identifier
    if is_postgres():
        return f"ALTER TABLE {q(table)} RENAME COLUMN {q(old_col)} TO {q(new_col)}"
    return f"ALTER TABLE {q(table)} CHANGE {q(old_col)} {q(new_col)} {col_type}"


def get_unused_indexes_data() -> list[dict]:
    """Return unused indexes data for reporting, normalized across both backends."""
    if is_postgres():
        return frappe.db.sql(
            """\
            SELECT
                s.relname AS "TABLE_NAME",
                mtbl.name AS "MariaDB Table",
                s.indexrelname AS "INDEX_NAME",
                s.indexrelname || '--' || a.attname || '--' || s.relname AS "MariaDB Index"
            FROM pg_stat_user_indexes s
            JOIN pg_index idx ON idx.indexrelid = s.indexrelid
            JOIN pg_attribute a ON a.attrelid = idx.indrelid
                AND a.attnum = ANY(idx.indkey)
            LEFT JOIN "tabMariaDB Table" mtbl ON s.relname = mtbl._table_name
            WHERE s.idx_scan = 0
                AND NOT idx.indisunique
            ORDER BY s.relname, s.indexrelname""",
            as_dict=True,
        )
    return frappe.db.sql(
        """\
        SELECT
            st.TABLE_NAME 'TABLE_NAME',
            mtbl.name 'MariaDB Table',
            st.INDEX_NAME 'INDEX_NAME',
            CONCAT(st.INDEX_NAME, '--', st.COLUMN_NAME, '--', st.TABLE_NAME) 'MariaDB Index'
        FROM
            information_schema.STATISTICS st
            LEFT JOIN information_schema.INDEX_STATISTICS idx
            ON
                idx.INDEX_NAME    = st.INDEX_NAME
                AND idx.TABLE_NAME    = st.TABLE_NAME
                AND idx.TABLE_SCHEMA  = st.TABLE_SCHEMA
            LEFT JOIN `tabMariaDB Table` mtbl
            ON
                st.TABLE_NAME = mtbl._table_name
        WHERE
            (idx.INDEX_NAME IS NULL OR idx.ROWS_READ = 0)
            AND st.NON_UNIQUE = 1
        ORDER BY
            1, 2, 3""",
        as_dict=True,
    )


def parse_pg_explain_analyze(result: list[dict]) -> list[dict]:
    """Parse Postgres EXPLAIN (ANALYZE, FORMAT JSON) output into normalized format.

    Normalizes to match MariaDB ANALYZE output with r_rows and r_filtered keys.
    """
    import json

    if not result:
        return [{"r_filtered": -1, "r_rows": "0.00", "Extra": ""}]

    # Postgres EXPLAIN (ANALYZE, FORMAT JSON) returns a single row with a JSON column
    raw = result[0]
    if isinstance(raw, dict):
        # frappe.db.sql as_dict=True returns dict
        json_str = list(raw.values())[0]
    else:
        json_str = raw[0]

    try:
        plan_data = json.loads(json_str) if isinstance(json_str, str) else json_str
    except (json.JSONDecodeError, TypeError):
        return [{"r_filtered": -1, "r_rows": "0.00", "Extra": ""}]

    if isinstance(plan_data, list) and plan_data:
        plan_data = plan_data[0]

    plan = plan_data.get("Plan", {})
    rows = []
    _extract_plan_rows(plan, rows)
    return rows or [{"r_filtered": -1, "r_rows": "0.00", "Extra": ""}]


def _extract_plan_rows(plan: dict, rows: list[dict]):
    """Recursively extract row estimates from a Postgres EXPLAIN plan node."""
    actual_rows = plan.get("Actual Rows", 0)
    plan_rows = plan.get("Plan Rows", 1)

    # r_filtered: percentage of rows remaining after filtering (higher = better)
    # In MariaDB, r_filtered = 100 means all rows matched
    if plan_rows > 0 and actual_rows > 0:
        r_filtered = min(100.0, (actual_rows / plan_rows) * 100)
    elif actual_rows == 0:
        r_filtered = 0.0
    else:
        r_filtered = 100.0

    extra_parts = []
    node_type = plan.get("Node Type", "")
    if "Filter" in plan:
        extra_parts.append("Using where")
    if "Index Name" in plan:
        extra_parts.append(f"Using index ({plan['Index Name']})")
    if node_type in ("Sort", "Incremental Sort"):
        extra_parts.append("Using filesort")

    rows.append({
        "r_rows": f"{actual_rows:.2f}",
        "r_filtered": round(r_filtered, 2),
        "Extra": "; ".join(extra_parts) if extra_parts else "",
    })

    for child in plan.get("Plans", []):
        _extract_plan_rows(child, rows)
