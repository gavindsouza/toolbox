# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt
#
# Primary Key Exhaustion Monitoring
# Detects tables approaching their auto-increment integer limits.

import re

import frappe

# Max values for signed/unsigned integer types
_MAX_VALUES = {
    "tinyint": 127,
    "tinyint unsigned": 255,
    "smallint": 32_767,
    "smallint unsigned": 65_535,
    "mediumint": 8_388_607,
    "mediumint unsigned": 16_777_215,
    "int": 2_147_483_647,
    "int unsigned": 4_294_967_295,
    "bigint": 9_223_372_036_854_775_807,
    "bigint unsigned": 18_446_744_073_709_551_615,
}

_DISPLAY_WIDTH_RE = re.compile(r"\(\d+\)")


def parse_column_type(column_type: str) -> str:
    """Normalize COLUMN_TYPE from INFORMATION_SCHEMA (e.g. 'int(11) unsigned' -> 'int unsigned')."""
    return _DISPLAY_WIDTH_RE.sub("", column_type).strip()


def get_max_value_for_type(column_type: str) -> int | None:
    """Return the maximum integer value for a given column type, or None if not an integer type."""
    normalized = parse_column_type(column_type.lower())
    return _MAX_VALUES.get(normalized)


def calculate_pk_usage(auto_increment: int | None, max_value: int) -> float | None:
    """Calculate PK usage as a percentage. Returns None if auto_increment is None."""
    if auto_increment is None:
        return None
    return (auto_increment / max_value) * 100


def classify_pk_severity(usage_percent: float | None) -> str | None:
    """Classify PK exhaustion severity: green (<50%), yellow (50-80%), red (>=80%)."""
    if usage_percent is None:
        return None
    if usage_percent >= 80.0:
        return "red"
    if usage_percent >= 50.0:
        return "yellow"
    return "green"


def get_pk_exhaustion_report(min_usage_percent: float = 0.0) -> list[dict]:
    """Generate a report of all tables with auto-increment PKs and their usage levels.

    Args:
        min_usage_percent: Only include tables with usage >= this percentage.

    Returns:
        List of dicts sorted by usage_percent descending.
    """
    rows = frappe.db.sql(
        """
        SELECT
            t.TABLE_NAME,
            t.AUTO_INCREMENT,
            c.COLUMN_TYPE
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c
            ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND t.TABLE_NAME = c.TABLE_NAME
            AND c.COLUMN_KEY = 'PRI'
            AND c.EXTRA LIKE '%%auto_increment%%'
        WHERE t.TABLE_SCHEMA = DATABASE()
            AND t.AUTO_INCREMENT IS NOT NULL
        """,
        as_dict=True,
    )

    report = []
    for row in rows:
        max_value = get_max_value_for_type(row["COLUMN_TYPE"])
        if max_value is None:
            continue

        usage = calculate_pk_usage(row["AUTO_INCREMENT"], max_value)
        severity = classify_pk_severity(usage)

        if usage is not None and usage >= min_usage_percent:
            report.append({
                "table_name": row["TABLE_NAME"],
                "auto_increment": row["AUTO_INCREMENT"],
                "max_value": max_value,
                "usage_percent": round(usage, 3),
                "severity": severity,
            })

    report.sort(key=lambda x: x["usage_percent"], reverse=True)
    return report
