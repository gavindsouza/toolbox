# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

import re
from itertools import groupby

import frappe
from frappe.model.document import Document

from toolbox.db_adapter import (
    get_create_index_ddl,
    get_drop_index_ddl,
    get_drop_index_if_exists_ddl,
    get_index_query as _get_db_index_query,
    is_postgres,
)
from toolbox.utils import IndexCandidate

VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_ ]*$")

TOOLBOX_INDEX_PREFIX = "toolbox_index_"

FIELD_ALIAS_MARIADB = {
    "name": "name",
    "owner": "owner",
    "modified_by": "modified_by",
    "creation": "creation",
    "modified": "modified",
    "table": "TABLE_NAME",
    "key_name": "INDEX_NAME",
    "column_name": "COLUMN_NAME",
    "non_unique": "NON_UNIQUE",
    "index_type": "INDEX_TYPE",
    "cardinality": "CARDINALITY",
    "collation": "COLLATION",
    "frappe_table_id": "f.`name`",
    "seq_id": "SEQ_IN_INDEX",
}

FIELD_ALIAS_POSTGRES = {
    "name": "name",
    "owner": "owner",
    "modified_by": "modified_by",
    "creation": "creation",
    "modified": "modified",
    "table": "i.tablename",
    "key_name": "ix.indexname",
    "column_name": "a.attname",
    "non_unique": "non_unique",
    "index_type": "index_type",
    "cardinality": "cardinality",
    "collation": "collation",
    "frappe_table_id": "f.name",
    "seq_id": "a.attnum",
}


def _get_field_alias():
    if is_postgres():
        return FIELD_ALIAS_POSTGRES
    return FIELD_ALIAS_MARIADB


# Keep FIELD_ALIAS as the default for backward compatibility
FIELD_ALIAS = FIELD_ALIAS_MARIADB


def _get_index_query():
    return _get_db_index_query()


def get_index_name(ic: IndexCandidate) -> str:
    return f"{TOOLBOX_INDEX_PREFIX}{'_'.join(ic)}"


def _validate_identifier(value: str, label: str = "identifier"):
    if not value or not VALID_IDENTIFIER.match(value):
        frappe.throw(f"Invalid {label}: {value}")


class MariaDBIndexDocument(Document):
    _table_fieldnames = {}

    def db_insert(self, *args, **kwargs):
        raise NotImplementedError

    def db_update(self):
        raise NotImplementedError

    def delete(self): ...

    @staticmethod
    def get_stats(args): ...

    def load_from_db(self):
        index, column_name, table = self.name.split("--")
        idx_query = _get_index_query()
        if is_postgres():
            document_data = frappe.db.sql(
                f"{idx_query} AND i.tablename = %s AND ix.indexname = %s AND a.attname = %s",
                (table, index, column_name),
                as_dict=True,
            )[0]
        else:
            document_data = frappe.db.sql(
                f"{idx_query} WHERE TABLE_NAME = %s AND INDEX_NAME = %s and COLUMN_NAME = %s",
                (table, index, column_name),
                as_dict=True,
            )[0]
        self.update(document_data)

    @staticmethod
    def get_last_doc():
        name = MariaDBIndex.get_list(limit=1, order_by="modified desc", pluck="name")
        return MariaDBIndex("MariaDB Index", name[0]) if name else None

    @staticmethod
    def get_list(args=None, **kwargs):
        args = get_args(args, kwargs)
        order_by = get_mapped_field(args["order_by"]) or "cardinality desc, name"
        fields = get_accessible_fields(args["fields"])
        select_query, params = get_index_query(fields, args["filters"])

        query = f"{select_query} ORDER BY {order_by}"

        if args.get("page_length"):
            query += f" LIMIT {int(args['page_length'])}"

        if args.get("limit_start"):
            query += f" OFFSET {int(args['limit_start'])}"

        data = frappe.db.sql(
            query,
            params,
            as_dict=True,
        )

        if pluck := args.get("pluck"):
            return [d[pluck] for d in data]
        return data

    @staticmethod
    def get_count(args=None, **kwargs):
        args = get_args(args, kwargs)
        query, params = get_index_query(["count(distinct name)"], args["filters"])
        return frappe.db.sql(query, params)[0][0]


class MariaDBIndex(MariaDBIndexDocument):
    @staticmethod
    def get_indexes(table=None, *, reduce=False, toolbox_only=False):
        filters = []

        if toolbox_only:
            filters.append(["key_name", "like", f"{TOOLBOX_INDEX_PREFIX}%"])

        if table:
            filters.append(["table", "=", table])

        table_indexes = MariaDBIndex.get_list(filters=filters)

        if reduce:
            if not table:
                raise ValueError("Table name is required to reduce indexes")

            return [
                [x["column_name"] for x in index]
                for index in (
                    sorted(y, key=lambda x: x["seq_id"])
                    for _, y in groupby(
                        sorted(table_indexes, key=lambda x: x["key_name"]), lambda x: x["key_name"]
                    )
                )
            ]

        return table_indexes

    @staticmethod
    def create(
        table, index_candidates: list[IndexCandidate], verbose=False
    ) -> list[IndexCandidate]:
        _validate_identifier(table, "table name")
        failures = []
        for ic in index_candidates:
            index_name = get_index_name(ic)
            _validate_identifier(index_name, "index name")
            for col in ic:
                _validate_identifier(col, "column name")
            try:
                frappe.db.sql_ddl(
                    get_create_index_ddl(table, index_name, list(ic)),
                    debug=verbose,
                )
            except Exception:
                failures.append(ic)
        return failures

    @staticmethod
    def drop(table, index_candidates: list[IndexCandidate], verbose=False):
        _validate_identifier(table, "table name")
        for ic in index_candidates:
            index_name = get_index_name(ic)
            _validate_identifier(index_name, "index name")
            frappe.db.sql_ddl(
                get_drop_index_ddl(table, index_name),
                debug=verbose,
            )

    @staticmethod
    def drop_toolbox_indexes(table, verbose=False):
        _validate_identifier(table, "table name")
        dropped_indexes = set()
        for index in MariaDBIndex.get_indexes(table):
            index_name = index["key_name"]
            if index_name.startswith(TOOLBOX_INDEX_PREFIX) and index_name not in dropped_indexes:
                _validate_identifier(index_name, "index name")
                frappe.db.sql_ddl(
                    get_drop_index_if_exists_ddl(table, index_name),
                    debug=verbose,
                )
                dropped_indexes.add(index_name)


ALLOWED_OPERATORS = {"=", "!=", "<", ">", "<=", ">=", "like", "not like", "in", "not in"}


def wrap_query_field(value: str) -> str:
    from toolbox.db_adapter import quote_identifier

    # Already quoted — return as-is
    if value.startswith("`") or value.startswith('"'):
        return value

    # Dotted references (e.g. "f.name", "i.tablename") — don't quote the whole thing
    if "." in value:
        return value

    return quote_identifier(value)


def get_filter_clause(filters: list[list]) -> tuple[str, tuple]:
    if not filters:
        return "", ()

    where_clause = []
    params = []

    for f in filters:
        f = f[:4]
        i = len(f) - 3
        fieldname = f[i]
        operator = f[i + 1].strip().lower()
        value = f[i + 2]

        if operator not in ALLOWED_OPERATORS:
            frappe.throw(f"Invalid filter operator: {f[i + 1]}")

        column = get_column_name(fieldname)

        if operator in ("in", "not in"):
            if isinstance(value, (list, tuple)):
                placeholders = ", ".join(["%s"] * len(value))
                where_clause.append(f"{column} {operator} ({placeholders})")
                params.extend(value)
            else:
                where_clause.append(f"{column} {operator} (%s)")
                params.append(value)
        else:
            where_clause.append(f"{column} {operator} %s")
            params.append(value)

    return f"WHERE {' AND '.join(where_clause)}", tuple(params)


def get_accessible_fields(fields: list[str]) -> list[str]:
    if fields == ["*"] or fields == ["count(*)"] or fields == ["count(*) as result"]:
        return fields

    alias = _get_field_alias()
    allowed_fields = []

    for field in fields:
        if _x := field.split(".", 1)[-1]:
            if _x.replace("`", "").replace('"', "") in alias:
                allowed_fields.append(_x)

    return allowed_fields


def get_mapped_field(field: str) -> str | None:
    alias = _get_field_alias()
    eq_field = field.split(".", 1)[-1].replace("`", "").replace('"', "")

    _first_field = eq_field.split(" ")
    first_field = _first_field[0]
    order = "asc"

    if len(_first_field) > 1:
        order = _first_field[1].strip(",").lower()

    if order not in ("asc", "desc"):
        order = "asc"

    if first_field in alias:
        return f"{first_field} {order}"

    return None


def get_index_query(fields: list[str], filters: list[list]) -> tuple[str, tuple]:
    idx_query = _get_index_query()
    filter_clause, params = get_filter_clause(filters)

    # Postgres index query already has WHERE clause, so use AND instead of WHERE
    if filter_clause and is_postgres():
        filter_clause = filter_clause.replace("WHERE ", "AND ", 1)

    qry = f"{idx_query} {filter_clause}"

    if fields:
        return f"SELECT {', '.join(fields)} FROM ({qry}) as t", params

    return qry, params


def get_column_name(fieldname: str) -> str:
    alias = _get_field_alias()
    return wrap_query_field(alias.get(fieldname, fieldname))


def get_args(args=None, kwargs=None):
    _args = {"filters": [], "fields": [], "order_by": ""}
    _args.update(args or {})
    _args.update(kwargs or {})

    for limit_char in ["limit_page_length", "limit"]:
        if limit_char in _args:
            _args["page_length"] = _args[limit_char]
            del _args[limit_char]

    if isinstance(_args["filters"], dict):
        _args["filters"] = [[k, *v] for k, v in _args["filters"].items()]

    if isinstance(_args["filters"], list):
        offset = -1 if _args["filters"] and len(_args["filters"][0]) == 3 else 0

        for f in _args["filters"]:
            if f[2 + offset] == "is":
                if f[3 + offset] == "set":
                    f[2 + offset] = "!="
                elif f[3 + offset] == "not set":
                    f[2 + offset] = "="
                f[3 + offset] = ""

    return _args


# --- Duplicate & Redundant Index Detection (Feature 2) ---


def reduce_indexes_to_column_lists(raw_indexes: list[dict]) -> list[dict]:
    """Convert raw INFORMATION_SCHEMA rows into per-index column lists.

    Input: [{"key_name": "idx", "column_name": "col", "seq_id": 1}, ...]
    Output: [{"key_name": "idx", "columns": ["col1", "col2"]}, ...]
    """
    from itertools import groupby

    sorted_rows = sorted(raw_indexes, key=lambda x: (x["key_name"], x["seq_id"]))
    result = []
    for key_name, group in groupby(sorted_rows, key=lambda x: x["key_name"]):
        columns = [row["column_name"] for row in sorted(group, key=lambda x: x["seq_id"])]
        result.append({"key_name": key_name, "columns": columns})
    return result


def find_duplicate_indexes(indexes: list[dict]) -> list[dict]:
    """Find exact duplicate indexes (same columns in same order).

    Returns list of {"redundant": name, "superseded_by": name, "columns": [...]}.
    PRIMARY KEY is never recommended for dropping.
    """
    duplicates = []
    seen = {}  # column_tuple -> first index name

    for idx in indexes:
        col_key = tuple(idx["columns"])
        if col_key in seen:
            # The later index is redundant; never mark PRIMARY as redundant
            if idx["key_name"] == "PRIMARY":
                continue
            duplicates.append({
                "redundant": idx["key_name"],
                "superseded_by": seen[col_key],
                "columns": idx["columns"],
            })
        else:
            seen[col_key] = idx["key_name"]

    return duplicates


def find_redundant_indexes(indexes: list[dict]) -> list[dict]:
    """Find left-prefix redundant indexes.

    Index (A) is redundant if index (A, B, C) exists, because MySQL uses leftmost prefix.
    PRIMARY KEY is never marked as redundant.

    Returns list of {"redundant": name, "superseded_by": name, "columns": [...], "superseding_columns": [...]}.
    """
    redundant = []
    # Sort by column count descending so longer indexes are checked first
    sorted_indexes = sorted(indexes, key=lambda x: len(x["columns"]), reverse=True)

    for i, smaller in enumerate(sorted_indexes):
        if smaller["key_name"] == "PRIMARY":
            continue

        for larger in sorted_indexes:
            if larger["key_name"] == smaller["key_name"]:
                continue
            if len(larger["columns"]) <= len(smaller["columns"]):
                continue

            # Check if smaller is a left-prefix of larger
            if larger["columns"][:len(smaller["columns"])] == smaller["columns"]:
                redundant.append({
                    "redundant": smaller["key_name"],
                    "superseded_by": larger["key_name"],
                    "columns": smaller["columns"],
                    "superseding_columns": larger["columns"],
                })
                break  # Only report once per redundant index

    return redundant


def analyze_table_indexes(indexes: list[dict]) -> dict:
    """Analyze a table's indexes for duplicates and left-prefix redundancy.

    Args:
        indexes: List of {"key_name": str, "columns": list[str]}

    Returns:
        {"duplicates": [...], "redundant": [...]}
    """
    return {
        "duplicates": find_duplicate_indexes(indexes),
        "redundant": find_redundant_indexes(indexes),
    }
