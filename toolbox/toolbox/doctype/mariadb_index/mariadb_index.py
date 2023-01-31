# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

from textwrap import dedent

import frappe
from frappe.model.document import Document

FIELD_ALIAS = {
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
}

INDEX_QUERY = dedent(
    """
    SELECT
        TABLE_NAME `table`,
        f.name `frappe_table_id`,
        INDEX_NAME key_name,
        COLUMN_NAME column_name,
        NON_UNIQUE non_unique,
        INDEX_TYPE index_type,
        CARDINALITY cardinality,
        COLLATION collation,
        CONCAT(TABLE_NAME, '--', INDEX_NAME) name,
        "Administrator" owner,
        "Administrator" modified_by,
        NULL creation,
        NULL modified
    FROM
        INFORMATION_SCHEMA.STATISTICS s LEFT JOIN `tabMariaDB Table` f
    ON
        s.TABLE_NAME = f._table_name"""
)


class MariaDBIndex(Document):
    _table_fieldnames = {}

    def db_insert(self, *args, **kwargs):
        raise NotImplementedError

    def db_update(self):
        raise NotImplementedError

    def delete(self):
        ...

    @staticmethod
    def get_stats(args):
        ...

    def load_from_db(self):
        table, index = self.name.split("--")
        document_data = frappe.db.sql(
            f"{INDEX_QUERY} WHERE TABLE_NAME = %s AND INDEX_NAME = %s",
            (table, index),
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
        start, page_length = args["start"], args["page_length"]
        fields = get_accessible_fields(args["fields"])
        select_query = get_index_query(fields, args["filters"])

        data = frappe.db.sql(
            f"{select_query} ORDER BY {order_by} limit {page_length} offset {start}",
            as_dict=True,
        )

        if pluck := args.get("pluck"):
            return [d[pluck] for d in data]
        return data

    @staticmethod
    def get_count(args=None, **kwargs):
        args = get_args(args, **kwargs)
        query = get_index_query(["count(*)"], args["filters"])
        return frappe.db.sql(query)[0][0]


def wrap_query_constant(value: str) -> str:
    if isinstance(value, str):
        if value.isnumeric():
            return value
        return f"'{value}'"
    return value


def wrap_query_field(value: str) -> str:
    if "`" != value[0] and "`" not in value:
        return f"`{value}`"
    return value


def get_filter_clause(filters: list[list]) -> str:
    if not filters:
        return ""

    return f"""WHERE {' AND '.join(f"{get_column_name(f[1])} {f[2]} {wrap_query_constant(f[3])}" for f in filters)}"""


def get_accessible_fields(fields: list[str]) -> list[str]:
    if fields == ["*"] or fields == ["count(*)"]:
        return fields

    allowed_fields = []

    for field in fields:
        if _x := field.split(".", 1)[-1]:
            if _x.replace("`", "") in FIELD_ALIAS:
                allowed_fields.append(_x)

    return allowed_fields


def get_mapped_field(field: str) -> str | None:
    eq_field = field.split(".", 1)[-1].replace("`", "")

    _first_field = eq_field.split(" ")
    first_field = _first_field[0]
    order = "asc"

    if len(_first_field) > 1:
        order = _first_field[1]

    if first_field in FIELD_ALIAS:
        return f"{first_field} {order}"

    return None


def get_index_query(fields: list[str], filters: list[list]) -> str:
    filter_clause = get_filter_clause(filters)
    qry = f"{INDEX_QUERY} {filter_clause}"

    if fields:
        return f"SELECT {', '.join(fields)} FROM ({qry}) as t"

    return qry


def get_column_name(fieldname: str) -> str:
    return wrap_query_field(FIELD_ALIAS.get(fieldname, fieldname))


def get_args(args=None, kwargs=None):
    _args = {"filters": [], "fields": [], "start": 0, "page_length": 20, "order_by": ""}
    _args.update(args or {})
    _args.update(kwargs or {})

    for limit_char in ["limit_page_length", "limit"]:
        if limit_char in _args:
            _args["page_length"] = _args[limit_char]
            del _args[limit_char]

    return _args
