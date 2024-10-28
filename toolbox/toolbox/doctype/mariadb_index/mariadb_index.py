# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

from itertools import groupby
from textwrap import dedent

import frappe
from frappe.model.document import Document

from toolbox.utils import IndexCandidate

TOOLBOX_INDEX_PREFIX = "toolbox_index_"

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
    "seq_id": "SEQ_IN_INDEX",
}

INDEX_QUERY = dedent(
    """
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


def get_index_name(ic: IndexCandidate) -> str:
    return f"{TOOLBOX_INDEX_PREFIX}{'_'.join(ic)}"


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
        document_data = frappe.db.sql(
            f"{INDEX_QUERY} WHERE TABLE_NAME = %s AND INDEX_NAME = %s and COLUMN_NAME = %s",
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
        select_query = get_index_query(fields, args["filters"])

        query = f"{select_query} ORDER BY {order_by}"

        if args.get("page_length"):
            query += f" LIMIT {args['page_length']}"

        if args.get("limit_start"):
            query += f" OFFSET {args['limit_start']}"

        data = frappe.db.sql(
            query,
            as_dict=True,
        )

        if pluck := args.get("pluck"):
            return [d[pluck] for d in data]
        return data

    @staticmethod
    def get_count(args=None, **kwargs):
        args = get_args(args, kwargs)
        query = get_index_query(["count(distinct name)"], args["filters"])
        return frappe.db.sql(query)[0][0]


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
        failures = []
        for ic in index_candidates:
            try:
                frappe.db.sql_ddl(
                    f"CREATE INDEX `{get_index_name(ic)}` ON `{table}` ({', '.join(f'`{i}`' for i in ic)})",
                    debug=verbose,
                )
            except Exception:
                failures.append(ic)
        return failures

    @staticmethod
    def drop(table, index_candidates: list[IndexCandidate], verbose=False):
        for ic in index_candidates:
            frappe.db.sql_ddl(
                f"DROP INDEX `{get_index_name(ic)}` ON `{table}`",
                debug=verbose,
            )

    @staticmethod
    def drop_toolbox_indexes(table, verbose=False):
        dropped_indexes = set()
        for index in MariaDBIndex.get_indexes(table):
            index_name = index["key_name"]
            if index_name.startswith(TOOLBOX_INDEX_PREFIX) and index_name not in dropped_indexes:
                frappe.db.sql_ddl(
                    f"DROP INDEX IF EXISTS `{index_name}` ON `{table}`",
                    debug=verbose,
                )
                dropped_indexes.add(index_name)


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

    where_clause = []

    for f in filters:
        f = f[:4]
        i = len(f) - 3
        where_clause.append(f"{get_column_name(f[i])} {f[i+1]} {wrap_query_constant(f[i+2])}")

    return f"WHERE {' AND '.join(where_clause)}"


def get_accessible_fields(fields: list[str]) -> list[str]:
    if fields == ["*"] or fields == ["count(*)"] or fields == ["count(*) as result"]:
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
        order = _first_field[1].strip(",")

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
