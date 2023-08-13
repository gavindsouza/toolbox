# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class MariaDBTable(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        from toolbox.toolbox.doctype.mariadb_query_candidate.mariadb_query_candidate import (
            MariaDBQueryCandidate,
        )

        _table_exists: DF.Check
        _table_name: DF.Data | None
        num_queries: DF.Int
        queries: DF.Table[MariaDBQueryCandidate]
        table_category: DF.Literal["Read", "Write"]
        table_category_meta: DF.JSON | None
    # end: auto-generated types

    # TODO:
    # analyze table
    # 1. Show table size (MiB)
    # 2. Show table indexes / index size (MiB)
    # 3. Show table health status / fragmentation
    # 5. Show table row count / require partitioning?
    def __init__(self, *args, **kwargs):
        self._all_queries = []
        super().__init__(*args, **kwargs)

    def load_from_db(self):
        super().load_from_db()
        self.load_queries()

    def load_queries(self):
        self._all_queries = frappe.get_all(
            "MariaDB Query",
            filters={"table": self.name},
            fields=["*", "name as query"],
            order_by="occurence desc",
            update={"doctype": "MariaDB Query Candidate"},
        )
        if frappe.request:
            self.set("queries", self._all_queries[:100])
            self.num_queries = len(self._all_queries)
        else:
            self.set("queries", self._all_queries)

    def validate(self):
        self.set_exists_check()
        self.set_table_category()

    def set_table_category(self):
        all_queries = len(self._all_queries)
        write_queries = len(
            [
                x
                for x in self._all_queries
                if x.parameterized_query[:6].lower() in ("update", "insert", "delete")
            ]
        )

        if not all_queries or (write_queries / all_queries < 0.5):
            self.table_category = "Read"

        else:
            self.table_category = "Write"
        self.table_category_meta = frappe.as_json(
            {"total_queries": all_queries, "write_queries": write_queries}
        )

    def set_exists_check(self):
        if frappe.db.sql("SHOW TABLES LIKE %s", self._table_name):
            self._table_exists = True

    @property
    def num_queries(self):
        if (_computed := getattr(self, "_num_queries", None)) is None:
            return len(self.queries)
        return _computed

    @num_queries.setter
    def num_queries(self, value):
        self._num_queries = value

    @frappe.whitelist()
    def analyze(self):
        return frappe.db.sql(f"ANALYZE TABLE `{self._table_name}`")

    @frappe.whitelist()
    def optimize(self):
        return frappe.db.sql(f"OPTIMIZE TABLE `{self._table_name}`")
