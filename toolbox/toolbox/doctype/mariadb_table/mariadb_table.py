# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class MariaDBTable(Document):
    ...
    # TODO:
    # analyze table
    # 1. Show table size (MiB)
    # 2. Show table indexes / index size (MiB)
    # 3. Show table health status / fragmentation
    # 5. Show table row count / require partitioning?

    def load_from_db(self):
        super().load_from_db()
        self.load_queries()

    def load_queries(self):
        _queries = frappe.get_all(
            "MariaDB Query",
            filters={"table": self.name},
            fields=["*", "name as query"],
            order_by="occurence desc",
            update={"doctype": "MariaDB Query Candidate"},
        )
        if frappe.request:
            self.set("queries", _queries[:100])
            self.num_queries = len(_queries)
        else:
            self.set("queries", _queries)

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
