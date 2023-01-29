# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class MariaDBTable(Document):
    ...
    # TODO:
    # 1. Show table size (MiB)
    # 2. Show table indexes / index size (MiB)
    # 3. Show table health status / fragmentation
    # 5. Show table row count / require partitioning?

    def load_from_db(self):
        super().load_from_db()
        self.load_queries()

    def load_queries(self):
        queries = frappe.get_all(
            "MariaDB Query",
            filters={"table": self.name},
            fields=["*"],
            order_by="occurence desc",
        )
        self.set("queries", queries)
        for qry in self.queries:
            qry.doctype = "MariaDB Query"

    @property
    def num_queries(self):
        return len(self.queries)
