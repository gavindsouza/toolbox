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
        queries = frappe.get_all(
            "MariaDB Query",
            filters={"table": self.name},
            fields=["*", "name as query"],
            order_by="occurence desc",
            update={"doctype": "MariaDB Query Candidate"},
        )
        self.set("queries", queries)

    @property
    def num_queries(self):
        return len(self.queries)
