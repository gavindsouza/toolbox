# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.utils import record_query


class TestMariaDBTable(FrappeTestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        frappe.db.rollback()
        return super().tearDown()

    def test_load_queries(self):
        query = "SELECT * FROM `tabMariaDB Table`"
        query_record = record_query(query)
        for explain in frappe.db.sql(f"EXPLAIN EXTENDED {query}", as_dict=True):
            query_record.apply_explain(explain)
        query_record.save()

        # check if recorded query is loaded
        mariadb_table = frappe.get_doc("MariaDB Table", {"_table_name": "tabMariaDB Table"})
        query_id = mariadb_table.get("queries", {"name": query_record.name})[0].name
        self.assertEqual(query_id, query_record.name)

        # in context of a request, num_queries is computed
        frappe.request = frappe._dict(
            method="GET", path="/api/resource/MariaDB Table/fake-request"
        )
        mariadb_table = frappe.get_doc("MariaDB Table", {"_table_name": "tabMariaDB Table"})
        self.assertTrue(getattr(mariadb_table, "_num_queries", None))
