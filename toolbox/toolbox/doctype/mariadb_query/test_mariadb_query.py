# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.utils import record_query


class TestMariaDBQuery(FrappeTestCase):
    def tearDown(self) -> None:
        frappe.db.rollback()
        return super().tearDown()

    def test_apply_explain(self):
        query = (
            "SELECT * FROM `tabNote`, `tabDocType` WHERE `tabNote`.`owner` = `tabDocType`.`owner`"
        )
        explain_data = frappe.db.sql(f"EXPLAIN EXTENDED {query}", as_dict=True)

        qry = record_query(query)
        self.assertEqual(qry.query_explain, [])

        # check if data has been applied
        for explain in explain_data:
            qry.apply_explain(explain)
        self.assertEqual(len(explain_data), len(qry.query_explain))

        # values are being applied correctly
        found_table = explain_data[0]["table"]
        recorded_table = frappe.db.get_value(
            "MariaDB Table", qry.query_explain[0].table, "_table_name"
        )
        self.assertEqual(found_table, recorded_table)

        # check that rows don't duplicate on re-running apply
        for explain in explain_data:
            qry.apply_explain(explain)
        self.assertEqual(len(explain_data), len(qry.query_explain))
