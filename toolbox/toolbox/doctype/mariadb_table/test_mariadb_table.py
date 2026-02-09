# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import unittest

import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.db_adapter import is_postgres
from toolbox.toolbox.doctype.mariadb_table.mariadb_table import MariaDBTable
from toolbox.utils import record_query


class TestMariaDBTable(FrappeTestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        frappe.db.rollback()
        return super().tearDown()

    @unittest.skipIf(is_postgres(), "EXPLAIN format differs on PostgreSQL")
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


class TestValidateTableName(FrappeTestCase):
    def _make_table_doc(self, table_name):
        doc = MariaDBTable.__new__(MariaDBTable)
        doc._table_name = table_name
        return doc

    def test_valid_existing_table_passes(self):
        doc = self._make_table_doc("tabDocType")
        doc._validate_table_name()

    def test_none_table_name_rejected(self):
        doc = self._make_table_doc(None)
        self.assertRaises(frappe.ValidationError, doc._validate_table_name)

    def test_empty_table_name_rejected(self):
        doc = self._make_table_doc("")
        self.assertRaises(frappe.ValidationError, doc._validate_table_name)

    def test_sql_injection_in_table_name_rejected(self):
        injection_attempts = [
            "tabFoo`; DROP TABLE tabDocType; --",
            "`; SELECT * FROM tabUser; --",
            "1table",
            "tab;injection",
            "tab`backtick",
        ]
        for attempt in injection_attempts:
            doc = self._make_table_doc(attempt)
            self.assertRaises(frappe.ValidationError, doc._validate_table_name)

    def test_nonexistent_table_rejected(self):
        doc = self._make_table_doc("tabNonExistentTableXYZ123")
        self.assertRaises(frappe.ValidationError, doc._validate_table_name)

    def test_analyze_calls_validation(self):
        doc = self._make_table_doc("tabFoo`; DROP TABLE tabDocType")
        self.assertRaises(frappe.ValidationError, doc.analyze)

    def test_optimize_calls_validation(self):
        doc = self._make_table_doc("tabFoo`; DROP TABLE tabDocType")
        self.assertRaises(frappe.ValidationError, doc.optimize)
