# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.doctypes import MariaDBIndex
from toolbox.toolbox.doctype.mariadb_index.mariadb_index import (
    ALLOWED_OPERATORS,
    _validate_identifier,
    get_filter_clause,
    get_mapped_field,
)


class TestMariaDBIndex(FrappeTestCase):
    def test_get_list(self):
        i_1 = MariaDBIndex.get_list(limit=1)
        i_2 = MariaDBIndex.get_list({"limit": 1})
        i_3 = MariaDBIndex.get_list(limit=1, fields=["name"])
        self.assertEqual(i_1, i_2)
        self.assertEqual(len(i_1), 1)
        self.assertTrue("name" in i_3[0])
        self.assertEqual(len(i_3[0]), 1)

    def test_get_count(self):
        c_1 = MariaDBIndex.get_count()
        c_2 = MariaDBIndex.get_count({"filters": []})
        self.assertEqual(c_1, c_2)

        c_3 = MariaDBIndex.get_count({"limit": 20})
        self.assertNotEqual(c_3, 20)  # limit is ignored

        c_4 = MariaDBIndex.get_count({"filters": [["MariaDB Query", "key_name", "=", "PRIMARY"]]})
        self.assertTrue(c_4 > 0)
        self.assertTrue(c_4 < c_3)

    def test_get_doc(self):
        last_doc = MariaDBIndex.get_last_doc()
        last_doc_int = frappe.get_last_doc("MariaDB Index")
        doc = frappe.get_doc("MariaDB Index", last_doc.name)

        self.assertIsInstance(last_doc, MariaDBIndex)
        self.assertTrue(last_doc.name)
        self.assertDictEqual(last_doc.as_dict(), last_doc_int.as_dict())
        self.assertIsInstance(doc, MariaDBIndex)
        self.assertDictEqual(doc.as_dict(), last_doc.as_dict())

    def test_get_indexes(self):
        indexes = MariaDBIndex.get_indexes("tabDocType")
        self.assertTrue(indexes)
        self.assertIsInstance(indexes, list)
        self.assertIsInstance(indexes[0], frappe._dict)

        indexes = MariaDBIndex.get_indexes("tabDocType", reduce=True)
        self.assertTrue(indexes)
        self.assertIsInstance(indexes, list)
        self.assertIsInstance(indexes[0], list)
        self.assertIsInstance(indexes[0][0], str)


class TestFilterClauseSecurity(FrappeTestCase):
    def test_empty_filters_returns_empty(self):
        clause, params = get_filter_clause([])
        self.assertEqual(clause, "")
        self.assertEqual(params, ())

    def test_simple_eq_filter_is_parameterized(self):
        filters = [["key_name", "=", "PRIMARY"]]
        clause, params = get_filter_clause(filters)
        self.assertIn("%s", clause)
        self.assertNotIn("PRIMARY", clause)
        self.assertEqual(params, ("PRIMARY",))

    def test_like_filter_is_parameterized(self):
        filters = [["key_name", "like", "toolbox_%"]]
        clause, params = get_filter_clause(filters)
        self.assertIn("%s", clause)
        self.assertNotIn("toolbox_", clause)
        self.assertEqual(params, ("toolbox_%",))

    def test_malicious_value_not_interpolated(self):
        malicious = "'; DROP TABLE users; --"
        filters = [["key_name", "=", malicious]]
        clause, params = get_filter_clause(filters)
        self.assertNotIn(malicious, clause)
        self.assertIn("%s", clause)
        self.assertEqual(params, (malicious,))

    def test_invalid_operator_is_rejected(self):
        filters = [["key_name", "; DROP TABLE", "value"]]
        self.assertRaises(frappe.ValidationError, get_filter_clause, filters)

    def test_all_allowed_operators_accepted(self):
        for op in ALLOWED_OPERATORS:
            clause, params = get_filter_clause([["key_name", op, "test"]])
            self.assertTrue(clause.startswith("WHERE"))

    def test_in_operator_with_list(self):
        filters = [["key_name", "in", ["a", "b", "c"]]]
        clause, params = get_filter_clause(filters)
        self.assertIn("in", clause.lower())
        self.assertEqual(params, ("a", "b", "c"))
        self.assertEqual(clause.count("%s"), 3)

    def test_in_operator_with_scalar(self):
        filters = [["key_name", "in", "single_val"]]
        clause, params = get_filter_clause(filters)
        self.assertEqual(params, ("single_val",))

    def test_multiple_filters_produce_multiple_params(self):
        filters = [
            ["key_name", "=", "PRIMARY"],
            ["table", "like", "tab%"],
        ]
        clause, params = get_filter_clause(filters)
        self.assertEqual(len(params), 2)
        self.assertIn("AND", clause)

    def test_four_element_filter_uses_last_three(self):
        filters = [["MariaDB Query", "key_name", "=", "PRIMARY"]]
        clause, params = get_filter_clause(filters)
        self.assertEqual(params, ("PRIMARY",))

    def test_parameterized_filters_work_end_to_end(self):
        results = MariaDBIndex.get_list(
            filters=[["key_name", "=", "PRIMARY"]],
            limit=5,
        )
        self.assertIsInstance(results, list)
        for r in results:
            self.assertEqual(r["key_name"], "PRIMARY")

    def test_parameterized_count_works_end_to_end(self):
        count = MariaDBIndex.get_count(
            {"filters": [["key_name", "=", "PRIMARY"]]}
        )
        self.assertIsInstance(count, int)
        self.assertGreater(count, 0)

    def test_limit_cast_to_int(self):
        self.assertRaises(ValueError, MariaDBIndex.get_list, limit="5; DROP TABLE users")

    def test_order_by_direction_validated(self):
        result = get_mapped_field("cardinality desc")
        self.assertEqual(result, "cardinality desc")

        result = get_mapped_field("cardinality asc")
        self.assertEqual(result, "cardinality asc")

        result = get_mapped_field("cardinality DROP")
        self.assertEqual(result, "cardinality asc")

        result = get_mapped_field("cardinality ; DELETE FROM")
        self.assertEqual(result, "cardinality asc")


class TestValidateIdentifier(FrappeTestCase):
    def test_valid_identifiers_pass(self):
        for name in ("tabDocType", "my_table", "column_name", "A", "tab With Space"):
            _validate_identifier(name, "test")

    def test_empty_string_rejected(self):
        self.assertRaises(frappe.ValidationError, _validate_identifier, "", "test")

    def test_none_rejected(self):
        self.assertRaises(frappe.ValidationError, _validate_identifier, None, "test")

    def test_sql_injection_in_identifier_rejected(self):
        injection_attempts = [
            "table`; DROP TABLE users; --",
            "col`; SELECT * FROM",
            "1invalid",
            "`backtick_start",
            "semi;colon",
            "paren(theses)",
            "dash-name",
        ]
        for attempt in injection_attempts:
            self.assertRaises(
                frappe.ValidationError,
                _validate_identifier,
                attempt,
                "test",
            )
