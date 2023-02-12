# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.doctypes import MariaDBIndex


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
