import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.utils import record_query, record_table


class TestToolBoxUtils(FrappeTestCase):
    def tearDown(self) -> None:
        frappe.db.rollback()
        return super().tearDown()

    def test_record_table(self):
        table_name = "tabTestTable"

        table_id = record_table(table_name)
        self.assertIsNotNone(table_id)
        self.assertNotEqual(table_id, table_name)
        self.assertTrue(frappe.db.exists("MariaDB Table", table_id))

    def test_record_query(self):
        query = "SELECT * FROM `tabTestTable`"

        mqry = record_query(query).save()
        self.assertEqual(mqry.occurence, 1)
        mqry_again = record_query(query).save()
        self.assertEqual(mqry.name, mqry_again.name)
        self.assertEqual(mqry_again.occurence, 2)
