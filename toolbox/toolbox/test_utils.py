import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.utils import Table, record_query, record_table


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

    def test_table_find_index_where_candidates(self):
        queries = [
            "select `name` from `tabNote` where `modified` = `creation` or `creation` > `modified`",
        ]
        table_id = frappe.db.get_value("MariaDB Table", {"_table_name": "tabNote"})
        table = Table(table_id)
        index_candidates = table.find_index_candidates(queries)
        self.assertEqual(index_candidates, [["modified", "creation"], ["creation", "modified"]])

        queries = [
            "select `name` from `tabNote` where `modified` = `creation` or `creation` > '2023-02-13 13:35:01.556111'",
        ]
        index_candidates = table.find_index_candidates(queries)
        self.assertEqual(index_candidates, [["modified", "creation"], ["creation"]])

    def test_table_find_index_select_candidates(self):
        queries = [
            "select `name`, `frequency`, `date`, `weekday` from `tabQuality Goal` order by `tabQuality Goal`.`modified` DESC",
        ]
        table_id = frappe.db.get_value("MariaDB Table", {"_table_name": "tabQuality Goal"})
        table = Table(table_id)
        index_candidates = table.find_index_candidates(queries)
        self.assertEqual(index_candidates, [["name", "frequency", "date", "weekday"]])
