import frappe
from frappe.tests.utils import FrappeTestCase

from toolbox.utils import (
    EXPLAINABLE_QUERIES,
    IndexCandidate,
    Query,
    QueryBenchmark,
    Table,
    _explain_and_record_query,
    _increment_query_count,
    process_sql_metadata_chunk,
    record_table,
)


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

    def test_table_find_index_where_candidates(self):
        queries = [
            Query(
                "select `name` from `tabNote` where `modified` = `creation` or `creation` > `modified`"
            )
        ]
        table_id = frappe.db.get_value("MariaDB Table", {"_table_name": "tabNote"})
        table = Table(table_id)
        index_candidates = table.find_index_candidates(queries)
        self.assertEqual(index_candidates, [["modified", "creation"], ["creation", "modified"]])

        queries = [
            Query(
                "select `name` from `tabNote` where `modified` = `creation` or `creation` > '2023-02-13 13:35:01.556111' order by `title`"
            ),
        ]
        index_candidates = table.find_index_candidates(queries)
        self.assertEqual(index_candidates, [["modified", "creation"], ["creation"], ["title"]])

    def test_table_find_index_select_candidates(self):
        table = Table(None)
        table.name = (
            "tabQuality Goal Non Existent Table"  # this table does not exist on the database
        )

        q_1_ic = table.find_index_candidates(
            [
                Query(
                    f"select `name`, `frequency`, `date`, `weekday` from `{table.name}` order by `{table.name}`.`modified` DESC",
                    table=table,
                ),
            ]
        )
        self.assertIn(["name", "frequency", "date", "weekday"], q_1_ic)
        self.assertIn(["modified"], q_1_ic)
        assert len(q_1_ic) == 2

        q_2_ic = table.find_index_candidates(
            [
                Query(
                    f"select `name` as `aliased_name` from `{table.name}` order by `{table.name}`.`modified` DESC",
                    table=table,
                ),
            ]
        )
        self.assertIn(["name"], q_2_ic)
        self.assertIn(["modified"], q_2_ic)
        assert len(q_2_ic) == 2

    def test_query_benchmark_no_changes(self):
        index_candidates = [
            IndexCandidate(query=Query(qry))
            for qry in (
                "SELECT 1",
                "SELECT `name` from `tabNote`",
            )
        ]

        with QueryBenchmark(index_candidates=index_candidates) as qbm:
            ...
        results = dict(qbm.get_unchanged_results())

        self.assertEqual(len(results), 2)
        for _, data in results.items():
            self.assertDictEqual(data["before"], data["after"])


class TestCompareResultsListIndependence(FrappeTestCase):
    def test_sublists_are_independent(self):
        """Verify compare_results produces independent sublists, not shared references."""
        ic = IndexCandidate(query=Query("SELECT 1"))
        qbm = QueryBenchmark(index_candidates=[ic])

        before = [
            [{"r_rows": "1.00", "r_filtered": 100.0, "Extra": ""}],
            [{"r_rows": "2.00", "r_filtered": 50.0, "Extra": "Using where"}],
            [{"r_rows": "3.00", "r_filtered": 75.0, "Extra": ""}],
        ]
        after = [
            [{"r_rows": "1.00", "r_filtered": 100.0, "Extra": ""}],
            [{"r_rows": "1.00", "r_filtered": 80.0, "Extra": "Using index"}],
            [{"r_rows": "3.00", "r_filtered": 75.0, "Extra": ""}],
        ]

        results = qbm.compare_results(before, after)

        self.assertEqual(len(results), 3)
        # Each sublist must be a different object
        self.assertIsNot(results[0], results[1])
        self.assertIsNot(results[1], results[2])
        self.assertIsNot(results[0], results[2])

        # Modifying one sublist must not affect others
        results[0].append("sentinel")
        self.assertNotIn("sentinel", results[1])
        self.assertNotIn("sentinel", results[2])

    def test_compare_results_content_correct(self):
        """Verify compare_results produces correct before/after data."""
        ic = IndexCandidate(query=Query("SELECT 1"))
        qbm = QueryBenchmark(index_candidates=[ic])

        before = [[{"r_rows": "5.00", "r_filtered": 50.0, "Extra": "Using where"}]]
        after = [[{"r_rows": "1.00", "r_filtered": 100.0, "Extra": "Using index"}]]

        results = qbm.compare_results(before, after)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]), 1)
        self.assertEqual(results[0][0]["before"]["r_rows"], "5.00")
        self.assertEqual(results[0][0]["after"]["r_rows"], "1.00")
        self.assertEqual(results[0][0]["before"]["r_filtered"], 50.0)
        self.assertEqual(results[0][0]["after"]["r_filtered"], 100.0)


class TestIncrementQueryCount(FrappeTestCase):
    def tearDown(self) -> None:
        frappe.db.rollback()
        return super().tearDown()

    def test_returns_false_for_new_query(self):
        mq_table = frappe.qb.DocType("MariaDB Query")
        result = _increment_query_count(mq_table, "SELECT `nonexistent_xyz_query` FROM dual", 5)
        self.assertFalse(result)

    def test_returns_true_for_existing_query(self):
        from toolbox.utils import record_query

        p_query = "SELECT %s FROM `tabDocType` WHERE name = %s"
        qr = record_query("SELECT 1 FROM `tabDocType` WHERE name = 1", p_query=p_query)
        qr.occurrence = 1
        qr.insert()
        frappe.db.commit()

        mq_table = frappe.qb.DocType("MariaDB Query")
        result = _increment_query_count(mq_table, p_query, 3)
        self.assertTrue(result)

        updated = frappe.get_doc("MariaDB Query", qr.name)
        self.assertEqual(updated.occurrence, 4)


class TestExplainAndRecordQuery(FrappeTestCase):
    def tearDown(self) -> None:
        frappe.db.rollback()
        return super().tearDown()

    def test_returns_record_for_valid_query(self):
        result = _explain_and_record_query("SELECT `name` FROM `tabDocType`", 5)
        self.assertIsNotNone(result)
        self.assertEqual(result.occurrence, 5)
        self.assertTrue(result.query_explain)

    def test_returns_none_for_invalid_query(self):
        result = _explain_and_record_query("SELECT * FROM `nonexistent_table_xyz`", 1)
        self.assertIsNone(result)


class TestProcessSqlMetadataChunk(FrappeTestCase):
    def tearDown(self) -> None:
        frappe.db.rollback()
        return super().tearDown()

    def test_skips_non_explainable_queries(self):
        queries = {
            "SET @variable = 1": 5,
            "SHOW TABLES": 3,
        }
        result = process_sql_metadata_chunk(queries)
        self.assertEqual(result.total_sql_count, 8)
        self.assertEqual(result.unique_sql_count, 2)

    def test_handles_bytes_keys(self):
        queries = {
            b"SET @x = 1": 1,
        }
        result = process_sql_metadata_chunk(queries)
        self.assertEqual(result.total_sql_count, 1)

    def test_explainable_queries_constant(self):
        self.assertIn("select", EXPLAINABLE_QUERIES)
        self.assertIn("insert", EXPLAINABLE_QUERIES)
        self.assertIn("update", EXPLAINABLE_QUERIES)
        self.assertIn("delete", EXPLAINABLE_QUERIES)


class TestMigrationPatch(FrappeTestCase):
    def test_patches_txt_entry_exists(self):
        import os

        patches_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "patches.txt"
        )
        with open(patches_path) as f:
            content = f.read()
        self.assertIn("toolbox.patches.rename_occurence_to_occurrence", content)

    def test_patch_module_is_importable(self):
        from toolbox.patches import rename_occurence_to_occurrence

        self.assertTrue(hasattr(rename_occurence_to_occurrence, "execute"))
        self.assertTrue(callable(rename_occurence_to_occurrence.execute))
