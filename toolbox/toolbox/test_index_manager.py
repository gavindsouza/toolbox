# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import unittest
from unittest.mock import MagicMock, patch

from toolbox.utils import IndexCandidate, IndexCandidateType, Query, QueryBenchmark, Table


class TestIndexCandidateClass(unittest.TestCase):
    """Tests for IndexCandidate list subclass."""

    def test_default_type_is_where(self):
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        self.assertEqual(ic.type, IndexCandidateType.WHERE)

    def test_custom_type(self):
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q, type=IndexCandidateType.ORDER_BY)
        self.assertEqual(ic.type, IndexCandidateType.ORDER_BY)

    def test_append_deduplicates(self):
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        ic.append("col_a")
        ic.append("col_a")
        ic.append("col_b")
        self.assertEqual(list(ic), ["col_a", "col_b"])

    def test_repr(self):
        t = MagicMock()
        t.__repr__ = lambda self: "Table(test)"
        q = Query("SELECT 1", table=t)
        ic = IndexCandidate(query=q)
        ic.append("col_a")
        self.assertIn("IndexCandidate", repr(ic))

    def test_ctx_stored(self):
        q = Query("SELECT 1")
        ctx = [MagicMock(), MagicMock()]
        ic = IndexCandidate(query=q, ctx=ctx)
        self.assertEqual(ic.ctx, ctx)


class TestQueryClass(unittest.TestCase):
    """Tests for the Query wrapper class."""

    def test_init(self):
        q = Query("  SELECT 1  ", occurrence=5)
        self.assertEqual(q.sql, "SELECT 1")
        self.assertEqual(q.occurrence, 5)
        self.assertIsNone(q.table)

    def test_default_occurrence(self):
        q = Query("SELECT 1")
        self.assertEqual(q.occurrence, 1)

    def test_repr_short(self):
        q = Query("SELECT 1")
        self.assertEqual(repr(q), "Query(SELECT 1)")

    def test_repr_long(self):
        q = Query("SELECT * FROM `tabUser` WHERE name = 'Administrator'")
        self.assertIn("...", repr(q))

    def test_repr_with_table(self):
        t = MagicMock()
        q = Query("SELECT 1", table=t)
        self.assertIn("table=", repr(q))

    def test_get_sample_positional_params(self):
        q = Query("SELECT * FROM `tabUser` WHERE name = %s AND age > %s")
        sample = q.get_sample()
        self.assertNotIn("%s", sample)
        self.assertIn("1", sample)

    def test_get_sample_named_params(self):
        q = Query("SELECT * FROM `tabUser` WHERE name = %(user_name)s AND age > %(min_age)s")
        sample = q.get_sample()
        self.assertNotIn("%(user_name)s", sample)
        self.assertNotIn("%(min_age)s", sample)

    def test_get_sample_no_params(self):
        q = Query("SELECT * FROM `tabUser`")
        sample = q.get_sample()
        self.assertIn("SELECT", sample)

    def test_parsed_caches(self):
        q = Query("SELECT 1")
        p1 = q.parsed
        p2 = q.parsed
        self.assertIs(p1, p2)

    def test_d_parsed_caches(self):
        q = Query("SELECT name FROM tabUser")
        d1 = q.d_parsed
        d2 = q.d_parsed
        self.assertIs(d1, d2)


class TestIndexCandidateGeneration(unittest.TestCase):
    """Tests for Table.find_index_candidates with various SQL patterns."""

    def _make_table(self, name="tabNote"):
        t = Table.__new__(Table)
        t.id = "test-id"
        t.name = name
        return t

    def test_where_and_produces_composite_candidate(self):
        table = self._make_table()
        # Backtick-wrapped columns like Frappe generates — sqlparse needs them for proper Comparison parsing
        queries = [Query("SELECT `name` FROM `tabNote` WHERE `modified` = '2024-01-01' AND `owner` = 'Admin'")]
        candidates = table.find_index_candidates(queries)
        # AND clauses should produce a single composite index candidate
        found_composite = False
        for ic in candidates:
            if "modified" in ic and "owner" in ic:
                found_composite = True
        self.assertTrue(found_composite, f"Expected composite candidate, got: {candidates}")

    def test_where_or_produces_separate_candidates(self):
        table = self._make_table()
        queries = [Query("SELECT `name` FROM `tabNote` WHERE `modified` = '2024-01-01' OR `owner` = 'Admin'")]
        candidates = table.find_index_candidates(queries)
        # OR clauses should produce separate index candidates
        self.assertTrue(len(candidates) >= 2, f"Expected >= 2 candidates for OR, got: {candidates}")

    def test_order_by_produces_candidate(self):
        table = self._make_table()
        queries = [Query("SELECT name FROM `tabNote` WHERE modified = '2024-01-01' ORDER BY title")]
        candidates = table.find_index_candidates(queries)
        has_order_by = any(ic.type == IndexCandidateType.ORDER_BY for ic in candidates)
        self.assertTrue(has_order_by, f"Expected ORDER_BY candidate, got: {candidates}")

    def test_select_only_query_uses_d_parsed(self):
        """SELECT without WHERE should use find_index_candidates_from_select_query."""
        table = self._make_table("tabQuality Goal")
        queries = [Query(
            "SELECT name, frequency FROM `tabQuality Goal` ORDER BY modified DESC",
            table=table,
        )]
        candidates = table.find_index_candidates(queries)
        self.assertTrue(len(candidates) > 0)

    def test_qualifier_filters_low_occurrence(self):
        table = self._make_table()
        queries = [
            Query("SELECT name FROM `tabNote` WHERE modified = '2024-01-01'", occurrence=1),
            Query("SELECT name FROM `tabNote` WHERE owner = 'Admin'", occurrence=10),
        ]
        qualifier = lambda q: q.occurrence > 5
        candidates = table.find_index_candidates(queries, qualifier=qualifier)
        # Only the high-occurrence query should produce candidates
        for ic in candidates:
            self.assertGreater(ic.query.occurrence, 5)

    def test_no_duplicate_candidates(self):
        table = self._make_table()
        queries = [
            Query("SELECT name FROM `tabNote` WHERE modified = '2024-01-01'"),
            Query("SELECT name FROM `tabNote` WHERE modified = '2024-02-01'"),
        ]
        candidates = table.find_index_candidates(queries)
        # Should not have duplicate index candidates
        seen = []
        for ic in candidates:
            self.assertNotIn(list(ic), seen, f"Duplicate candidate: {ic}")
            seen.append(list(ic))


class TestQualifyIndexCandidates(unittest.TestCase):
    """Tests for Table.qualify_index_candidates (dedup, subset removal, 5-col cap)."""

    def _make_table(self, current_indexes=None):
        t = Table.__new__(Table)
        t.id = "test-id"
        t.name = "tabNote"
        return t

    def test_caps_at_5_columns(self):
        t = self._make_table()
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        for i in range(6):
            ic.append(f"col_{i}")
        self.assertEqual(len(ic), 6)

        with patch("toolbox.doctypes.MariaDBIndex") as mock_idx:
            mock_idx.get_indexes.return_value = []
            result = t.qualify_index_candidates([ic])
            self.assertEqual(len(result), 0)

    def test_removes_subset_candidates(self):
        """ic(A,B) should be removed if ic(A,B,C) is already in the list."""
        t = self._make_table()
        q = Query("SELECT 1")

        ic_large = IndexCandidate(query=q)
        ic_large.extend(["a", "b", "c"])

        ic_small = IndexCandidate(query=q)
        ic_small.extend(["a", "b"])

        with patch("toolbox.doctypes.MariaDBIndex") as mock_idx:
            mock_idx.get_indexes.return_value = []
            result = t.qualify_index_candidates([ic_large, ic_small])
            self.assertEqual(len(result), 1)
            self.assertEqual(list(result[0]), ["a", "b", "c"])

    def test_removes_duplicate_sets(self):
        """ic(A,B) should be removed if ic(B,A) is already there (same set)."""
        t = self._make_table()
        q = Query("SELECT 1")

        ic1 = IndexCandidate(query=q)
        ic1.extend(["a", "b"])

        ic2 = IndexCandidate(query=q)
        ic2.extend(["b", "a"])

        with patch("toolbox.doctypes.MariaDBIndex") as mock_idx:
            mock_idx.get_indexes.return_value = []
            result = t.qualify_index_candidates([ic1, ic2])
            self.assertEqual(len(result), 1)

    def test_skips_existing_indexes(self):
        t = self._make_table()
        q = Query("SELECT 1")

        ic = IndexCandidate(query=q)
        ic.extend(["name", "modified"])

        with patch("toolbox.doctypes.MariaDBIndex") as mock_idx:
            mock_idx.get_indexes.return_value = [["name", "modified"]]
            result = t.qualify_index_candidates([ic])
            self.assertEqual(len(result), 0)

    def test_keeps_non_overlapping_candidates(self):
        t = self._make_table()
        q = Query("SELECT 1")

        ic1 = IndexCandidate(query=q)
        ic1.extend(["a", "b"])

        ic2 = IndexCandidate(query=q)
        ic2.extend(["c", "d"])

        with patch("toolbox.doctypes.MariaDBIndex") as mock_idx:
            mock_idx.get_indexes.return_value = []
            result = t.qualify_index_candidates([ic1, ic2])
            self.assertEqual(len(result), 2)


class TestQueryBenchmarkLogic(unittest.TestCase):
    """Tests for QueryBenchmark comparison logic without DB access."""

    def test_get_unchanged_detects_no_improvement(self):
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        qbm = QueryBenchmark(index_candidates=[ic])

        # Identical before/after = no improvement
        qbm.before = [[{"r_rows": "100.00", "r_filtered": 50.0, "Extra": "Using where"}]]
        qbm.after = [[{"r_rows": "100.00", "r_filtered": 50.0, "Extra": "Using where"}]]

        unchanged = dict(qbm.get_unchanged_results())
        self.assertEqual(len(unchanged), 1)

    def test_get_unchanged_detects_improvement(self):
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        qbm = QueryBenchmark(index_candidates=[ic])

        # Fewer rows + better selectivity = improvement
        qbm.before = [[{"r_rows": "100.00", "r_filtered": 50.0, "Extra": "Using where"}]]
        qbm.after = [[{"r_rows": "10.00", "r_filtered": 100.0, "Extra": "Using index"}]]

        unchanged = dict(qbm.get_unchanged_results())
        self.assertEqual(len(unchanged), 0)

    def test_get_unchanged_worse_selectivity_is_unchanged(self):
        """If selectivity got worse, index is not helping."""
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        qbm = QueryBenchmark(index_candidates=[ic])

        qbm.before = [[{"r_rows": "100.00", "r_filtered": 80.0, "Extra": "Using where"}]]
        qbm.after = [[{"r_rows": "100.00", "r_filtered": 50.0, "Extra": "Using index"}]]

        unchanged = dict(qbm.get_unchanged_results())
        self.assertEqual(len(unchanged), 1)

    def test_compare_results_structure(self):
        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        qbm = QueryBenchmark(index_candidates=[ic])

        before = [[{"r_rows": "5.00", "r_filtered": 50.0, "Extra": "Using where"}]]
        after = [[{"r_rows": "1.00", "r_filtered": 100.0, "Extra": "Using index"}]]

        results = qbm.compare_results(before, after)
        self.assertEqual(len(results), 1)
        self.assertIn("before", results[0][0])
        self.assertIn("after", results[0][0])


class TestIndexManagerPipeline(unittest.TestCase):
    """Integration-style tests for the full index manager pipeline."""

    @patch("toolbox.index_manager.MariaDBIndex")
    @patch("toolbox.index_manager.frappe")
    def test_skips_nonexistent_tables(self, mock_frappe, mock_idx):
        from toolbox.index_manager import process_index_manager

        mock_frappe.get_all.return_value = [
            MagicMock(
                table="nonexistent_table_id",
                query="SELECT 1",
                parameterized_query="SELECT 1",
                occurrence=5,
            )
        ]

        with patch("toolbox.index_manager.Table") as MockTable:
            mock_table = MockTable.return_value
            mock_table.name = None  # table not found
            process_index_manager(verbose=True)
            mock_idx.create.assert_not_called()

    @patch("toolbox.index_manager.MariaDBIndex")
    @patch("toolbox.index_manager.frappe")
    def test_skip_backtest_creates_without_benchmark(self, mock_frappe, mock_idx):
        from toolbox.index_manager import process_index_manager

        mock_frappe.get_all.return_value = [
            MagicMock(
                table="test_table_id",
                query="SELECT name FROM tabUser",
                parameterized_query="SELECT name FROM tabUser",
                occurrence=5,
            )
        ]

        with patch("toolbox.index_manager.Table") as MockTable, \
             patch("toolbox.index_manager.get_table_id"):
            mock_table = MockTable.return_value
            mock_table.name = "tabUser"
            mock_table.exists.return_value = True
            ic = IndexCandidate(query=Query("SELECT name FROM tabUser"))
            ic.append("name")
            mock_table.find_index_candidates.return_value = [ic]
            mock_table.qualify_index_candidates.return_value = [ic]
            mock_idx.create.return_value = []

            process_index_manager(skip_backtest=True, verbose=True)

            mock_idx.create.assert_called_once()

    @patch("toolbox.index_manager.MariaDBIndex")
    @patch("toolbox.index_manager.frappe")
    def test_no_candidates_skips_table(self, mock_frappe, mock_idx):
        from toolbox.index_manager import process_index_manager

        mock_frappe.get_all.return_value = [
            MagicMock(
                table="test_table_id",
                query="SELECT 1",
                parameterized_query="SELECT 1",
                occurrence=5,
            )
        ]

        with patch("toolbox.index_manager.Table") as MockTable, \
             patch("toolbox.index_manager.get_table_id"):
            mock_table = MockTable.return_value
            mock_table.name = "tabUser"
            mock_table.exists.return_value = True
            mock_table.find_index_candidates.return_value = []
            mock_table.qualify_index_candidates.return_value = []

            process_index_manager(verbose=True)

            mock_idx.create.assert_not_called()


class TestToolboxIndexPrefix(unittest.TestCase):
    """Test that toolbox_index_ prefix is applied correctly."""

    def test_index_name_format(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import (
            TOOLBOX_INDEX_PREFIX,
            get_index_name,
        )

        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        ic.extend(["col_a", "col_b"])

        name = get_index_name(ic)
        self.assertTrue(name.startswith(TOOLBOX_INDEX_PREFIX))
        self.assertEqual(name, "toolbox_index_col_a_col_b")

    def test_single_column_index_name(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import get_index_name

        q = Query("SELECT 1")
        ic = IndexCandidate(query=q)
        ic.append("name")

        name = get_index_name(ic)
        self.assertEqual(name, "toolbox_index_name")


if __name__ == "__main__":
    unittest.main()
