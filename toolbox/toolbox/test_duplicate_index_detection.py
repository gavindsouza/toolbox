# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt
#
# TDD tests for Feature 2: Duplicate & Redundant Index Detection

import unittest
from unittest.mock import MagicMock, patch


class TestFindDuplicateIndexes(unittest.TestCase):
    """Test detection of exact duplicate indexes."""

    def test_exact_duplicates_detected(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_duplicate_indexes

        indexes = [
            {"key_name": "idx_a", "columns": ["name", "owner"]},
            {"key_name": "idx_b", "columns": ["name", "owner"]},
        ]
        duplicates = find_duplicate_indexes(indexes)
        self.assertEqual(len(duplicates), 1)
        self.assertEqual(duplicates[0]["redundant"], "idx_b")
        self.assertEqual(duplicates[0]["superseded_by"], "idx_a")

    def test_no_duplicates_returns_empty(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_duplicate_indexes

        indexes = [
            {"key_name": "idx_a", "columns": ["name"]},
            {"key_name": "idx_b", "columns": ["owner"]},
        ]
        duplicates = find_duplicate_indexes(indexes)
        self.assertEqual(len(duplicates), 0)

    def test_primary_key_excluded(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_duplicate_indexes

        indexes = [
            {"key_name": "PRIMARY", "columns": ["name"]},
            {"key_name": "idx_a", "columns": ["name"]},
        ]
        duplicates = find_duplicate_indexes(indexes)
        # idx_a duplicates PRIMARY, but PRIMARY should not be recommended for drop
        for d in duplicates:
            self.assertNotEqual(d["redundant"], "PRIMARY")


class TestFindRedundantIndexes(unittest.TestCase):
    """Test detection of left-prefix redundant indexes."""

    def test_left_prefix_detected(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_redundant_indexes

        indexes = [
            {"key_name": "idx_abc", "columns": ["a", "b", "c"]},
            {"key_name": "idx_a", "columns": ["a"]},
            {"key_name": "idx_ab", "columns": ["a", "b"]},
        ]
        redundant = find_redundant_indexes(indexes)
        redundant_names = [r["redundant"] for r in redundant]
        self.assertIn("idx_a", redundant_names)
        self.assertIn("idx_ab", redundant_names)

    def test_non_prefix_not_detected(self):
        """Index (B, C) is NOT a left-prefix of (A, B, C)."""
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_redundant_indexes

        indexes = [
            {"key_name": "idx_abc", "columns": ["a", "b", "c"]},
            {"key_name": "idx_bc", "columns": ["b", "c"]},
        ]
        redundant = find_redundant_indexes(indexes)
        redundant_names = [r["redundant"] for r in redundant]
        self.assertNotIn("idx_bc", redundant_names)

    def test_primary_key_not_marked_redundant(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_redundant_indexes

        indexes = [
            {"key_name": "PRIMARY", "columns": ["name"]},
            {"key_name": "idx_name_owner", "columns": ["name", "owner"]},
        ]
        redundant = find_redundant_indexes(indexes)
        redundant_names = [r["redundant"] for r in redundant]
        self.assertNotIn("PRIMARY", redundant_names)

    def test_same_length_not_redundant(self):
        """Two indexes with same columns in different order are NOT left-prefix redundant."""
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_redundant_indexes

        indexes = [
            {"key_name": "idx_ab", "columns": ["a", "b"]},
            {"key_name": "idx_ba", "columns": ["b", "a"]},
        ]
        redundant = find_redundant_indexes(indexes)
        self.assertEqual(len(redundant), 0)

    def test_single_index_no_redundancy(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import find_redundant_indexes

        indexes = [{"key_name": "idx_a", "columns": ["a"]}]
        redundant = find_redundant_indexes(indexes)
        self.assertEqual(len(redundant), 0)


class TestAnalyzeTableIndexes(unittest.TestCase):
    """Test the combined analysis that finds both duplicates and redundant indexes."""

    def test_returns_both_types(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import analyze_table_indexes

        indexes = [
            {"key_name": "idx_abc", "columns": ["a", "b", "c"]},
            {"key_name": "idx_ab", "columns": ["a", "b"]},      # left-prefix redundant
            {"key_name": "idx_xy", "columns": ["x", "y"]},
            {"key_name": "idx_xy2", "columns": ["x", "y"]},     # exact duplicate
        ]
        result = analyze_table_indexes(indexes)
        self.assertIn("duplicates", result)
        self.assertIn("redundant", result)
        self.assertTrue(len(result["duplicates"]) > 0 or len(result["redundant"]) > 0)

    def test_empty_indexes(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import analyze_table_indexes

        result = analyze_table_indexes([])
        self.assertEqual(result["duplicates"], [])
        self.assertEqual(result["redundant"], [])


class TestReduceIndexesToColumnLists(unittest.TestCase):
    """Test helper that converts raw INFORMATION_SCHEMA rows to column lists."""

    def test_groups_by_index_name(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import reduce_indexes_to_column_lists

        raw_indexes = [
            {"key_name": "idx_a", "column_name": "col1", "seq_id": 1},
            {"key_name": "idx_a", "column_name": "col2", "seq_id": 2},
            {"key_name": "idx_b", "column_name": "col3", "seq_id": 1},
        ]
        result = reduce_indexes_to_column_lists(raw_indexes)
        self.assertEqual(len(result), 2)
        idx_a = next(r for r in result if r["key_name"] == "idx_a")
        self.assertEqual(idx_a["columns"], ["col1", "col2"])

    def test_respects_seq_order(self):
        from toolbox.toolbox.doctype.mariadb_index.mariadb_index import reduce_indexes_to_column_lists

        raw_indexes = [
            {"key_name": "idx_a", "column_name": "col2", "seq_id": 2},
            {"key_name": "idx_a", "column_name": "col1", "seq_id": 1},
        ]
        result = reduce_indexes_to_column_lists(raw_indexes)
        self.assertEqual(result[0]["columns"], ["col1", "col2"])


if __name__ == "__main__":
    unittest.main()
