# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import unittest

from toolbox.db_adapter import is_postgres
from toolbox.toolbox.doctype.mariadb_index.mariadb_index import (
    ALLOWED_OPERATORS,
    FIELD_ALIAS,
    TOOLBOX_INDEX_PREFIX,
    get_accessible_fields,
    get_args,
    get_column_name,
    get_filter_clause,
    get_index_query,
    get_mapped_field,
    wrap_query_field,
)

_PG = is_postgres()


class TestWrapQueryField(unittest.TestCase):
    """Test identifier quoting for field names."""

    def test_wraps_plain_field(self):
        expected = '"name"' if _PG else "`name`"
        self.assertEqual(wrap_query_field("name"), expected)

    def test_does_not_double_wrap(self):
        self.assertEqual(wrap_query_field("`name`"), "`name`")

    def test_wraps_field_with_spaces(self):
        expected = '"my field"' if _PG else "`my field`"
        self.assertEqual(wrap_query_field("my field"), expected)


class TestGetColumnName(unittest.TestCase):
    def test_known_alias_maps_correctly(self):
        result = get_column_name("table")
        if _PG:
            self.assertIn("tablename", result)
        else:
            self.assertIn("TABLE_NAME", result)

    def test_unknown_field_returned_wrapped(self):
        result = get_column_name("unknown_col")
        expected = '"unknown_col"' if _PG else "`unknown_col`"
        self.assertEqual(result, expected)


class TestGetAccessibleFields(unittest.TestCase):
    """Test field whitelist for SELECT."""

    def test_wildcard_passes_through(self):
        self.assertEqual(get_accessible_fields(["*"]), ["*"])

    def test_count_star_passes_through(self):
        self.assertEqual(get_accessible_fields(["count(*)"]), ["count(*)"])
        self.assertEqual(get_accessible_fields(["count(*) as result"]), ["count(*) as result"])

    def test_known_fields_allowed(self):
        result = get_accessible_fields(["name", "table", "key_name"])
        self.assertEqual(len(result), 3)

    def test_unknown_fields_filtered(self):
        result = get_accessible_fields(["name", "hacked_field", "key_name"])
        self.assertNotIn("hacked_field", result)
        self.assertEqual(len(result), 2)

    def test_dotted_field_notation(self):
        result = get_accessible_fields(["s.name", "s.table"])
        self.assertEqual(len(result), 2)

    def test_backtick_fields_normalized(self):
        result = get_accessible_fields(["`name`", "`table`"])
        self.assertEqual(len(result), 2)


class TestGetMappedField(unittest.TestCase):
    """Test ORDER BY field mapping with direction validation."""

    def test_valid_field_with_desc(self):
        self.assertEqual(get_mapped_field("cardinality desc"), "cardinality desc")

    def test_valid_field_with_asc(self):
        self.assertEqual(get_mapped_field("cardinality asc"), "cardinality asc")

    def test_invalid_direction_defaults_to_asc(self):
        self.assertEqual(get_mapped_field("cardinality DROP"), "cardinality asc")

    def test_unknown_field_returns_none(self):
        self.assertIsNone(get_mapped_field("nonexistent_field desc"))

    def test_field_only_no_direction(self):
        result = get_mapped_field("cardinality")
        self.assertEqual(result, "cardinality asc")

    def test_dotted_notation(self):
        result = get_mapped_field("s.cardinality desc")
        self.assertEqual(result, "cardinality desc")


class TestGetFilterClause(unittest.TestCase):
    """Extended tests for filter clause building."""

    def test_empty_filters(self):
        clause, params = get_filter_clause([])
        self.assertEqual(clause, "")
        self.assertEqual(params, ())

    def test_eq_operator(self):
        clause, params = get_filter_clause([["key_name", "=", "PRIMARY"]])
        self.assertIn("WHERE", clause)
        self.assertIn("%s", clause)
        self.assertEqual(params, ("PRIMARY",))

    def test_not_eq_operator(self):
        clause, params = get_filter_clause([["key_name", "!=", "PRIMARY"]])
        self.assertIn("!=", clause)

    def test_less_than(self):
        clause, params = get_filter_clause([["cardinality", "<", 100]])
        self.assertIn("<", clause)
        self.assertEqual(params, (100,))

    def test_greater_than(self):
        clause, params = get_filter_clause([["cardinality", ">", 100]])
        self.assertIn(">", clause)

    def test_like_operator(self):
        clause, params = get_filter_clause([["key_name", "like", "toolbox%"]])
        self.assertIn("like", clause)
        self.assertEqual(params, ("toolbox%",))

    def test_not_like_operator(self):
        clause, params = get_filter_clause([["key_name", "not like", "toolbox%"]])
        self.assertIn("not like", clause)

    def test_in_operator_with_list(self):
        clause, params = get_filter_clause([["key_name", "in", ["a", "b", "c"]]])
        self.assertEqual(params, ("a", "b", "c"))
        self.assertEqual(clause.count("%s"), 3)

    def test_not_in_operator_with_list(self):
        clause, params = get_filter_clause([["key_name", "not in", ["a", "b"]]])
        self.assertIn("not in", clause)
        self.assertEqual(params, ("a", "b"))

    def test_in_operator_with_scalar(self):
        clause, params = get_filter_clause([["key_name", "in", "single"]])
        self.assertEqual(params, ("single",))

    def test_multiple_filters_joined_with_and(self):
        clause, params = get_filter_clause([
            ["key_name", "=", "PRIMARY"],
            ["table", "like", "tab%"],
        ])
        self.assertIn("AND", clause)
        self.assertEqual(len(params), 2)

    def test_four_element_filter_skips_doctype(self):
        clause, params = get_filter_clause([["MariaDB Query", "key_name", "=", "PRIMARY"]])
        self.assertEqual(params, ("PRIMARY",))

    def test_all_operators_accepted(self):
        for op in ALLOWED_OPERATORS:
            clause, _ = get_filter_clause([["key_name", op, "val"]])
            self.assertTrue(clause.startswith("WHERE"), f"Failed for operator: {op}")


class TestGetArgs(unittest.TestCase):
    """Test argument normalization for get_list/get_count."""

    def test_defaults(self):
        args = get_args()
        self.assertEqual(args["filters"], [])
        self.assertEqual(args["fields"], [])
        self.assertEqual(args["order_by"], "")

    def test_limit_remapped_to_page_length(self):
        args = get_args({"limit": 10})
        self.assertEqual(args["page_length"], 10)
        self.assertNotIn("limit", args)

    def test_limit_page_length_remapped(self):
        args = get_args({"limit_page_length": 20})
        self.assertEqual(args["page_length"], 20)
        self.assertNotIn("limit_page_length", args)

    def test_kwargs_override_args(self):
        args = get_args({"order_by": "name"}, {"order_by": "modified"})
        self.assertEqual(args["order_by"], "modified")

    def test_dict_filters_converted_to_list(self):
        args = get_args({"filters": {"key_name": ["=", "PRIMARY"]}})
        self.assertIsInstance(args["filters"], list)
        self.assertEqual(args["filters"][0], ["key_name", "=", "PRIMARY"])

    def test_is_set_converted(self):
        args = get_args({"filters": [["DocType", "key_name", "is", "set"]]})
        self.assertEqual(args["filters"][0][2], "!=")
        self.assertEqual(args["filters"][0][3], "")

    def test_is_not_set_converted(self):
        args = get_args({"filters": [["DocType", "key_name", "is", "not set"]]})
        self.assertEqual(args["filters"][0][2], "=")
        self.assertEqual(args["filters"][0][3], "")


class TestGetIndexQuery(unittest.TestCase):
    """Test the query builder for INFORMATION_SCHEMA."""

    def test_no_filters_no_subquery(self):
        query, params = get_index_query([], [])
        if _PG:
            self.assertIn("pg_indexes", query)
        else:
            self.assertIn("INFORMATION_SCHEMA.STATISTICS", query)
        self.assertEqual(params, ())

    def test_with_fields_wraps_in_subquery(self):
        query, params = get_index_query(["name", "table"], [])
        self.assertIn("SELECT name, table FROM (", query)
        self.assertIn(") as t", query)

    def test_with_filters_adds_where(self):
        query, params = get_index_query([], [["key_name", "=", "PRIMARY"]])
        self.assertIn("WHERE", query)
        self.assertEqual(params, ("PRIMARY",))


class TestFieldAlias(unittest.TestCase):
    """Test that FIELD_ALIAS mappings are correct."""

    def test_expected_aliases_present(self):
        expected = ["name", "owner", "table", "key_name", "column_name",
                     "non_unique", "index_type", "cardinality", "collation", "seq_id"]
        for field in expected:
            self.assertIn(field, FIELD_ALIAS, f"Missing alias: {field}")

    def test_toolbox_index_prefix_value(self):
        self.assertEqual(TOOLBOX_INDEX_PREFIX, "toolbox_index_")


if __name__ == "__main__":
    unittest.main()
