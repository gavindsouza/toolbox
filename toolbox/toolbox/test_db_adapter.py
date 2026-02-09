# Copyright (c) 2025, Gavin D'souza and Contributors
# See license.txt

import unittest
from unittest.mock import MagicMock, patch


class TestGetDbType(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_returns_mariadb_by_default(self, mock_frappe):
        mock_frappe.conf = MagicMock(spec=[])
        from toolbox.db_adapter import get_db_type

        self.assertEqual(get_db_type(), "mariadb")

    @patch("toolbox.db_adapter.frappe")
    def test_returns_configured_db_type(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_db_type

        self.assertEqual(get_db_type(), "postgres")


class TestIsMariadb(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_true_for_mariadb(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import is_mariadb

        self.assertTrue(is_mariadb())

    @patch("toolbox.db_adapter.frappe")
    def test_false_for_postgres(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import is_mariadb

        self.assertFalse(is_mariadb())


class TestIsPostgres(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_true_for_postgres(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import is_postgres

        self.assertTrue(is_postgres())

    @patch("toolbox.db_adapter.frappe")
    def test_false_for_mariadb(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import is_postgres

        self.assertFalse(is_postgres())


class TestQuoteIdentifier(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_backticks(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import quote_identifier

        self.assertEqual(quote_identifier("my_table"), "`my_table`")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_double_quotes(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import quote_identifier

        self.assertEqual(quote_identifier("my_table"), '"my_table"')


class TestTableExists(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_uses_show_tables(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        mock_frappe.db.sql.return_value = [("tabUser",)]
        from toolbox.db_adapter import table_exists

        self.assertTrue(table_exists("tabUser"))
        mock_frappe.db.sql.assert_called_once_with("SHOW TABLES LIKE %s", ("tabUser",))

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_uses_pg_tables(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        mock_frappe.db.sql.return_value = [(1,)]
        from toolbox.db_adapter import table_exists

        self.assertTrue(table_exists("tabUser"))
        call_args = mock_frappe.db.sql.call_args
        self.assertIn("pg_tables", call_args[0][0])

    @patch("toolbox.db_adapter.frappe")
    def test_returns_false_when_empty(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        mock_frappe.db.sql.return_value = []
        from toolbox.db_adapter import table_exists

        self.assertFalse(table_exists("nonexistent"))


class TestGetExplainSql(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_explain_extended(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_explain_sql

        result = get_explain_sql("SELECT * FROM tab")
        self.assertEqual(result, "EXPLAIN EXTENDED SELECT * FROM tab")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_explain(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_explain_sql

        result = get_explain_sql("SELECT * FROM tab")
        self.assertEqual(result, "EXPLAIN SELECT * FROM tab")


class TestGetAnalyzeSql(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_analyze(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_analyze_sql

        result = get_analyze_sql("SELECT * FROM tab")
        self.assertEqual(result, "ANALYZE SELECT * FROM tab")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_explain_analyze_json(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_analyze_sql

        result = get_analyze_sql("SELECT * FROM tab")
        self.assertEqual(result, "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM tab")


class TestGetOptimizeSql(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_optimize_table(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_optimize_sql

        result = get_optimize_sql("tabUser")
        self.assertEqual(result, "OPTIMIZE TABLE `tabUser`")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_vacuum_analyze(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_optimize_sql

        result = get_optimize_sql("tabUser")
        self.assertEqual(result, 'VACUUM ANALYZE "tabUser"')


class TestGetAnalyzeTableSql(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_analyze_table(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_analyze_table_sql

        result = get_analyze_table_sql("tabUser")
        self.assertEqual(result, "ANALYZE TABLE `tabUser`")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_analyze(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_analyze_table_sql

        result = get_analyze_table_sql("tabUser")
        self.assertEqual(result, 'ANALYZE "tabUser"')


class TestGetCreateIndexDdl(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_backtick_syntax(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_create_index_ddl

        result = get_create_index_ddl("tabUser", "idx_name", ["name", "email"])
        self.assertEqual(result, "CREATE INDEX `idx_name` ON `tabUser` (`name`, `email`)")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_double_quote_syntax(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_create_index_ddl

        result = get_create_index_ddl("tabUser", "idx_name", ["name", "email"])
        self.assertEqual(result, 'CREATE INDEX "idx_name" ON "tabUser" ("name", "email")')


class TestGetDropIndexDdl(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_drop_with_on(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_drop_index_ddl

        result = get_drop_index_ddl("tabUser", "idx_name")
        self.assertEqual(result, "DROP INDEX `idx_name` ON `tabUser`")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_drop_without_on(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_drop_index_ddl

        result = get_drop_index_ddl("tabUser", "idx_name")
        self.assertEqual(result, 'DROP INDEX "idx_name"')


class TestGetDropIndexIfExistsDdl(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_drop_index_if_exists_ddl

        result = get_drop_index_if_exists_ddl("tabUser", "idx_name")
        self.assertEqual(result, "DROP INDEX IF EXISTS `idx_name` ON `tabUser`")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_drop_index_if_exists_ddl

        result = get_drop_index_if_exists_ddl("tabUser", "idx_name")
        self.assertEqual(result, 'DROP INDEX IF EXISTS "idx_name"')


class TestGetActiveConnections(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_show_status(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        mock_frappe.db.sql.return_value = [{"Variable_name": "Threads_connected", "Value": "5"}]
        from toolbox.db_adapter import get_active_connections

        result = get_active_connections()
        self.assertEqual(result[0]["Variable_name"], "Threads_connected")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_pg_stat_activity(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        mock_frappe.db.sql.return_value = [{"Variable_name": "Connections_active", "Value": 3}]
        from toolbox.db_adapter import get_active_connections

        result = get_active_connections()
        call_sql = mock_frappe.db.sql.call_args[0][0]
        self.assertIn("pg_stat_activity", call_sql)


class TestGetIndexQuery(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_uses_information_schema(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_index_query

        result = get_index_query()
        self.assertIn("INFORMATION_SCHEMA.STATISTICS", result)

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_uses_pg_indexes(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_index_query

        result = get_index_query()
        self.assertIn("pg_indexes", result)
        self.assertIn("pg_index", result)
        self.assertIn("pg_attribute", result)


class TestGetRenameColumnSql(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_change(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        from toolbox.db_adapter import get_rename_column_sql

        result = get_rename_column_sql("tabQuery", "occurence", "occurrence", "INT(11)")
        self.assertEqual(result, "ALTER TABLE `tabQuery` CHANGE `occurence` `occurrence` INT(11)")

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_rename_column(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        from toolbox.db_adapter import get_rename_column_sql

        result = get_rename_column_sql("tabQuery", "occurence", "occurrence", "INT(11)")
        self.assertEqual(
            result, 'ALTER TABLE "tabQuery" RENAME COLUMN "occurence" TO "occurrence"'
        )


class TestParsePgExplainAnalyze(unittest.TestCase):
    def test_parses_simple_plan(self):
        import json

        from toolbox.db_adapter import parse_pg_explain_analyze

        plan_json = json.dumps([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Actual Rows": 100,
                "Plan Rows": 100,
                "Filter": "(name = 'test')",
            }
        }])
        result = parse_pg_explain_analyze([{"QUERY PLAN": plan_json}])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["r_rows"], "100.00")
        self.assertEqual(result[0]["r_filtered"], 100.0)
        self.assertIn("Using where", result[0]["Extra"])

    def test_parses_nested_plan(self):
        import json

        from toolbox.db_adapter import parse_pg_explain_analyze

        plan_json = json.dumps([{
            "Plan": {
                "Node Type": "Sort",
                "Actual Rows": 50,
                "Plan Rows": 50,
                "Plans": [
                    {
                        "Node Type": "Index Scan",
                        "Index Name": "idx_test",
                        "Actual Rows": 50,
                        "Plan Rows": 50,
                    }
                ],
            }
        }])
        result = parse_pg_explain_analyze([{"QUERY PLAN": plan_json}])
        self.assertEqual(len(result), 2)
        self.assertIn("Using filesort", result[0]["Extra"])
        self.assertIn("Using index", result[1]["Extra"])

    def test_empty_result(self):
        from toolbox.db_adapter import parse_pg_explain_analyze

        result = parse_pg_explain_analyze([])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["r_filtered"], -1)

    def test_invalid_json(self):
        from toolbox.db_adapter import parse_pg_explain_analyze

        result = parse_pg_explain_analyze([{"QUERY PLAN": "not json"}])
        self.assertEqual(result[0]["r_filtered"], -1)


class TestGetPkExhaustionData(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_uses_information_schema(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        mock_frappe.db.sql.return_value = []
        from toolbox.db_adapter import get_pk_exhaustion_data

        get_pk_exhaustion_data()
        call_sql = mock_frappe.db.sql.call_args[0][0]
        self.assertIn("INFORMATION_SCHEMA", call_sql)
        self.assertIn("AUTO_INCREMENT", call_sql)

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_uses_pg_sequences(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        mock_frappe.db.sql.return_value = [
            {
                "sequencename": "tabuser_id_seq",
                "data_type": "integer",
                "last_value": 1000,
                "max_value": 2147483647,
            }
        ]
        from toolbox.db_adapter import get_pk_exhaustion_data

        result = get_pk_exhaustion_data()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "tabuser")
        self.assertEqual(result[0]["auto_increment"], 1000)
        self.assertEqual(result[0]["column_type"], "integer")


class TestGetMaxValueForTypePg(unittest.TestCase):
    def test_integer(self):
        from toolbox.db_adapter import get_max_value_for_type_pg

        self.assertEqual(get_max_value_for_type_pg("integer"), 2**31 - 1)

    def test_bigint(self):
        from toolbox.db_adapter import get_max_value_for_type_pg

        self.assertEqual(get_max_value_for_type_pg("bigint"), 2**63 - 1)

    def test_smallint(self):
        from toolbox.db_adapter import get_max_value_for_type_pg

        self.assertEqual(get_max_value_for_type_pg("smallint"), 2**15 - 1)

    def test_unknown(self):
        from toolbox.db_adapter import get_max_value_for_type_pg

        self.assertIsNone(get_max_value_for_type_pg("text"))


class TestGetUnusedIndexesData(unittest.TestCase):
    @patch("toolbox.db_adapter.frappe")
    def test_mariadb_uses_index_statistics(self, mock_frappe):
        mock_frappe.conf.db_type = "mariadb"
        mock_frappe.db.sql.return_value = []
        from toolbox.db_adapter import get_unused_indexes_data

        get_unused_indexes_data()
        call_sql = mock_frappe.db.sql.call_args[0][0]
        self.assertIn("INDEX_STATISTICS", call_sql)

    @patch("toolbox.db_adapter.frappe")
    def test_postgres_uses_pg_stat_user_indexes(self, mock_frappe):
        mock_frappe.conf.db_type = "postgres"
        mock_frappe.db.sql.return_value = []
        from toolbox.db_adapter import get_unused_indexes_data

        get_unused_indexes_data()
        call_sql = mock_frappe.db.sql.call_args[0][0]
        self.assertIn("pg_stat_user_indexes", call_sql)
        self.assertIn("idx_scan = 0", call_sql)


if __name__ == "__main__":
    unittest.main()
