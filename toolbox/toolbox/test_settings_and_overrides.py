# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import unittest
from unittest.mock import MagicMock, patch

from toolbox.toolbox.doctype.toolbox_settings.toolbox_settings import (
    SCHEDULED_JOBS,
    clear_system_manager_cache,
    toggle_sql_recorder,
)


class TestScheduledJobsConfig(unittest.TestCase):
    """Test SCHEDULED_JOBS constant structure."""

    def test_has_two_jobs(self):
        self.assertEqual(len(SCHEDULED_JOBS), 2)

    def test_sql_recorder_job_config(self):
        job = next(j for j in SCHEDULED_JOBS if j["id"] == "process_sql_recorder")
        self.assertEqual(job["title"], "Process SQL Recorder")
        self.assertIn("process_sql_recorder", job["method"])
        self.assertEqual(job["frequency_property"], "sql_recorder_processing_interval")
        self.assertEqual(job["enabled_property"], "is_sql_recorder_enabled")

    def test_index_manager_job_config(self):
        job = next(j for j in SCHEDULED_JOBS if j["id"] == "process_index_manager")
        self.assertEqual(job["title"], "Process Index Manager")
        self.assertIn("process_index_manager", job["method"])
        self.assertEqual(job["frequency_property"], "index_manager_processing_interval")
        self.assertEqual(job["enabled_property"], "is_index_manager_enabled")

    def test_job_ids_unique(self):
        ids = [j["id"] for j in SCHEDULED_JOBS]
        self.assertEqual(len(ids), len(set(ids)))

    def test_job_methods_are_dotted_paths(self):
        for job in SCHEDULED_JOBS:
            self.assertIn(".", job["method"])


class TestToggleSqlRecorder(unittest.TestCase):
    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_toggle_enabled(self, mock_frappe):
        toggle_sql_recorder(True)
        mock_frappe.cache.set_value.assert_called_once()
        args = mock_frappe.cache.set_value.call_args
        self.assertTrue(args[0][1])

    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_toggle_disabled(self, mock_frappe):
        toggle_sql_recorder(False)
        mock_frappe.cache.set_value.assert_called_once()
        args = mock_frappe.cache.set_value.call_args
        self.assertFalse(args[0][1])


class TestClearSystemManagerCache(unittest.TestCase):
    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_clears_bootinfo_for_each_system_manager(self, mock_frappe):
        mock_frappe.get_all.return_value = ["admin@example.com", "manager@example.com"]

        clear_system_manager_cache()

        mock_frappe.get_all.assert_called_once_with(
            "Has Role", filters={"role": "System Manager"}, pluck="parent", distinct=True
        )
        self.assertEqual(mock_frappe.cache.hdel.call_count, 2)
        mock_frappe.cache.hdel.assert_any_call("bootinfo", "admin@example.com")
        mock_frappe.cache.hdel.assert_any_call("bootinfo", "manager@example.com")

    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_no_system_managers_no_cache_clear(self, mock_frappe):
        mock_frappe.get_all.return_value = []
        clear_system_manager_cache()
        mock_frappe.cache.hdel.assert_not_called()


class TestSetMissingSettings(unittest.TestCase):
    """Test ToolBoxSettings.set_missing_settings logic."""

    def _make_settings(self, **kwargs):
        settings = MagicMock()
        settings.is_sql_recorder_enabled = kwargs.get("is_sql_recorder_enabled", 0)
        settings.is_index_manager_enabled = kwargs.get("is_index_manager_enabled", 0)
        settings.sql_recorder_processing_interval = kwargs.get("sql_recorder_processing_interval", "")
        settings.index_manager_processing_interval = kwargs.get("index_manager_processing_interval", "")
        return settings

    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_index_manager_enables_sql_recorder(self, mock_frappe):
        """If index manager is enabled but sql recorder is not, sql recorder should be auto-enabled."""
        settings = self._make_settings(is_index_manager_enabled=1, is_sql_recorder_enabled=0)

        from toolbox.toolbox.doctype.toolbox_settings.toolbox_settings import ToolBoxSettings
        ToolBoxSettings.set_missing_settings(settings)

        self.assertTrue(settings.is_sql_recorder_enabled)
        mock_frappe.msgprint.assert_called_once()

    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_missing_intervals_default_to_hourly(self, mock_frappe):
        settings = self._make_settings()

        from toolbox.toolbox.doctype.toolbox_settings.toolbox_settings import ToolBoxSettings
        ToolBoxSettings.set_missing_settings(settings)

        self.assertEqual(settings.sql_recorder_processing_interval, "Hourly")
        self.assertEqual(settings.index_manager_processing_interval, "Hourly")

    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_existing_intervals_not_overwritten(self, mock_frappe):
        settings = self._make_settings(
            sql_recorder_processing_interval="Daily",
            index_manager_processing_interval="Daily",
        )

        from toolbox.toolbox.doctype.toolbox_settings.toolbox_settings import ToolBoxSettings
        ToolBoxSettings.set_missing_settings(settings)

        self.assertEqual(settings.sql_recorder_processing_interval, "Daily")
        self.assertEqual(settings.index_manager_processing_interval, "Daily")

    @patch("toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.frappe")
    def test_both_disabled_still_sets_defaults(self, mock_frappe):
        settings = self._make_settings()

        from toolbox.toolbox.doctype.toolbox_settings.toolbox_settings import ToolBoxSettings
        ToolBoxSettings.set_missing_settings(settings)

        # Should NOT auto-enable sql recorder when index manager is off
        self.assertEqual(settings.is_sql_recorder_enabled, 0)
        mock_frappe.msgprint.assert_not_called()


class TestBootSession(unittest.TestCase):
    """Test boot_session override."""

    @patch("toolbox.overrides.toolbox")
    def test_adds_toolbox_key_for_system_manager(self, mock_toolbox):
        import frappe

        mock_toolbox.get_settings.return_value = True

        from toolbox.overrides import boot_session

        with patch.object(frappe, "get_roles", return_value=["System Manager", "Administrator"]):
            bootinfo = frappe._dict()
            boot_session(bootinfo)

        self.assertIn("toolbox", bootinfo)
        self.assertTrue(bootinfo["toolbox"]["index_manager"]["enabled"])

    def test_skips_for_non_system_manager(self):
        import frappe

        from toolbox.overrides import boot_session

        with patch.object(frappe, "get_roles", return_value=["Guest"]):
            bootinfo = {}
            boot_session(bootinfo)

        self.assertNotIn("toolbox", bootinfo)


class TestUtilHelpers(unittest.TestCase):
    """Test small utility functions in toolbox.utils."""

    def test_wrap_converts_numeric_string(self):
        from toolbox.utils import wrap

        self.assertEqual(wrap("3.14"), 3.14)
        self.assertEqual(wrap("42"), 42.0)

    def test_wrap_returns_non_numeric_unchanged(self):
        from toolbox.utils import wrap

        self.assertEqual(wrap("hello"), "hello")
        self.assertEqual(wrap(None), None)

    def test_wrap_with_int_string(self):
        from toolbox.utils import wrap

        self.assertEqual(wrap("0"), 0.0)

    @patch("toolbox.utils.secho")
    def test_check_dbms_compatibility_warns_unknown_db(self, mock_secho):
        from toolbox.utils import check_dbms_compatibility

        conf = MagicMock()
        conf.db_type = "sqlite"

        with check_dbms_compatibility(conf):
            pass

        mock_secho.assert_called_once()
        self.assertIn("sqlite", mock_secho.call_args[0][0])

    @patch("toolbox.utils.secho")
    def test_check_dbms_compatibility_raises_for_unknown_db(self, mock_secho):
        from toolbox.utils import check_dbms_compatibility

        conf = MagicMock()
        conf.db_type = "sqlite"

        with self.assertRaises(NotImplementedError):
            with check_dbms_compatibility(conf, raise_error=True):
                pass

    @patch("toolbox.utils.secho")
    def test_check_dbms_compatibility_passes_for_mariadb(self, mock_secho):
        from toolbox.utils import check_dbms_compatibility

        conf = MagicMock()
        conf.db_type = "mariadb"

        with check_dbms_compatibility(conf):
            pass

        mock_secho.assert_not_called()

    @patch("toolbox.utils.secho")
    def test_check_dbms_compatibility_passes_for_postgres(self, mock_secho):
        from toolbox.utils import check_dbms_compatibility

        conf = MagicMock()
        conf.db_type = "postgres"

        with check_dbms_compatibility(conf):
            pass

        mock_secho.assert_not_called()

    def test_handle_redis_connection_error_catches(self):
        from redis.exceptions import ConnectionError as RedisConnectionError

        from toolbox.utils import handle_redis_connection_error

        with patch("toolbox.utils.secho"):
            with handle_redis_connection_error():
                raise RedisConnectionError("Connection refused")

    def test_handle_redis_connection_error_passes_through_other(self):
        from toolbox.utils import handle_redis_connection_error

        with self.assertRaises(ValueError):
            with handle_redis_connection_error():
                raise ValueError("not a redis error")

    def test_explainable_queries_constant(self):
        from toolbox.utils import EXPLAINABLE_QUERIES

        self.assertIn("select", EXPLAINABLE_QUERIES)
        self.assertIn("insert", EXPLAINABLE_QUERIES)
        self.assertIn("update", EXPLAINABLE_QUERIES)
        self.assertIn("delete", EXPLAINABLE_QUERIES)
        self.assertEqual(len(EXPLAINABLE_QUERIES), 4)

    def test_params_pattern_matches_frappe_style(self):
        from toolbox.utils import PARAMS_PATTERN

        matches = PARAMS_PATTERN.findall("SELECT * FROM tab WHERE name = %(name)s AND age > %(min_age)s")
        self.assertEqual(len(matches), 2)
        self.assertIn("%(name)s", matches)
        self.assertIn("%(min_age)s", matches)

    def test_params_pattern_no_match_on_positional(self):
        from toolbox.utils import PARAMS_PATTERN

        matches = PARAMS_PATTERN.findall("SELECT * FROM tab WHERE name = %s")
        self.assertEqual(len(matches), 0)


class TestAPITablesTransformation(unittest.TestCase):
    """Test the tables() API endpoint data transformation logic."""

    @patch("toolbox.api.index_manager.frappe")
    def test_filters_tables_without_queries(self, mock_frappe):
        import json

        from toolbox.api.index_manager import tables

        mock_frappe.get_all.return_value = ["tabDocType"]
        mock_frappe.get_list.return_value = [
            {
                "name": "tabUser",
                "table_category": "Read",
                "table_category_meta": json.dumps({"total_queries": 100, "write_queries": 10}),
            },
            {
                "name": "tabEmpty",
                "table_category": "Read",
                "table_category_meta": json.dumps({}),
            },
            {
                "name": "tabNull",
                "table_category": "Read",
                "table_category_meta": None,
            },
        ]

        result = tables(limit=20, offset=0)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "tabUser")
        self.assertEqual(result[0]["num_queries"], 100)
        self.assertEqual(result[0]["num_write_queries"], 10)
        self.assertEqual(result[0]["num_read_queries"], 90)

    @patch("toolbox.api.index_manager.frappe")
    def test_sorts_by_num_queries_descending(self, mock_frappe):
        import json

        from toolbox.api.index_manager import tables

        mock_frappe.get_all.return_value = []
        mock_frappe.get_list.return_value = [
            {
                "name": "tabLow",
                "table_category": "Read",
                "table_category_meta": json.dumps({"total_queries": 10, "write_queries": 1}),
            },
            {
                "name": "tabHigh",
                "table_category": "Write",
                "table_category_meta": json.dumps({"total_queries": 500, "write_queries": 400}),
            },
        ]

        result = tables(limit=20, offset=0)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "tabHigh")
        self.assertEqual(result[1]["name"], "tabLow")

    @patch("toolbox.api.index_manager.frappe")
    def test_pagination_via_offset_and_limit(self, mock_frappe):
        import json

        from toolbox.api.index_manager import tables

        mock_frappe.get_all.return_value = []
        mock_frappe.get_list.return_value = [
            {
                "name": f"tab{i}",
                "table_category": "Read",
                "table_category_meta": json.dumps({"total_queries": 100 - i, "write_queries": 0}),
            }
            for i in range(10)
        ]

        result = tables(limit=3, offset=2)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["name"], "tab2")

    @patch("toolbox.api.index_manager.frappe")
    def test_read_write_calculation(self, mock_frappe):
        import json

        from toolbox.api.index_manager import tables

        mock_frappe.get_all.return_value = []
        mock_frappe.get_list.return_value = [
            {
                "name": "tabUser",
                "table_category": "Write",
                "table_category_meta": json.dumps({"total_queries": 200, "write_queries": 150}),
            },
        ]

        result = tables(limit=20, offset=0)
        self.assertEqual(result[0]["num_read_queries"], 50)
        self.assertEqual(result[0]["num_write_queries"], 150)
        self.assertEqual(result[0]["table_category"], "Write")


class TestIndexCandidateType(unittest.TestCase):
    """Test the IndexCandidateType enum."""

    def test_enum_values(self):
        from toolbox.utils import IndexCandidateType

        self.assertIsNotNone(IndexCandidateType.SELECT)
        self.assertIsNotNone(IndexCandidateType.WHERE)
        self.assertIsNotNone(IndexCandidateType.ORDER_BY)

    def test_all_types_distinct(self):
        from toolbox.utils import IndexCandidateType

        types = [IndexCandidateType.SELECT, IndexCandidateType.WHERE, IndexCandidateType.ORDER_BY]
        self.assertEqual(len(types), len(set(types)))


if __name__ == "__main__":
    unittest.main()
