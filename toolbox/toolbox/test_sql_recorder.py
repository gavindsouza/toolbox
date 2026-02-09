# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import unittest
from collections import Counter
from unittest.mock import MagicMock, call, patch

from toolbox.sql_recorder import (
    TOOLBOX_RECORDER_DATA,
    TOOLBOX_RECORDER_FLAG,
    SQLRecorder,
    _patch,
    _unpatch,
    after_hook,
    before_hook,
    get_current_stack_frames,
    sql,
)


class TestSQLRecorderClass(unittest.TestCase):
    """Unit tests for the SQLRecorder accumulator class."""

    def test_init_empty(self):
        recorder = SQLRecorder()
        self.assertEqual(recorder.queries, [])

    def test_register_single_query(self):
        recorder = SQLRecorder()
        recorder.register("SELECT 1")
        self.assertEqual(recorder.queries, ["SELECT 1"])

    def test_register_multiple_queries(self):
        recorder = SQLRecorder()
        recorder.register("SELECT 1")
        recorder.register("SELECT 2")
        recorder.register("SELECT 1")
        self.assertEqual(recorder.queries, ["SELECT 1", "SELECT 2", "SELECT 1"])

    def test_register_preserves_duplicates(self):
        """Queries are accumulated raw — deduplication happens in dump()."""
        recorder = SQLRecorder()
        for _ in range(5):
            recorder.register("SELECT 1")
        self.assertEqual(len(recorder.queries), 5)

    @patch("toolbox.sql_recorder.frappe")
    def test_dump_empty_queries_noop(self, mock_frappe):
        """dump() with no queries should not touch Redis at all."""
        recorder = SQLRecorder()
        recorder.dump()
        mock_frappe.cache.pipeline.assert_not_called()

    @patch("toolbox.sql_recorder.frappe")
    def test_dump_deduplicates_via_counter(self, mock_frappe):
        """dump() should aggregate identical queries using Counter before Redis ops."""
        mock_cache = MagicMock()
        mock_frappe.cache = mock_cache
        mock_pipe = MagicMock()
        mock_cache.pipeline.return_value = mock_pipe

        recorder = SQLRecorder()
        recorder.register("SELECT 1")
        recorder.register("SELECT 1")
        recorder.register("SELECT 2")

        # hsetnx returns True if key was set (new), False if existed
        mock_cache.hsetnx.return_value = True
        recorder.dump()

        # Counter should produce: {"SELECT 1": 2, "SELECT 2": 1}
        # hsetnx called for each unique query
        self.assertEqual(mock_cache.hsetnx.call_count, 2)
        mock_pipe.execute.assert_called_once()
        # queries list should be cleared after dump
        self.assertEqual(recorder.queries, [])

    @patch("toolbox.sql_recorder.frappe")
    def test_dump_hincrby_when_key_exists(self, mock_frappe):
        """When hsetnx returns False (key exists), hincrby should be pipelined."""
        mock_cache = MagicMock()
        mock_frappe.cache = mock_cache
        mock_pipe = MagicMock()
        mock_cache.pipeline.return_value = mock_pipe

        recorder = SQLRecorder()
        recorder.register("SELECT 1")

        # hsetnx returns False — key already existed
        mock_cache.hsetnx.return_value = False
        recorder.dump()

        key = mock_cache.make_key.return_value
        mock_pipe.hincrby.assert_called_once_with(key, "SELECT 1", 1)

    @patch("toolbox.sql_recorder.frappe")
    def test_dump_clears_queries(self, mock_frappe):
        mock_cache = MagicMock()
        mock_frappe.cache = mock_cache
        mock_cache.pipeline.return_value = MagicMock()
        mock_cache.hsetnx.return_value = True

        recorder = SQLRecorder()
        recorder.register("SELECT 1")
        recorder.dump()
        self.assertEqual(recorder.queries, [])


class TestMonkeyPatching(unittest.TestCase):
    """Tests for the _patch/_unpatch lifecycle of frappe.db.sql."""

    @patch("toolbox.sql_recorder.frappe")
    def test_patch_saves_original_and_replaces(self, mock_frappe):
        original_sql = MagicMock()
        mock_frappe.db.sql = original_sql

        _patch()

        # Original should be saved
        self.assertEqual(mock_frappe.local.db_sql, original_sql)
        # frappe.db.sql should now be our sql function
        self.assertEqual(mock_frappe.db.sql, sql)

    @patch("toolbox.sql_recorder.frappe")
    def test_unpatch_restores_original(self, mock_frappe):
        original_sql = MagicMock()
        mock_frappe.local.db_sql = original_sql

        _unpatch()

        self.assertEqual(mock_frappe.db.sql, original_sql)

    @patch("toolbox.sql_recorder.frappe")
    def test_sql_wrapper_calls_original_and_registers(self, mock_frappe):
        """The sql() wrapper should call the original db.sql and register the query."""
        original_sql = MagicMock(return_value=[{"name": "test"}])
        mock_frappe.local.db_sql = original_sql
        mock_recorder = MagicMock()
        mock_frappe.local.toolbox_recorder = mock_recorder

        result = sql("SELECT * FROM tabUser WHERE name = %s", ("Admin",))

        original_sql.assert_called_once_with("SELECT * FROM tabUser WHERE name = %s", ("Admin",))
        mock_recorder.register.assert_called_once_with("SELECT * FROM tabUser WHERE name = %s")
        self.assertEqual(result, [{"name": "test"}])


class TestBeforeAfterHook(unittest.TestCase):
    """Tests for the request/job hook lifecycle."""

    @patch("toolbox.sql_recorder.toolbox")
    @patch("toolbox.sql_recorder.frappe")
    def test_before_hook_enabled_creates_recorder_and_patches(self, mock_frappe, mock_toolbox):
        mock_frappe.cache.get_value.return_value = True
        mock_frappe.local = MagicMock()
        original_sql = MagicMock()
        mock_frappe.db.sql = original_sql

        before_hook()

        self.assertIsInstance(mock_frappe.local.toolbox_recorder, SQLRecorder)
        self.assertEqual(mock_frappe.db.sql, sql)

    @patch("toolbox.sql_recorder.toolbox")
    @patch("toolbox.sql_recorder.frappe")
    def test_before_hook_disabled_does_not_patch(self, mock_frappe, mock_toolbox):
        mock_frappe.cache.get_value.return_value = False
        original_sql = MagicMock()
        mock_frappe.db.sql = original_sql

        before_hook()

        self.assertEqual(mock_frappe.db.sql, original_sql)

    @patch("toolbox.sql_recorder.toolbox")
    @patch("toolbox.sql_recorder.frappe")
    def test_before_hook_caches_flag_when_none(self, mock_frappe, mock_toolbox):
        """When cache returns None, the setting should be fetched and cached."""
        mock_frappe.cache.get_value.return_value = None
        mock_toolbox.get_settings.return_value = False

        before_hook()

        mock_toolbox.get_settings.assert_called_once_with("is_index_manager_enabled")
        mock_frappe.cache.set_value.assert_called_once_with(TOOLBOX_RECORDER_FLAG, False)

    @patch("toolbox.sql_recorder.frappe")
    def test_after_hook_dumps_and_unpatches(self, mock_frappe):
        mock_recorder = MagicMock()
        mock_frappe.local.toolbox_recorder = mock_recorder
        mock_frappe.cache.get_value.return_value = True
        original_sql = MagicMock()
        mock_frappe.local.db_sql = original_sql

        after_hook()

        mock_recorder.dump.assert_called_once()
        self.assertEqual(mock_frappe.db.sql, original_sql)

    @patch("toolbox.sql_recorder.frappe")
    def test_after_hook_noop_when_no_recorder(self, mock_frappe):
        """after_hook should be a no-op when there's no recorder attribute."""
        mock_frappe.local = MagicMock(spec=[])  # no toolbox_recorder attribute
        mock_frappe.cache.get_value.return_value = True

        # Should not raise
        after_hook()

    @patch("toolbox.sql_recorder.frappe")
    def test_after_hook_noop_when_flag_disabled(self, mock_frappe):
        mock_recorder = MagicMock()
        mock_frappe.local.toolbox_recorder = mock_recorder
        mock_frappe.cache.get_value.return_value = False

        after_hook()

        mock_recorder.dump.assert_not_called()


class TestGetCurrentStackFrames(unittest.TestCase):
    """Tests for stack frame extraction used in call stack recording."""

    def test_returns_generator(self):
        result = get_current_stack_frames()
        import types

        self.assertIsInstance(result, types.GeneratorType)

    def test_filters_blacklisted_files(self):
        """Frames from frappe app/api/handler should be filtered out."""
        frames = list(get_current_stack_frames())
        blacklisted = {"frappe/frappe/app.py", "frappe/frappe/api.py", "frappe/frappe/handler.py"}
        for frame in frames:
            self.assertNotIn(frame["filename"], blacklisted)

    def test_frame_structure(self):
        """Each frame should have filename, lineno, function keys."""
        frames = list(get_current_stack_frames())
        # We're running from a test file in /apps/ so we should get at least this frame
        if frames:
            for frame in frames:
                self.assertIn("filename", frame)
                self.assertIn("lineno", frame)
                self.assertIn("function", frame)


if __name__ == "__main__":
    unittest.main()
