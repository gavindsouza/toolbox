# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import unittest

from click.testing import CliRunner

from toolbox.commands import (
    commands,
    doctype_manager_cli,
    drop_recording,
    index_manager_cli,
    optimize_indexes,
    sql_manager_cli,
    sql_recorder_cli,
    start_recording,
    stop_recording,
    trace_doctypes,
)


class TestCommandRegistration(unittest.TestCase):
    """Test that all CLI commands are properly registered."""

    def test_commands_list_has_four_groups(self):
        self.assertEqual(len(commands), 4)

    def test_sql_recorder_commands_registered(self):
        cmd_names = {c.name for c in sql_recorder_cli.commands.values()}
        self.assertIn("start", cmd_names)
        self.assertIn("stop", cmd_names)
        self.assertIn("drop", cmd_names)

    def test_index_manager_commands_registered(self):
        cmd_names = {c.name for c in index_manager_cli.commands.values()}
        self.assertIn("show-toolbox-indexes", cmd_names)
        self.assertIn("drop-toolbox-indexes", cmd_names)
        self.assertIn("optimize", cmd_names)

    def test_sql_manager_commands_registered(self):
        cmd_names = {c.name for c in sql_manager_cli.commands.values()}
        self.assertIn("process", cmd_names)
        self.assertIn("cleanup", cmd_names)

    def test_doctype_manager_commands_registered(self):
        cmd_names = {c.name for c in doctype_manager_cli.commands.values()}
        self.assertIn("trace", cmd_names)


class TestCommandGroupNames(unittest.TestCase):
    """Test that command groups have correct names."""

    def test_group_names(self):
        self.assertEqual(sql_recorder_cli.name, "sql-recorder")
        self.assertEqual(index_manager_cli.name, "index-manager")
        self.assertEqual(sql_manager_cli.name, "sql-manager")
        self.assertEqual(doctype_manager_cli.name, "doctype-manager")


class TestTraceCommand(unittest.TestCase):
    """Test the trace command argument parsing."""

    def test_trace_requires_status_arg(self):
        runner = CliRunner()
        result = runner.invoke(trace_doctypes, [])
        self.assertNotEqual(result.exit_code, 0)

    def test_trace_accepts_valid_choices(self):
        for choice in ["on", "off", "status", "purge", "draw"]:
            # Check that the command recognizes these as valid arguments
            # (won't actually run because frappe.init_site would fail)
            runner = CliRunner()
            result = runner.invoke(trace_doctypes, [choice], catch_exceptions=True)
            # If it fails, it should be because of site init, not argument parsing
            if result.exit_code != 0:
                self.assertNotIn("Invalid value", result.output or "")

    def test_trace_rejects_invalid_choice(self):
        runner = CliRunner()
        result = runner.invoke(trace_doctypes, ["invalid"])
        self.assertNotEqual(result.exit_code, 0)


class TestOptimizeCommandOptions(unittest.TestCase):
    """Test optimize command option definitions."""

    def test_has_expected_params(self):
        param_names = {p.name for p in optimize_indexes.params}
        self.assertIn("table_name", param_names)
        self.assertIn("sql_occurrence", param_names)
        self.assertIn("skip_backtest", param_names)
        self.assertIn("verbose", param_names)


if __name__ == "__main__":
    unittest.main()
