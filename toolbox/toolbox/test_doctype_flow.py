# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt

import json
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, call, patch

from toolbox.doctype_flow import (
    TOOLBOX_FLOW_DATA,
    TOOLBOX_FLOW_SET,
    append_call_stack,
    dump,
    get_doctype_key,
    purge,
    render,
    start,
    status,
    stop,
    trace,
    untrace,
)


class TestDocTypeFlowKeys(unittest.TestCase):
    """Test key generation helpers."""

    def test_get_doctype_key_format(self):
        self.assertEqual(get_doctype_key("Sales Invoice"), f"{TOOLBOX_FLOW_DATA}:Sales Invoice")

    def test_get_doctype_key_special_chars(self):
        self.assertEqual(get_doctype_key("My DocType"), f"{TOOLBOX_FLOW_DATA}:My DocType")

    def test_constants(self):
        self.assertEqual(TOOLBOX_FLOW_SET, "toolbox-doctype_flow-doctypes")
        self.assertEqual(TOOLBOX_FLOW_DATA, "toolbox-doctype_flow-records")


class TestTraceUntraceStatus(unittest.TestCase):
    """Test trace/untrace/status Redis set operations."""

    @patch("toolbox.doctype_flow.frappe")
    def test_trace_adds_to_redis_set(self, mock_frappe):
        trace(["Sales Invoice", "Purchase Order"])
        mock_frappe.cache.sadd.assert_called_once_with(
            TOOLBOX_FLOW_SET, "Sales Invoice", "Purchase Order"
        )

    @patch("toolbox.doctype_flow.frappe")
    def test_untrace_removes_from_redis_set(self, mock_frappe):
        untrace(["Sales Invoice"])
        mock_frappe.cache.srem.assert_called_once_with(TOOLBOX_FLOW_SET, "Sales Invoice")

    @patch("toolbox.doctype_flow.frappe")
    def test_status_returns_set_members(self, mock_frappe):
        mock_frappe.cache.smembers.return_value = {"Sales Invoice", "Purchase Order"}
        result = status()
        mock_frappe.cache.smembers.assert_called_once_with(TOOLBOX_FLOW_SET)
        self.assertEqual(result, {"Sales Invoice", "Purchase Order"})


class TestPurge(unittest.TestCase):
    @patch("toolbox.doctype_flow.frappe")
    def test_purge_deletes_keys_for_each_doctype(self, mock_frappe):
        purge(["Sales Invoice", "Purchase Order"])
        mock_frappe.cache.delete_key.assert_has_calls(
            [
                call(get_doctype_key("Sales Invoice")),
                call(get_doctype_key("Purchase Order")),
            ]
        )


class TestAppendCallStack(unittest.TestCase):
    @patch("toolbox.doctype_flow.frappe")
    def test_creates_flow_dict_if_missing(self, mock_frappe):
        mock_frappe.local = MagicMock(spec=[])
        doc = MagicMock()
        doc.doctype = "Journal Entry"

        append_call_stack(doc, "Sales Invoice")

        self.assertIsInstance(mock_frappe.local.doctype_flow, defaultdict)
        self.assertEqual(mock_frappe.local.doctype_flow["Sales Invoice"], ["Journal Entry"])

    @patch("toolbox.doctype_flow.frappe")
    def test_appends_to_existing_flow(self, mock_frappe):
        flow = defaultdict(list)
        flow["Sales Invoice"] = ["Payment Entry"]
        mock_frappe.local.doctype_flow = flow

        doc = MagicMock()
        doc.doctype = "GL Entry"
        append_call_stack(doc, "Sales Invoice")

        self.assertEqual(flow["Sales Invoice"], ["Payment Entry", "GL Entry"])


class TestStartStop(unittest.TestCase):
    """Test the start/stop doc event hooks for flow tracing."""

    @patch("toolbox.doctype_flow.frappe")
    def test_start_skips_if_already_started(self, mock_frappe):
        doc = MagicMock()
        doc.flags.flow_started = True

        start(doc, "before_insert")

        mock_frappe.cache.sismember.assert_not_called()

    @patch("toolbox.doctype_flow.frappe")
    def test_start_begins_recording_for_traced_doctype(self, mock_frappe):
        doc = MagicMock()
        doc.doctype = "Sales Invoice"
        doc.flags.flow_started = False
        mock_frappe.local = MagicMock(spec=[])
        mock_frappe.cache.sismember.return_value = True

        start(doc, "before_insert")

        mock_frappe.cache.sismember.assert_called_once_with(TOOLBOX_FLOW_SET, "Sales Invoice")
        self.assertEqual(mock_frappe.local.in_flow_recording, "Sales Invoice")
        # flow_started is set to True on doc.flags
        self.assertTrue(doc.flags.flow_started)

    @patch("toolbox.doctype_flow.append_call_stack")
    @patch("toolbox.doctype_flow.frappe")
    def test_start_appends_when_already_recording(self, mock_frappe, mock_append):
        doc = MagicMock()
        doc.doctype = "GL Entry"
        doc.flags.flow_started = False
        mock_frappe.local.in_flow_recording = "Sales Invoice"

        start(doc, "before_validate")

        mock_append.assert_called_once_with(doc, key="Sales Invoice")

    @patch("toolbox.doctype_flow.frappe")
    def test_start_ignores_untraced_doctype(self, mock_frappe):
        doc = MagicMock()
        doc.doctype = "ToDo"
        doc.flags.flow_started = False
        mock_frappe.local = MagicMock(spec=[])
        mock_frappe.cache.sismember.return_value = False

        start(doc, "before_insert")

        # Should not set in_flow_recording
        self.assertFalse(hasattr(mock_frappe.local, "in_flow_recording") and
                         mock_frappe.local.in_flow_recording == "ToDo")

    @patch("toolbox.doctype_flow.frappe")
    def test_stop_clears_recording_for_root_doctype(self, mock_frappe):
        doc = MagicMock()
        doc.doctype = "Sales Invoice"
        mock_frappe.local.in_flow_recording = "Sales Invoice"

        stop(doc, "on_change")

        self.assertIsNone(mock_frappe.local.in_flow_recording)

    @patch("toolbox.doctype_flow.frappe")
    def test_stop_ignores_child_doctype(self, mock_frappe):
        doc = MagicMock()
        doc.doctype = "GL Entry"
        mock_frappe.local.in_flow_recording = "Sales Invoice"

        stop(doc, "on_change")

        # Should NOT clear recording since GL Entry != Sales Invoice
        self.assertEqual(mock_frappe.local.in_flow_recording, "Sales Invoice")


class TestDump(unittest.TestCase):
    """Test the dump function that persists flow data to Redis."""

    @patch("toolbox.doctype_flow.frappe")
    def test_dump_with_flow_maps(self, mock_frappe):
        flow_maps = {"Sales Invoice": ["Payment Entry", "GL Entry"]}
        mock_frappe.local.doctype_flow = flow_maps

        dump()

        mock_frappe.cache.sadd.assert_called_once_with(
            get_doctype_key("Sales Invoice"),
            json.dumps(["Payment Entry", "GL Entry"]),
        )

    @patch("toolbox.doctype_flow.frappe")
    def test_dump_empty_flow_with_recording(self, mock_frappe):
        """When flow_maps is empty but in_flow_recording is set, store empty array."""
        mock_frappe.local.doctype_flow = {}
        mock_frappe.local.in_flow_recording = "Sales Invoice"

        dump()

        mock_frappe.cache.sadd.assert_called_once_with(
            get_doctype_key("Sales Invoice"), "[]"
        )

    @patch("toolbox.doctype_flow.frappe")
    def test_dump_noop_when_nothing_recording(self, mock_frappe):
        mock_frappe.local = MagicMock(spec=[])

        dump()

        mock_frappe.cache.sadd.assert_not_called()

    @patch("toolbox.doctype_flow.frappe")
    def test_dump_multiple_doctypes(self, mock_frappe):
        flow_maps = {
            "Sales Invoice": ["GL Entry"],
            "Purchase Order": ["Purchase Receipt"],
        }
        mock_frappe.local.doctype_flow = flow_maps

        dump()

        self.assertEqual(mock_frappe.cache.sadd.call_count, 2)


class TestRender(unittest.TestCase):
    """Test the render function that prints flow chains."""

    @patch("builtins.print")
    @patch("toolbox.doctype_flow.frappe")
    def test_render_prints_chains(self, mock_frappe, mock_print):
        mock_frappe.cache.get_keys.return_value = [
            b"toolbox-doctype_flow-records:Sales Invoice"
        ]
        mock_frappe.cache.smembers.return_value = [
            json.dumps(["Payment Entry", "GL Entry"]).encode()
        ]

        render()

        mock_print.assert_called_with("Sales Invoice -> Payment Entry -> GL Entry")

    @patch("builtins.print")
    @patch("toolbox.doctype_flow.frappe")
    def test_render_prints_bare_doctype_for_empty_chain(self, mock_frappe, mock_print):
        mock_frappe.cache.get_keys.return_value = [
            b"toolbox-doctype_flow-records:Sales Invoice"
        ]
        mock_frappe.cache.smembers.return_value = [b"[]"]

        render()

        mock_print.assert_called_with("Sales Invoice")


if __name__ == "__main__":
    unittest.main()
