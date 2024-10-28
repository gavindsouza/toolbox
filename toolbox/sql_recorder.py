import inspect
from collections import Counter
from contextlib import suppress
from re import compile

import frappe

import toolbox

TRACEBACK_PATH_PATTERN = compile(".*/apps/")
TOOLBOX_RECORDER_FLAG = "toolbox-sql_recorder-enabled"
TOOLBOX_RECORDER_DATA = "toolbox-sql_recorder-records"


def sql(*args, **kwargs):
    # impl 1: store most context - gets slowerer to process & adds more overhead to each request
    # impl 2 note: using args[0] to capture only the parameterized query

    # NOTE: this is not a complete solution, as it does not capture the values of the parameters
    # TODO: evaluate a solution that captures the values so we can generate better Query.get_sample

    result = frappe.local.db_sql(*args, **kwargs)
    frappe.local.toolbox_recorder.register(args[0])
    return result


def _patch():
    frappe.local.db_sql = frappe.db.sql
    frappe.db.sql = sql


def _unpatch():
    frappe.db.sql = frappe.local.db_sql


def before_hook(*args, **kwargs):
    toolbox_recorder_enabled = frappe.cache.get_value(TOOLBOX_RECORDER_FLAG)

    if toolbox_recorder_enabled is None:
        toolbox_recorder_enabled = toolbox.get_settings("is_index_manager_enabled")
        frappe.cache.set_value(TOOLBOX_RECORDER_FLAG, toolbox_recorder_enabled)

    if toolbox_recorder_enabled:
        frappe.local.toolbox_recorder = SQLRecorder()
        _patch()


def after_hook(*args, **kwargs):
    if hasattr(frappe.local, "toolbox_recorder") and frappe.cache().get_value(
        TOOLBOX_RECORDER_FLAG
    ):
        frappe.local.toolbox_recorder.dump()
        _unpatch()


def get_current_stack_frames():
    BLACKLIST_FILENAME = {
        "frappe/frappe/app.py",
        "frappe/frappe/api.py",
        "frappe/frappe/handler.py",
    }
    with suppress(Exception):
        current = inspect.currentframe()
        frames = inspect.getouterframes(current, context=10)
        for frame, filename, lineno, function, context, index in list(reversed(frames))[:-2]:
            if "/apps/" in filename or "<serverscript>" in filename:
                scrubbed_filename = TRACEBACK_PATH_PATTERN.sub("", filename)
                if scrubbed_filename not in BLACKLIST_FILENAME:
                    yield {
                        "filename": scrubbed_filename,
                        "lineno": lineno,
                        "function": function,
                    }


class SQLRecorder:
    def __init__(self):
        self.queries = []

    def register(self, query: str):
        self.queries.append(query)

    def dump(self):
        if not self.queries:
            return

        c = frappe.cache
        key = c.make_key(TOOLBOX_RECORDER_DATA)
        pipe = c.pipeline(transaction=False)

        for query, occurence in Counter(self.queries).items():
            if not c.hsetnx(key, query, occurence):
                pipe.hincrby(key, query, occurence)

        pipe.execute()
        self.queries = []
