import inspect
import time
from contextlib import suppress
from re import compile

import frappe

TRACEBACK_PATH_PATTERN = compile(".*/apps/")
TOOLBOX_RECORDER_FLAG = "toolbox-sql_recorder-enabled"
TOOLBOX_RECORDER_DATA = "toolbox-sql_recorder-records"


def export_data() -> list[list[dict[str, str | float | list[dict]]]]:
    return list(frappe.cache().hgetall(TOOLBOX_RECORDER_DATA).values())


def delete_data():
    frappe.cache().delete_value(TOOLBOX_RECORDER_DATA)


def sql(*args, **kwargs):
    start_time = time.monotonic()
    result = frappe.local.db_sql(*args, **kwargs)
    end_time = time.monotonic()

    frappe.local.toolbox_recorder.register(
        {
            "query": frappe.db.last_query,
            "args": args,
            "kwargs": kwargs,
            "stack": list(get_current_stack_frames()),
            "time": start_time,
            "duration": float(f"{(end_time - start_time) * 1000:.3f}"),
        }
    )

    return result


def _patch():
    frappe.local.db_sql = frappe.db.sql
    frappe.db.sql = sql


def _unpatch():
    frappe.db.sql = frappe.local.db_sql


def before_hook(*args, **kwargs):
    if frappe.cache().get_value(TOOLBOX_RECORDER_FLAG):
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
            if "/apps/" in filename:
                scrubbed_filename = TRACEBACK_PATH_PATTERN.sub("", filename)
                if scrubbed_filename not in BLACKLIST_FILENAME:
                    yield {
                        "filename": scrubbed_filename,
                        "lineno": lineno,
                        "function": function,
                    }


class SQLRecorder:
    def __init__(self):
        self.uuid = frappe.generate_hash(length=10)
        self.queries = []

    def register(self, data):
        self.queries.append(data)

    def dump(self):
        if not self.queries:
            return
        self.queries, dump = [], self.queries[:]
        frappe.cache().hset(TOOLBOX_RECORDER_DATA, self.uuid, dump)
