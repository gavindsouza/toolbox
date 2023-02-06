import time

import frappe.recorder


def lighter_sql(*args, **kwargs):
    start_time = time.monotonic()
    result = frappe.db._sql(*args, **kwargs)
    end_time = time.monotonic()

    data = {
        "query": frappe.db.last_query,
        # "stack": stack,
        "time": start_time,
        "duration": float(f"{(end_time - start_time) * 1000:.3f}"),
    }

    frappe.local._recorder.register(data)
    return result


frappe.recorder.sql = lighter_sql
