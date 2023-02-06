import time

import frappe
import frappe.recorder

# Currently the monkey patch is applied for all sites - this is not multi-site friendly
# TODO: Apply patch only if toolbox is installed on site


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


frappe_recorder_sql = frappe.recorder.sql
frappe.recorder.sql = lighter_sql
