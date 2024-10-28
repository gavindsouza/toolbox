import frappe

from toolbox.api.index_manager import tables


@frappe.whitelist()
def get():
    data = tables(limit=10)
    labels = [x["name"] for x in data]
    datasets = [
        {"name": "Read", "values": [x["num_read_queries"] for x in data]},
        {"name": "Write", "values": [x["num_write_queries"] for x in data]},
    ]

    return {
        "labels": labels,
        "datasets": datasets,
    }
