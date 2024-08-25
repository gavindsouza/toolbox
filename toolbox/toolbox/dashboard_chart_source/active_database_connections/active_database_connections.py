import frappe


@frappe.whitelist()
def get():
    data = active_database_connections()
    return {
        "labels": ["Unrecorded"] + [x["Variable_name"] for x in data] + ["Unrecorded"],
        "datasets": [
            {"name": "Value", "values": [0] + [x["Value"] for x in data] + [0]},
        ],
    }


def active_database_connections():
    return frappe.db.sql("show status where `variable_name` = 'Threads_connected'", as_dict=True)
