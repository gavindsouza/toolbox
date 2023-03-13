import json

import frappe


@frappe.whitelist(methods=["GET"])
def tables(limit: int = 20, offset: int = 0):
    TOOLBOX_TABLES = set(
        f"tab{x}" for x in frappe.get_all("DocType", {"module": "Toolbox"}, pluck="name")
    )
    filtered_data = []
    data = frappe.get_list(
        "MariaDB Table",
        fields=[
            "_table_name as name",
            "table_category",
            "table_category_meta",
        ],
        filters={"_table_exists": 1, "_table_name": ["not in", TOOLBOX_TABLES]},
    )

    for d in data:
        m = json.loads(d.pop("table_category_meta") or "{}")
        if m.get("total_queries"):
            filtered_data.append(
                {
                    "name": d["name"],
                    "table_category": d["table_category"],
                    "num_queries": m["total_queries"],
                    "num_write_queries": m.get("write_queries", 0),
                    "num_read_queries": m["total_queries"] - m["write_queries"],
                }
            )

    return sorted(filtered_data, key=lambda x: x["num_queries"], reverse=True)[
        offset : offset + limit
    ]
