import json
from collections import defaultdict

import frappe

TOOLBOX_FLOW_SET = "toolbox-doctype_flow-doctypes"
TOOLBOX_FLOW_DATA = "toolbox-doctype_flow-records"


def get_doctype_key(doctype: str) -> str:
    return f"{TOOLBOX_FLOW_DATA}:{doctype}"


def status():
    return frappe.cache().smembers(TOOLBOX_FLOW_SET)


def trace(doctypes: list[str]):
    frappe.cache().sadd(TOOLBOX_FLOW_SET, *doctypes)


def untrace(doctypes: list[str]):
    frappe.cache().srem(TOOLBOX_FLOW_SET, *doctypes)


def purge(doctypes: list[str]):
    for dt in doctypes:
        frappe.cache().delete_key(get_doctype_key(dt))


def dump():
    flow_maps = getattr(frappe.local, "doctype_flow", {})
    if not flow_maps:
        if doctype := getattr(frappe.local, "in_flow_recording", None):
            frappe.cache().sadd(get_doctype_key(doctype), "[]")
    else:
        for doctype, data in flow_maps.items():
            frappe.cache().sadd(get_doctype_key(doctype), json.dumps(data))


def append_call_stack(doc, key):
    if not hasattr(frappe.local, "doctype_flow"):
        frappe.local.doctype_flow = defaultdict(list)
    frappe.local.doctype_flow[key].append(doc.doctype)


def start(doc, event, **kwargs):
    if doc.flags.flow_started:
        return

    doctype = getattr(doc, "doctype", None) or kwargs.get("doctype")
    in_flow_recording = getattr(frappe.local, "in_flow_recording", None)

    if in_flow_recording:
        append_call_stack(doc, key=in_flow_recording)

    elif frappe.cache().sismember(TOOLBOX_FLOW_SET, doctype):
        frappe.local.in_flow_recording = doctype
        append_call_stack(doc, key=doctype)

    doc.flags.flow_started = True


def stop(doc, event, **kwargs):
    doctype = getattr(doc, "doctype", None) or kwargs.get("doctype")
    in_flow_recording = getattr(frappe.local, "in_flow_recording", None)

    if in_flow_recording == doctype:
        frappe.local.in_flow_recording = None


def render():
    for dt in (
        x.decode().rsplit(":", maxsplit=1)[-1]
        for x in frappe.cache().get_keys(get_doctype_key("*"))
    ):
        maps = [json.loads(x) for x in frappe.cache().smembers(get_doctype_key(dt))]
        for map in maps:
            if not map:
                print(dt)
            else:
                print(f"{dt} -> {' -> '.join(map)}")
