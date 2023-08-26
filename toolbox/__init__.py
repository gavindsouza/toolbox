__version__ = "0.0.2-beta.0"

import toolbox.overrides  # noqa: F401


def get_settings(key):
    """Get a value ToolBox Settings"""
    import frappe

    if not hasattr(frappe.local, "toolbox_settings"):
        try:
            frappe.local.toolbox_settings = frappe.get_cached_doc("ToolBox Settings")
        except frappe.DoesNotExistError:  # possible during new install
            frappe.clear_last_message()
            return

    return frappe.local.toolbox_settings.get(key)
