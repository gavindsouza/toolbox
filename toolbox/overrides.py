import toolbox


def boot_session(bootinfo):
    import frappe

    if "System Manager" in frappe.get_roles():
        bootinfo.toolbox = {
            "index_manager": {
                "enabled": toolbox.get_settings("is_index_manager_enabled"),
            }
        }


def after_migrate():
    import frappe

    frappe.get_single("ToolBox Settings").update_scheduled_jobs()
