frappe.provide('frappe.dashboards.chart_sources');
frappe.dashboards.chart_sources["Active Database Connections"] = {
    method: "toolbox.toolbox.dashboard_chart_source.active_database_connections.active_database_connections.get",
};
