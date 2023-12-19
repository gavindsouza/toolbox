function apply_index_manager_dashboard_patch() {
    const on_index_manager_dashboard = frappe?.dashboard?.dashboard_name === 'Index Manager - ToolBox';
    const index_manager_enabled = frappe?.boot?.toolbox?.index_manager?.enabled;

    if (on_index_manager_dashboard) {
        const warning_set = $('#index-manager-warn').length > 0;
        const ToolboxSettings = "ToolBox Settings";

        const settingsButtons = $(`button:contains("${ToolboxSettings}")`).filter(function() {
            return $(this).text().trim() === ToolboxSettings;
        });
        if (!settingsButtons.length) {
            frappe.dashboard.page.add_button(ToolboxSettings, () => frappe.set_route("Form", ToolboxSettings));
        }

        if (!index_manager_enabled) {
            if (!warning_set) {
                $(
                    `<div
                        class="form-message yellow"
                        id="index-manager-warn"
                    >
                        Index Manager is disabled. Enable it from
                            <a style="color: var(--yellow-800);" class="underline" href="${frappe.utils.generate_route({ type: "DocType", name: ToolboxSettings })}">
                                ${ToolboxSettings}
                            </a>
                    </div> `
                ).prependTo($('.page-body'));
            }
        } else {
            $('#index-manager-warn').remove();
        }
    }
}

$(document).ready(setTimeout(apply_index_manager_dashboard_patch, 1000));
navigation.addEventListener('navigate', apply_index_manager_dashboard_patch);
