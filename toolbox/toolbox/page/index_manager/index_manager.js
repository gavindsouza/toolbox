const ToolboxSettings = "ToolBox Settings";

$(document).on("page-change", function (event) {
    if (event.page?.page_name === "index-manager") {
        frappe.pages['index-manager'].refresh(event.page);
    }
});

frappe.pages['index-manager'].on_page_load = function (wrapper) {
    const page = frappe.ui.make_app_page({
        parent: wrapper,
        title: 'Index Manager - ToolBox',
        single_column: true
    });
    page.add_button("Settings", () => frappe.set_route("Form", ToolboxSettings));
}

frappe.pages['index-manager'].refresh = async function (wrapper) {
    const page = wrapper.page;

    if (!frappe.boot.toolbox.index_manager.enabled) {
        page.main.html(`<div class="text-center" style="margin-top: 100px;">
            <h1>Index Manager</h1>
            <p>Index Manager is disabled</p>
            <p>Enable it from <a href="${frappe.utils.generate_route({ type: "DocType", name: ToolboxSettings })}">${ToolboxSettings}</a></p>
        </div>`);
        return;
    } else {
        page.main.html("");
    }

    getTables().then(tables => {
        let chart_card = makeCard({width: '100%' }).appendTo(".page-content").get(0);
        makeActiveTablesChart(chart_card, tables);
        makeActiveTablesCard(tables.slice(0, 5)).appendTo(".page-content");
    });

    document.querySelector("#page-index-manager > div.container.page-body > div.page-wrapper > div > div.row.layout-main").remove();
}

function makeActiveTablesChart(wrapper, tables) {
    return new frappe.Chart(wrapper, {
        data: {
            'labels': tables.map(r => r.name),
            'datasets': [
                {'name': 'Read', 'values': tables.map(r => r.num_read_queries)},
                {'name': 'Write', 'values': tables.map(r => r.num_write_queries)},
            ]
        },
        type: 'bar',
        colors: ['#fc4f51', '#78d6ff', '#7575ff'],
        barOptions: {'stacked': true}
    });
}

function makeActiveTablesCard(tables) {
    return makeCard({
        title: "Most Active Tables",
        subtitle: "Tables which are most frequently queried",
        html: `<table class="table table-sm">
        <table class="table table-sm">
            <thead><tr><th scope="col">Table</th><th scope="col">Count</th></tr></thead>
            <tbody>${
                tables.map(r => `<tr><td>${r.name}</td> <td>${r.num_queries}</td> </tr>`).join("")
            }</tbody>
        </table>
        </div>
    </div>`
    });
}

async function getTables(limit = 10) {
    const { message } = await frappe.call({method: "toolbox.api.index_manager.tables", type: "GET", args: { limit }});
    return message;
}

function makeCard(opts) {
    return $(`<div class="card m-2" style="width: ${opts.width || '18rem'};">
        <div class="card-body">
            <h5 class="card-title">${opts.title}</h5>
            <h6 class="card-subtitle mb-2 text-muted">${opts.subtitle}</h6>
            ${opts.html}
        </div>
        </div>`
    );
}