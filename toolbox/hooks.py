from . import __version__ as app_version  # noqa

app_name = "toolbox"
app_title = "Toolbox"
app_publisher = "Gavin D'souza"
app_description = "App to optimize & maintain your sites"
app_email = "gavin18d@gmail.com"
app_license = "AGPL-3.0"
app_logo_url = "/assets/toolbox/images/toolbox-logo.png"
app_home = "/app/toolbox"

before_request = ["toolbox.sql_recorder.before_hook"]
after_request = ["toolbox.sql_recorder.after_hook", "toolbox.doctype_flow.dump"]

before_job = ["toolbox.sql_recorder.before_hook"]
after_job = ["toolbox.sql_recorder.after_hook", "toolbox.doctype_flow.dump"]

after_migrate = ["toolbox.overrides.after_migrate"]

doc_events = {
    "*": {
        "before_insert": "toolbox.doctype_flow.start",
        "before_validate": "toolbox.doctype_flow.start",
        "on_change": "toolbox.doctype_flow.stop",
    }
}

app_include_js = "/assets/toolbox/toolbox.js"

boot_session = ["toolbox.overrides.boot_session"]
export_python_type_annotations = True
