from . import __version__ as app_version  # noqa

app_name = "toolbox"
app_title = "Toolbox"
app_publisher = "Gavin D'souza"
app_description = "App to optimize & maintain your sites"
app_email = "gavin18d@gmail.com"
app_license = "No license"

before_request = ["toolbox.sql_recorder.before_request"]
after_request = ["toolbox.sql_recorder.after_request"]
