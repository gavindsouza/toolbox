# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

from typing import TYPE_CHECKING

import frappe
from frappe.model.document import Document

from toolbox.sql_recorder import TOOLBOX_RECORDER_FLAG
from toolbox.utils import check_dbms_compatibility

if TYPE_CHECKING:
    from frappe.core.doctype.scheduled_job_type.scheduled_job_type import ScheduledJobType

SCHEDULED_JOBS = [
    {
        "id": "process_sql_recorder",
        "title": "Process SQL Recorder",
        # Note: this is how Frappe stores the method name for Scheduled Job Type - updated Aug 2024
        "method": f"{__name__}.process_sql_recorder",
        "frequency_property": "sql_recorder_processing_interval",
        "enabled_property": "is_sql_recorder_enabled",
    },
    {
        "id": "process_index_manager",
        "title": "Process Index Manager",
        "method": "toolbox.index_manager.process_index_manager",
        "frequency_property": "index_manager_processing_interval",
        "enabled_property": "is_index_manager_enabled",
    },
]


def toggle_sql_recorder(enabled: bool):
    frappe.cache.set_value(TOOLBOX_RECORDER_FLAG, enabled)


def clear_system_manager_cache():
    for user in frappe.get_all(
        "Has Role", filters={"role": "System Manager"}, pluck="parent", distinct=True
    ):
        frappe.cache.hdel("bootinfo", user)


class ToolBoxSettings(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        index_manager_processing_interval: DF.Literal["Hourly", "Daily"]
        is_index_manager_enabled: DF.Check
        is_sql_recorder_enabled: DF.Check
        sql_recorder_processing_interval: DF.Literal["Hourly", "Daily"]
    # end: auto-generated types

    def validate(self):
        with check_dbms_compatibility(frappe.conf, raise_error=True):
            ...
        self.set_missing_settings()
        self.update_scheduled_jobs()

    def on_change(self):
        frappe.db.after_commit.add(lambda: toggle_sql_recorder(self.is_sql_recorder_enabled))
        frappe.db.after_commit.add(clear_system_manager_cache)

    def set_missing_settings(self):
        if self.is_index_manager_enabled and not self.is_sql_recorder_enabled:
            self.is_sql_recorder_enabled = True
            frappe.msgprint(
                "Index Manager requires SQL Recorder to be enabled. Enabling SQL Recorder.",
                alert=True,
            )
        if not self.sql_recorder_processing_interval:
            self.sql_recorder_processing_interval = "Hourly"
        if not self.index_manager_processing_interval:
            self.index_manager_processing_interval = "Hourly"

    def update_scheduled_jobs(self):
        # Set up scheduled jobs for index manager & sql recorder
        scheduled_job: "ScheduledJobType"

        for job in SCHEDULED_JOBS:
            try:
                scheduled_job = frappe.get_doc("Scheduled Job Type", {"method": job["method"]})
            except frappe.DoesNotExistError:
                frappe.clear_last_message()
                scheduled_job = frappe.new_doc("Scheduled Job Type")
                scheduled_job.name = job["method"]

            scheduled_job.stopped = not getattr(self, job["enabled_property"], False)
            scheduled_job.method = job["method"]
            scheduled_job.create_log = 1

            # add job for generating indexes to a longer queue
            if job["id"] == "process_index_manager":
                scheduled_job.frequency = f"{self.index_manager_processing_interval} Long"
            # add job for processing sql recorder to a shorter queue 30 mins before index manager job
            elif job["id"] == "process_sql_recorder":
                scheduled_job.frequency = "Cron"
                if self.sql_recorder_processing_interval == "Hourly":
                    scheduled_job.cron_format = "30 * * * *"
                elif self.sql_recorder_processing_interval == "Daily":
                    scheduled_job.cron_format = "0 23 * * *"
            scheduled_job.save()


def process_sql_recorder():
    import frappe
    from frappe.utils.synchronization import filelock

    from toolbox.sql_recorder import TOOLBOX_RECORDER_DATA
    from toolbox.utils import process_sql_metadata_chunk, record_database_state

    with filelock("process_sql_metadata", timeout=0.1):
        c = frappe.cache
        DATA_KEY = c.make_key(TOOLBOX_RECORDER_DATA)
        QRY_COUNT = c.hlen(DATA_KEY)
        frappe.logger("toolbox").info(f"Processing {QRY_COUNT:,} queries")

        pipe = c.pipeline()
        pipe.execute_command("HGETALL", DATA_KEY)
        pipe.execute_command("DEL", DATA_KEY)
        queries: dict[str, int] = {
            k.decode(): int(v.decode()) for k, v in pipe.execute()[0].items()
        }

        process_sql_metadata_chunk(queries)
        frappe.enqueue(
            # this ought to find broken links & generate records for them too
            record_database_state,
            queue="long",
            job_id=record_database_state.__name__,
            deduplicate=True,
        )
        frappe.logger("toolbox").info("Done processing queries across all jobs")


