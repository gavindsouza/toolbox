# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

from typing import TYPE_CHECKING

import frappe
from frappe.model.document import Document

from toolbox.utils import check_dbms_compatibility

if TYPE_CHECKING:
    from frappe.core.doctype.scheduled_job_type.scheduled_job_type import ScheduledJobType

PROCESS_SQL_JOB_TITLE = "Process SQL Recorder"
PROCESS_SQL_JOB_METHOD = "toolbox_settings.process_sql_recorder"


class ToolBoxSettings(Document):
    def validate(self):
        with check_dbms_compatibility(frappe.conf, raise_error=True):
            ...
        self.set_missing_settings()
        self.update_scheduled_job()

    def on_change(self):
        # clear bootinfo for all System Managers
        for user in frappe.get_all(
            "Has Role", filters={"role": "System Manager"}, pluck="parent", distinct=True
        ):
            frappe.cache.hdel("bootinfo", user)

    def set_missing_settings(self):
        if self.is_index_manager_enabled:
            self.is_sql_recorder_enabled = True
            frappe.msgprint(
                "Index Manager requires SQL Recorder to be enabled. Enabling SQL Recorder.",
                alert=True,
            )
        if not self.sql_recorder_processing_interval:
            self.sql_recorder_processing_interval = "Hourly"

    def update_scheduled_job(self):
        scheduled_job: "ScheduledJobType"
        try:
            scheduled_job = frappe.get_doc("Scheduled Job Type", PROCESS_SQL_JOB_METHOD)
        except frappe.DoesNotExistError:
            frappe.clear_last_message()
            scheduled_job = frappe.new_doc("Scheduled Job Type")
            scheduled_job.name = PROCESS_SQL_JOB_METHOD

        scheduled_job.stopped = not self.is_sql_recorder_enabled
        scheduled_job.method = (
            "toolbox.toolbox.doctype.toolbox_settings.toolbox_settings.process_sql_recorder"
        )
        scheduled_job.create_log = 1
        scheduled_job.frequency = self.sql_recorder_processing_interval + " Long"

        return scheduled_job.save()


def process_sql_recorder(chunk_size: int = 100_000):
    import frappe
    from frappe.utils.synchronization import filelock

    from toolbox.sql_recorder import TOOLBOX_RECORDER_DATA
    from toolbox.utils import _process_sql_metadata_chunk, record_database_state

    with filelock("process_sql_metadata", timeout=0.1):
        QRY_COUNT = frappe.cache.llen(TOOLBOX_RECORDER_DATA)
        print(f"Processing {QRY_COUNT:,} queries")
        _process_sql_metadata_chunk(chunk_size=chunk_size)
        frappe.enqueue(
            # this ought to find broken links & generate records for them too
            record_database_state,
            queue="long",
            job_id=record_database_state.__name__,
            deduplicate=True,
        )
        print("Done processing queries across all jobs")
