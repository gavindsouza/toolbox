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
        "method": f"{__name__}.process_index_manager",
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
        print(f"Processing {QRY_COUNT:,} queries")

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
        print("Done processing queries across all jobs")


def process_index_manager(
    table_name: str = None,
    sql_occurence: int = 0,
    skip_backtest: bool = False,
    verbose: bool = False,
):
    from collections import defaultdict
    from itertools import groupby

    import frappe

    from toolbox.doctypes import MariaDBIndex
    from toolbox.utils import Query, QueryBenchmark, Table, get_table_id

    # optimization algorithm v1:
    # 1. Check if the tables involved are scanning entire tables (type: ALL[Worst case] and similar)
    # 2. If so, check if there are any indexes that can be used - create a new query with the indexes
    # 3. compare # of rows scanned and filtered and execution time, before and after
    #
    # TODO: Check if there is any salvageable data in the query explain - like execution time,
    # possible_keys, key_len, filtered (for existing perf & test any improvement, along w query exec
    # time ofcs), etc
    #
    # TODO: Add logging to the process
    #
    # Note: don't push occurence filter in SQL without considering that we're storing captured queries
    # and not candidates. The Query objects here represent query candidates which are reduced considering
    # parameterized queries and occurences
    ok_types = ["ALL", "index", "range", "ref", "eq_ref", "fulltext", "ref_or_null"]
    table_grouper = lambda q: q.table  # noqa: E731
    sql_qualifier = (
        (lambda q: q.occurence > sql_occurence) if sql_occurence else None
    )  # noqa: E731
    filter_map = [
        ["MariaDB Query Explain", "type", "in", ok_types],
        ["MariaDB Query Explain", "parenttype", "=", "MariaDB Query"],
    ]
    if table_name:
        filter_map.append(["MariaDB Query Explain", "table", "=", get_table_id(table_name)])

    recorded_queries = frappe.get_all(
        "MariaDB Query",
        filters=filter_map,
        fields=["query", "parameterized_query", "query_explain.table", "occurence"],
        order_by=None,
        distinct=True,
    )
    recorded_queries = sorted(recorded_queries, key=table_grouper)  # required for groupby to work

    for table_id, _queries in groupby(recorded_queries, key=table_grouper):
        table = Table(id=table_id)

        if not table.name or not table.exists():
            # First condition is likely due to ghost data in MariaDB Query Explain  - this might be a bug, or require a cleanup
            # Second is for derived and temporary tables                            - this is expected
            if verbose:
                print(f"Skipping {table_id} - table not found")
            continue

        # combine occurences from parameterized query candidates
        _qrys = list(_queries)
        _query_candidates = defaultdict(lambda: defaultdict(int))

        for q in _qrys:
            reduced_key = q.parameterized_query or q.query
            _query_candidates[reduced_key]["sql"] = q.query
            _query_candidates[reduced_key]["occurence"] += q.occurence

        query_candidates = [Query(**q, table=table) for q in _query_candidates.values()]
        del _query_candidates

        # generate index candidates from the query candidates, qualify them
        index_candidates = table.find_index_candidates(query_candidates, qualifier=sql_qualifier)
        qualified_index_candidates = table.qualify_index_candidates(index_candidates)

        if not qualified_index_candidates:
            if verbose:
                print(f"No qualified index candidates for {table.name}")
            continue

        # Generate indexes from qualified index candidates, test gains
        if skip_backtest:
            failed_ics = MariaDBIndex.create(
                table.name, qualified_index_candidates, verbose=verbose
            )
            continue

        with QueryBenchmark(index_candidates=qualified_index_candidates, verbose=verbose) as qbm:
            failed_ics = MariaDBIndex.create(
                table.name, qualified_index_candidates, verbose=verbose
            )

        # Drop indexes that don't improve query metrics
        redundant_indexes = [
            qualified_index_candidates[q_id]
            for q_id, ctx in qbm.get_unchanged_results()
            if qualified_index_candidates[q_id] not in failed_ics
        ]
        MariaDBIndex.drop(table.name, redundant_indexes, verbose=verbose)

        total_indexes_created = len(qualified_index_candidates) - len(failed_ics)
        total_indexes_dropped = len(redundant_indexes)

        if verbose and (total_indexes_created != total_indexes_dropped):
            print(f"Optimized {table.name}")
            print(f"Indexes created: {total_indexes_created}")
            print(f"Indexes dropped: {total_indexes_dropped}")

    # TODO: Show summary of changes & record 'Index Record Summary'
