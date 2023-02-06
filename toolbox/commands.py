# bench --site business.localhost start-recording
# bench --site business.localhost execute frappe.recorder.export_data
# changed `save 30 100` in redis_cache from `save ""` to persist data

# TODO:
# 1. Build a more performant recorder that doesn't add so much overhead:
# defer explain & formatting to processing step. check warnings, extended
#  on explain too

import click
from frappe.commands import get_site, pass_context


@click.command("delete-recording")
@pass_context
def delete_recording(context):
    import frappe
    from frappe.recorder import delete

    with frappe.init_site(get_site(context)):
        frappe.connect()
        delete()


@click.command("process-sql-metadata")
@pass_context
def process_sql_metadata(context):
    from math import ceil

    import frappe
    from frappe.recorder import export_data
    from frappe.utils.synchronization import filelock

    from toolbox.utils import (
        check_dbms_compatibility,
        handle_redis_connection_error,
        process_sql_metadata_chunk,
        record_database_state,
    )

    CHUNK_SIZE = 2500
    SITE = get_site(context)

    with frappe.init_site(SITE), check_dbms_compatibility(
        frappe.conf
    ), handle_redis_connection_error(), filelock("process_sql_metadata", timeout=0.1):
        frappe.set_user("Administrator")
        # better drop data from redis before processing, & save the list in a file if anything happens
        queries: list[str] = [
            query["query"] for func_call in export_data() for query in func_call["calls"]
        ]

        NUM_JOBS = 1
        if len(queries) > CHUNK_SIZE:
            NUM_JOBS = ceil(len(queries) / CHUNK_SIZE)

        print(f"Processing {len(queries):,} queries in {NUM_JOBS} jobs")

        if NUM_JOBS > 1:
            from multiprocessing import Pool

            frappe.connect()
            record_database_state()

            with Pool(NUM_JOBS) as p:
                for i in range(NUM_JOBS):
                    p.apply_async(
                        process_sql_metadata_chunk,
                        args=(queries[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE],),
                        kwds={"site": SITE, "setup": False, "chunk_size": CHUNK_SIZE},
                    )
                p.close()
                p.join()
        else:
            process_sql_metadata_chunk(queries, SITE, setup=True, chunk_size=CHUNK_SIZE)

        print("Done processing queries")

        # the following line will delete the queries that have been logged after the processing started too
        delete_recording.callback()


@click.command("cleanup-sql-metadata")
@pass_context
def cleanup_sql_metadata(context):
    from collections import Counter

    import frappe

    with frappe.init_site(get_site(context)):
        frappe.connect()
        mdb_qry = frappe.qb.DocType("MariaDB Query")
        candidates = frappe.get_all("MariaDB Query", pluck="query")
        c = Counter()
        c.update(candidates)

        candidates = [q for q, count in c.most_common(10_000) if count > 1]

        for query in candidates:
            all_occurences = frappe.get_all("MariaDB Query", {"query": query}, pluck="name")
            pick = all_occurences[0]
            frappe.qb.from_(mdb_qry).where(mdb_qry.query == query).where(
                mdb_qry.name != pick
            ).delete().run()
            doc = frappe.get_doc("MariaDB Query", pick)
            doc.occurence = len(all_occurences)
            doc.save()
            frappe.db.commit()


commands = [process_sql_metadata, delete_recording, cleanup_sql_metadata]
