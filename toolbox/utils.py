import json
import re
from contextlib import contextmanager, suppress
from enum import Enum, auto
from functools import lru_cache
from html import escape
from itertools import groupby
from typing import TYPE_CHECKING, Callable

import frappe
from click import secho
from frappe.model.document import bulk_insert, now
from frappe.utils.caching import request_cache
from redis.exceptions import ConnectionError
from sql_metadata import Parser, QueryType
from sqlparse import format as format_sql
from sqlparse import parse
from sqlparse.sql import Comparison, Identifier, IdentifierList, Where
from sqlparse.tokens import Keyword

if TYPE_CHECKING:
    from sqlparse.sql import Statement

    from toolbox.doctypes import MariaDBQuery

PARAMS_PATTERN = re.compile(r"\%\([\w]*\)s")


def wrap(value):
    with suppress(Exception):
        return float(value)
    return value


def record_table(table: str) -> str:
    table = table or "NULL"

    if table_id := frappe.get_all("MariaDB Table", {"_table_name": table}, limit=1, pluck="name"):
        table_id = table_id[0]
    # handle derived tables & such
    elif table_id := frappe.get_all(
        "MariaDB Table",
        {"_table_name": escape(table)},
        limit=1,
        pluck="name",
    ):
        table_id = table_id[0]
    # generate temporary table names
    else:
        table_record = frappe.new_doc("MariaDB Table")
        table_record._table_name = table
        table_record.insert()
        table_id = table_record.name

    return table_id


@request_cache
def already_recorded(query: str):
    return frappe.get_all("MariaDB Query", {"query": query}, limit=1)


def record_query(
    query: str, p_query: str | None = None, call_stack: list[dict] | None = None
) -> "MariaDBQuery":
    if query_name := already_recorded(query):
        query_record = frappe.get_doc("MariaDB Query", query_name[0])
        query_record.parameterized_query = p_query

        # Note: Currently not being recorded
        if call_stack:
            query_record.call_stack = frappe.as_json(call_stack)

        return query_record

    query_record = frappe.new_doc("MariaDB Query")
    query_record.query = query
    query_record.parameterized_query = p_query
    query_record.occurence = 0
    query_record.call_stack = frappe.as_json(call_stack)

    return query_record


def record_database_state(init: bool = False):
    TABLE_DT = "MariaDB Table"

    if not init:
        _all_queries = frappe.get_all(
            "MariaDB Query",
            fields=["parameterized_query", "`tabMariaDB Query Explain`.table"],
            order_by="`table`",
        )
        for table_id, _queries in groupby(_all_queries, lambda x: x["table"]):
            queries: list[str] = [q["parameterized_query"] for q in _queries]
            data = {
                "total_queries": len(queries),
                "write_queries": len(
                    [
                        q
                        for q in queries
                        if q.lstrip()[:7]
                        .upper()
                        .startswith(
                            (
                                QueryType.INSERT,
                                QueryType.UPDATE,
                                QueryType.DELETE,
                            )
                        )
                    ]
                ),
            }

            frappe.db.set_value(
                TABLE_DT,
                table_id,
                "table_category_meta",
                json.dumps(data),
                update_modified=False,
            )

    else:
        records = [
            frappe.new_doc(TABLE_DT, _table_name=tbl, _table_exists=True)
            for tbl in frappe.db.get_tables(cached=False)
        ]

        if records:
            bulk_insert(
                doctype=TABLE_DT,
                documents=records,
                ignore_duplicates=True,
            )

    frappe.db.commit()


@contextmanager
def check_dbms_compatibility(conf, raise_error: bool = False):
    if conf.db_type != "mariadb":
        secho(f"WARN: This command might not be compatible with {conf.db_type}", fg="yellow")
        if raise_error:
            raise NotImplementedError(f"Command not compatible with {conf.db_type}")
    yield


@contextmanager
def handle_redis_connection_error():
    try:
        yield
    except ConnectionError as e:
        secho(f"ERROR: {e}", fg="red")
        secho("NOTE: Make sure Redis services are running", fg="yellow")


def process_sql_metadata_chunk(
    queries: dict[str, int],
):
    EXPLAINABLE_QUERIES = ("select", "insert", "update", "delete")
    MQ_TABLE = frappe.qb.DocType("MariaDB Query")
    RECORDED_QUERIES = {}
    USE_FALLBACK_PROPERTY = object()

    for p_query, p_occurence in queries.items():
        if isinstance(p_query, bytes):
            p_query = p_query.decode("utf-8")
        # this is a parameterized_query
        if not p_query.lstrip()[:7].lower().startswith(EXPLAINABLE_QUERIES):
            continue

        # increment occurence count
        frappe.qb.update(MQ_TABLE).set(MQ_TABLE.occurence, MQ_TABLE.occurence + p_occurence).set(
            MQ_TABLE.modified, now()
        ).where(MQ_TABLE.parameterized_query == p_query).limit(1).run()

        # check if query is already recorded (first try cursor's rowcount property)
        if (
            rowcount := getattr(frappe.db._cursor, "rowcount", USE_FALLBACK_PROPERTY)
        ) is not USE_FALLBACK_PROPERTY:
            if rowcount > 0:
                continue
        # fallback to raw sql if cursor property is not available
        elif frappe.db.sql("SELECT ROW_COUNT()", pluck=True)[0] > 0:
            continue

        query = Query(p_query).get_sample()

        # should check warnings too? unsure at this point
        try:
            explain_data = frappe.db.sql(f"EXPLAIN EXTENDED {query}", as_dict=True)
        except Exception as e:
            frappe.logger("toolbox").exception(
                f"process_sql_metadata_chunk: EXPLAIN EXTENDED {query}"
            )
            continue

        if not explain_data:
            print(f"Cannot explain query: {query}")
            continue

        # Note: Desk doesn't like Queries with whitespaces in long text for show title in links for forms
        # Better to strip them off and format on demand
        query_record = record_query(
            format_sql(query, strip_whitespace=True, keyword_case="upper"),
            p_query=p_query,
        )
        query_record.occurence += p_occurence
        for explain in explain_data:
            query_record.apply_explain(explain)
        query_record.set_new_name()
        query_record.set_parent_in_children()

        for df in query_record.meta.get_table_fields():
            RECORDED_QUERIES.setdefault(df.options, []).extend(query_record.get(df.fieldname))
        RECORDED_QUERIES.setdefault(query_record.doctype, []).append(query_record)

    for dt, records in RECORDED_QUERIES.items():
        bulk_insert(
            doctype=dt,
            documents=records,
            ignore_duplicates=True,
        )
        print(f"Recorded {len(records):,} new '{dt}' records")

    return frappe.new_doc(
        doctype="SQL Record Summary",
        total_sql_count=sum(queries.values()),
        unique_sql_count=len(queries),
    ).db_insert()


@lru_cache(maxsize=None)
def get_table_name(table_id: str) -> str | None:
    # Note: Use this util only via CLI / single threaded
    return frappe.db.get_value("MariaDB Table", table_id, "_table_name")


@lru_cache(maxsize=None)
def get_table_id(table_name: str):
    # Note: Use this util only via CLI / single threaded
    return frappe.db.get_value("MariaDB Table", {"_table_name": table_name}, "name")


class Query:
    def __init__(self, sql: str, occurence: int = 1, table: "Table" = None) -> None:
        self.sql = sql.strip()
        self.occurence = occurence
        self.table = table

    def __repr__(self) -> str:
        sub = f", table={self.table}" if self.table else ""
        dotted = "..." if len(self.sql) > 11 else ""
        return f"Query({self.sql[:10]}{dotted}{sub})"

    # Note: We're essentially parsing the same query multiple times
    # TODO: Avoid this, pass the parsed query to sql-metadata instead (or similar)
    @property
    def parsed(self) -> "Statement":
        if not hasattr(self, "_parsed"):
            self._parsed = parse(self.sql)[0]
        return self._parsed

    @property
    def d_parsed(self):
        if not hasattr(self, "_d_parsed"):
            self._d_parsed = Parser(self.sql)
        return self._d_parsed

    def get_sample(self) -> str:
        ret = self.sql

        if "%s" in self.sql:
            ret = ret.replace("%s", "1")

        else:
            for k, v in ((p, "1") for p in PARAMS_PATTERN.findall(self.sql)):
                ret = ret.replace(k, v)

        return format_sql(ret, strip_whitespace=True, keyword_case="upper")


class IndexCandidateType(Enum):
    SELECT: str = auto()
    WHERE: str = auto()
    ORDER_BY: str = auto()


class IndexCandidate(list):
    def __init__(
        self, query: Query, type: IndexCandidateType | None = None, ctx: list | None = None
    ) -> None:
        self.query = query
        self.type = type or IndexCandidateType.WHERE
        self.ctx = ctx

    def __repr__(self) -> str:
        return f"IndexCandidate({self.query.table or 'unspecified'}, {super().__repr__()})"

    def append(self, __object: str) -> None:
        if __object in self:
            return
        return super().append(__object)


class Table:
    def __init__(self, id: str) -> None:
        self.id = id
        self.name = get_table_name(self.id)

    def __repr__(self) -> str:
        return f"Table({self.name}, name={self.id})"

    def __str__(self) -> str:
        return self.name

    def exists(self) -> bool:
        return bool(frappe.db.sql("SHOW TABLES LIKE %s", self.name))

    def find_index_candidates(
        self, queries: list[Query], qualifier: Callable | None = None
    ) -> list[IndexCandidate]:
        index_candidates = []

        for query in queries:
            if qualifier and not qualifier(query):
                continue

            if any(isinstance(token, Where) for token in query.parsed):
                index_generator = self.find_index_candidates_from_where_query
            else:
                index_generator = self.find_index_candidates_from_select_query

            for c in index_generator(query):
                if c and c not in index_candidates:
                    index_candidates.append(c)

        return index_candidates

    def find_index_candidates_from_where_query(self, query: Query) -> list[IndexCandidate]:
        query_index_candidate = []
        ic_operator = "AND"
        parsed_where = False

        for clause_token in query.parsed.tokens:
            # check order by clause for index candidates
            if parsed_where and not clause_token.ttype:
                ic = IndexCandidate(query=query, type=IndexCandidateType.ORDER_BY)
                if isinstance(clause_token, Identifier):
                    ic.append(clause_token.get_name())
                elif isinstance(clause_token, IdentifierList):
                    ic.extend([x.get_name() for x in clause_token.get_identifiers()])
                query_index_candidate.append(ic)
                continue

            # check only the where clause
            if not isinstance(clause_token, Where):
                continue
            parsed_where = True

            # we may want to check type of operators for finding appropriate index types at this stage
            for in_token in clause_token.tokens:
                if in_token.ttype == Keyword and in_token.value.upper() in {"AND", "OR"}:
                    ic_operator = in_token.value.upper()

                if not isinstance(in_token, Comparison):
                    continue

                if ic_operator == "OR":
                    index_candidate = IndexCandidate(query=query, type=IndexCandidateType.WHERE)
                else:
                    index_candidate = (
                        query_index_candidate[-1]
                        if query_index_candidate
                        else IndexCandidate(query=query, type=IndexCandidateType.WHERE)
                    )

                # Store comparison context for qualifying ICs later
                index_candidate.ctx = [t for t in in_token.tokens if not t.is_whitespace]

                for inner_token in in_token.tokens:
                    if not isinstance(inner_token, Identifier):
                        continue
                    if inner_token.get_parent_name() in {None, self.name}:
                        index_candidate.append(inner_token.get_name())

                if index_candidate not in query_index_candidate:
                    query_index_candidate.append(index_candidate)

        return query_index_candidate

    def find_index_candidates_from_select_query(self, query: Query) -> list[IndexCandidate]:
        query_index_candidates = []

        if query.d_parsed.query_type != QueryType.SELECT:
            return query_index_candidates

        ic = {
            "select": IndexCandidate(query=query, type=IndexCandidateType.SELECT),
            "order_by": IndexCandidate(query=query, type=IndexCandidateType.ORDER_BY),
        }

        for type in {"select", "order_by"}:
            for column in (query.d_parsed.columns_dict or {}).get(type, []):
                q_index_candidate = ic[type]

                if "." in column:
                    tbl, col = column.split(".")

                    if not query.table or not query.table.name:
                        q_index_candidate.append(col)
                    elif tbl == query.table.name:
                        q_index_candidate.append(col)
                else:
                    q_index_candidate.append(column)

                if q_index_candidate:
                    query_index_candidates.append(q_index_candidate)

        return query_index_candidates

    def qualify_index_candidates(
        self, index_candidates: list[IndexCandidate]
    ) -> list[IndexCandidate]:
        from toolbox.doctypes import MariaDBIndex

        # if there are multiple columns in the query, create a composite index
        # * then covering index, etc etc
        # TODO: Treat select ICs as lesser prioity than where ICs - ignore failures in creation of select ICs
        required_indexes = []
        index_candidates.sort(key=len, reverse=True)
        current_indexes = MariaDBIndex.get_indexes(self.name, reduce=True)

        for ic in index_candidates:
            # skip ic if over 5 columns - too many columns in an index is bad
            if len(ic) > 5:
                continue

            # skip ic if relevant index already exists
            if ic in current_indexes:
                continue

            # skip ic if duplicate, similar
            similar_index_found = False

            for x in required_indexes:
                ic_set = set(ic)
                x_set = set(x)

                if similar_index_found:
                    break

                # TODO: check ic.ctx and retain the better suited IC / mark them as similar for now
                # if A > const() B = const(), keep ic(B, A) and remove ic(A, B)
                if ic_set == x_set:
                    similar_index_found = True
                # if ic(A, B, C) is in the list, remove ic(A, B), ic(A, C) & other permutations
                elif ic_set.issubset(x_set):
                    similar_index_found = True

            if not similar_index_found:
                required_indexes.append(ic)

        return required_indexes


def get_analyzed_result(sql: str, verbose: bool = False):
    try:
        return frappe.db.sql(f"ANALYZE {sql}", as_dict=True, debug=verbose)
    except Exception as e:
        print(f"ERROR: {e} while analyzing {sql}")
        return [{"r_filtered": -1, "r_rows": "0.00", "Extra": "Using where"}]


class QueryBenchmark:
    def __init__(self, index_candidates: list[IndexCandidate], verbose=False):
        self.index_candidates = index_candidates
        self.verbose = verbose
        self.before = []
        self.after = []

    def __enter__(self):
        self.before = self.conduct_benchmark()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.after = self.conduct_benchmark()

    def conduct_benchmark(self) -> list[list[dict]]:
        return [
            get_analyzed_result(ic.query.get_sample(), verbose=self.verbose)
            for ic in self.index_candidates
        ]

    def compare_results(
        self, before: list[list[dict]], after: list[list[dict]]
    ) -> list[list[dict]]:
        results = [[]] * len(before)

        for i, (before_data, after_data) in enumerate(zip(before, after)):
            for before_row, after_row in zip(before_data, after_data):
                results[i].append(
                    {
                        "before": {
                            key: before_row[key] for key in {"r_rows", "r_filtered", "Extra"}
                        },
                        "after": {
                            key: after_row[key] for key in {"r_rows", "r_filtered", "Extra"}
                        },
                    }
                )

        return results

    def get_unchanged_results(self):
        for q_id, context_table in enumerate(self.compare_results(self.before, self.after)):
            changes_detected = False

            for row_id, context in enumerate(context_table):
                # if the number of rows read is the same, then the index is not helping
                rows_read_changed = context["before"]["r_rows"] != context["after"]["r_rows"]

                # r_filtered relates to how many rows were read and filtered out,
                # higher the value, better the index - r_filtered = 100 best
                rows_selectivity_changed = (
                    context["before"]["r_filtered"] != context["after"]["r_filtered"]
                )

                # if the number of rows read and the selectivity of the index has not changed, then the index is not helping
                if not rows_read_changed and not rows_selectivity_changed:
                    ...
                # if the selectivity has gotten worse, then the index is not helping
                elif (
                    rows_selectivity_changed
                    and context["before"]["r_filtered"] > context["after"]["r_filtered"]
                ):
                    ...
                else:
                    changes_detected = True

            if not changes_detected:
                yield q_id, context
