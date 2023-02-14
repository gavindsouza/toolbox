from contextlib import contextmanager
from enum import Enum, auto
from functools import lru_cache
from typing import TYPE_CHECKING, Callable

from click import secho
from sqlparse import parse
from sqlparse.sql import Comparison, Identifier, IdentifierList, Where
from sqlparse.tokens import Keyword, Whitespace, Wildcard

if TYPE_CHECKING:
    from sqlparse.sql import Statement

    from toolbox.doctypes import MariaDBQuery


def record_table(table: str) -> str:
    from html import escape

    import frappe

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


def record_query(
    query: str, p_query: str | None = None, call_stack: list[dict] | None = None
) -> "MariaDBQuery":
    import frappe

    if query_name := frappe.get_all("MariaDB Query", {"query": query}, limit=1):
        query_record = frappe.get_doc("MariaDB Query", query_name[0])
        query_record.parameterized_query = p_query
        query_record.occurence += 1

        if call_stack:
            # TODO: Let's just maintain one stack for now
            # if not query_record.call_stack:
            query_record.call_stack = frappe.as_json(call_stack)

        return query_record

    query_record = frappe.new_doc("MariaDB Query")
    query_record.query = query
    query_record.parameterized_query = p_query
    query_record.occurence = 1
    query_record.call_stack = frappe.as_json(call_stack)

    return query_record


def record_database_state():
    import frappe

    for tbl in frappe.db.get_tables(cached=False):
        if not frappe.db.exists("MariaDB Table", {"_table_name": tbl}):
            table_record = frappe.new_doc("MariaDB Table")
            table_record._table_name = tbl
            table_record._table_exists = True
            table_record.db_insert()


@contextmanager
def check_dbms_compatibility(conf):
    if conf.db_type != "mariadb":
        secho(f"WARN: This command might not be compatible with {conf.db_type}", fg="yellow")
    yield


@contextmanager
def handle_redis_connection_error():
    from redis.exceptions import ConnectionError

    try:
        yield
    except ConnectionError as e:
        secho(f"ERROR: {e}", fg="red")
        secho("NOTE: Make sure Redis services are running", fg="yellow")


def process_sql_metadata_chunk(
    queries: list[dict],
    site: str,
    setup: bool = True,
    chunk_size: int = 5_000,
    auto_commit: bool = True,
):
    import frappe
    from sqlparse import format as sql_format

    with frappe.init_site(site):
        sql_count = 0
        granularity = chunk_size // 100
        frappe.connect()

        TOOLBOX_TABLES = set(frappe.get_all("DocType", {"module": "Toolbox"}, pluck="name"))

        if setup:
            record_database_state()

        for query_info in queries:
            query: str = query_info["query"]

            if not query.lower().startswith(("select", "insert", "update", "delete")):
                continue

            parameterized_query: str = query_info["args"][0]

            # should check warnings too? unsure at this point
            explain_data = frappe.db.sql(f"EXPLAIN EXTENDED {query}", as_dict=True)

            if not explain_data:
                print(f"Cannot explain query: {query}")
                continue

            # Note: Desk doesn't like Queries with whitespaces in long text for show title in links for forms
            # Better to strip them off and format on demand
            query_record = record_query(
                sql_format(query, strip_whitespace=True, keyword_case="upper"),
                p_query=parameterized_query,
                call_stack=query_info["stack"],
            )
            for explain in explain_data:
                # skip Toolbox internal queries
                if explain["table"] not in TOOLBOX_TABLES:
                    query_record.apply_explain(explain)
            query_record.save()

            sql_count += 1
            # Show approximate progress
            print(
                f"Processed ~{round(sql_count / granularity) * granularity:,} queries per job"
                + " " * 5,
                end="\r",
            )

            if auto_commit and frappe.db.transaction_writes > chunk_size:
                frappe.db.commit()

        if auto_commit:
            frappe.db.commit()


@lru_cache(maxsize=None)
def get_table_name(table_id: str):
    # Note: Use this util only via CLI / single threaded
    import frappe

    return frappe.db.get_value("MariaDB Table", table_id, "_table_name")


@lru_cache(maxsize=None)
def get_table_id(table_name: str):
    # Note: Use this util only via CLI / single threaded
    import frappe

    return frappe.db.get_value("MariaDB Table", {"_table_name": table_name}, "name")


class Query:
    def __init__(self, sql: str, occurence: int = 1, table: "Table" = None) -> None:
        self.sql = sql
        self.occurence = occurence
        self.table = table

    def __repr__(self) -> str:
        sub = f", table={self.table}" if self.table else ""
        dotted = "..." if len(self.sql) > 11 else ""
        return f"Query({self.sql[:10]}{dotted}{sub})"

    @property
    def parsed(self) -> "Statement":
        if not hasattr(self, "_parsed"):
            self._parsed = parse(self.sql)[0]
        return self._parsed


class IndexCandidateType(Enum):
    SELECT: str = auto()
    WHERE: str = auto()


class IndexCandidate(list):
    def __init__(self, query: Query, type: IndexCandidateType) -> None:
        self.query = query
        self.type = type

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
        import frappe

        if frappe.db.sql("SHOW TABLES LIKE %s", self.name):
            return True
        return False

    def find_index_candidates(
        self, queries: list[Query], qualifier: Callable | None = None
    ) -> list[IndexCandidate]:
        index_candidates = []

        for query in queries:
            if qualifier and not qualifier(query):
                continue

            # TODO: handle subqueries by making this recursive
            if any(isinstance(token, Where) for token in query.parsed):
                for c in self.find_index_candidates_from_where_query(query):
                    if c and c not in index_candidates:
                        index_candidates.append(c)
            elif ic := self.find_index_candidates_from_select_query(query):
                index_candidates.append(ic)

        return index_candidates

    def find_index_candidates_from_where_query(self, query: Query) -> list[IndexCandidate]:
        query_index_candidate = []

        for clause_token in query.parsed.tokens:
            if not isinstance(clause_token, Where):
                continue

            # we may want to check type of operators for finding appropriate index types at this stage
            for in_token in clause_token.tokens:
                if not isinstance(in_token, Comparison):
                    continue

                index_candidate = IndexCandidate(query=query, type=IndexCandidateType.WHERE)
                for inner_token in in_token.tokens:
                    if not isinstance(inner_token, Identifier):
                        continue
                    if inner_token.get_parent_name() in {None, self.name}:
                        index_candidate.append(inner_token.get_name())
                query_index_candidate.append(index_candidate)

        return query_index_candidate

    def find_index_candidates_from_select_query(self, query: Query) -> IndexCandidate:
        query_index_candidate = IndexCandidate(query=query, type=IndexCandidateType.SELECT)

        if query.parsed.get_type() != "SELECT":
            return query_index_candidate

        in_select = False

        for clause_token in query.parsed.tokens:
            if clause_token.ttype == Whitespace:
                continue

            if in_select:
                if clause_token.ttype == Wildcard:
                    query_index_candidate.append(clause_token.value)
                elif isinstance(clause_token, Identifier):
                    query_index_candidate.append(clause_token.get_name())
                elif isinstance(clause_token, IdentifierList):
                    query_index_candidate.extend(
                        identifier.get_name() for identifier in clause_token.get_identifiers()
                    )
                elif clause_token.ttype == Keyword and clause_token.value.upper() == "FROM":
                    in_select = False
                    break
            else:
                in_select = clause_token.value.upper() == "SELECT"

        return query_index_candidate
