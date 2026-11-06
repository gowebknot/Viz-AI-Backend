"""Phase 7: SQL on a connector datasource goes through the gateway, and only connector rows do.

The gateway is faked here; scripts/e2e_backend.sh runs the same paths against the real agent and
compares them with a real direct connection to the same database.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime
from decimal import Decimal

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.connector_integration import gateway_host, service
from app.core.db import get_db
from app.models.schema_models import ChartModel, DashboardChartsModel, DashboardModel, DatabaseConnectionModel
from app.routes import backend as backend_routes
from app.routes.ontology import _fetch_physical_schema
from app.schemas import UpdateDBConnectionRequest
from app.services import business_insights, embed, generate_queries
from app.services.db_connection import update_db_connection
from app.services.query_executor import (
    ConnectorQueryError,
    require_direct,
    saved_physical_schema,
    validate_read_only_sql,
)
from app.utils.crypt import decrypt_string, encrypt_string
from app.utils.token_parser import get_current_user
from vizai_connector_gateway import (
    ConnectorOffline,
    ConnectorTimeout,
    QueryFailed,
    QueryRejected,
    QueryResult,
    RateLimited,
    ResultColumn,
)



def result(columns, rows, truncated=False):
    return QueryResult(
        request_id="r1",
        connector_id="c1",
        instance_id="i1",
        columns=[ResultColumn(name, "text") for name in columns],
        rows=[tuple(row) for row in rows],
        row_count=len(rows),
        truncated=truncated,
        execution_time_ms=1,
        round_trip_ms=2,
    )


class FakeGateway:
    """Records every SQL it is asked to run. `answer` is a QueryResult, an exception, or a function."""

    def __init__(self, answer=None, online=True):
        self.answer = answer if answer is not None else result(["a", "b"], [["x", 1]])
        self.online = online
        self.calls: list[tuple[str, str]] = []
        self.kwargs: list[dict] = []

    def is_online(self, connector_id):
        return self.online

    def _reply(self, connector_id, sql, **kwargs):
        self.calls.append((connector_id, sql))
        self.kwargs.append(kwargs)
        answer = self.answer(sql) if callable(self.answer) else self.answer
        if isinstance(answer, Exception):
            raise answer
        return answer

    def execute_sync(self, connector_id, sql, **kwargs):
        return self._reply(connector_id, sql, **kwargs)

    async def execute(self, connector_id, sql, **kwargs):
        return self._reply(connector_id, sql, **kwargs)


@pytest.fixture
def connector(db, project, user):
    created = service.create_connector(db, project_id=project.id, user_id=user.id, name="Acme Malls", consent_given=True)
    return created.connection


@pytest.fixture
def direct(db, project):
    """A direct row pointing at the test database itself, so its path really runs."""
    row = DatabaseConnectionModel(
        connection_name="legacy",
        db_connection_string=encrypt_string(os.environ["VIZAI_TEST_DATABASE_URL"]),
        db_username="u",
        db_password=encrypt_string("p"),
        db_host_link="127.0.0.1",
        db_name="nothing",
        project_id=project.id,
        db_type="postgres",
        consent_given=True,
    )
    db.add(row)
    db.commit()
    return row


@pytest.fixture
def gateway(monkeypatch):
    fake = FakeGateway()
    monkeypatch.setattr(gateway_host, "_gateway", fake)
    return fake


@pytest.fixture
def no_gateway_allowed(monkeypatch):
    """Any use of the gateway fails the test: the direct flow must never come near it."""

    def boom():
        raise AssertionError("the direct flow reached the connector gateway")

    monkeypatch.setattr(gateway_host, "get_gateway", boom)


class TestVizAISqlCheck:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "select 1;",
            "  WITH x AS (SELECT 1) SELECT * FROM x ;  ",
            "(SELECT 1) UNION (SELECT 2)",
            "-- a comment; with a semicolon\nSELECT 1",
            "/* DROP TABLE x; /* nested */ */ SELECT 1",
            "SELECT 'a;b', \"c;d\", $$e;f$$, $tag$g;h$tag$",
            "SELECT 'it''s; fine'",
            "VALUES (1)",
            "TABLE stores",
        ],
    )
    def test_single_reads_pass(self, sql):
        validate_read_only_sql(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "",
            "   ",
            ";",
            "-- only a comment",
            "INSERT INTO x VALUES (1)",
            "drop table x",
            "UPDATE x SET a = 1",
            "SELECT 1; DROP TABLE x",
            "SELECT 1; SELECT 2",
            "SELECT 1; 'text'",
            "/* hidden */ DELETE FROM x",
            "(DELETE FROM x)",
            "'text'",
        ],
    )
    def test_everything_else_is_refused_before_leaving_vizai(self, sql):
        with pytest.raises(ConnectorQueryError) as caught:
            validate_read_only_sql(sql)
        assert caught.value.status_code == 400


def run_query(db, row, user, sql="SELECT a, b FROM t", **kwargs):
    return generate_queries.execute_external_query(
        db=db,
        datasource_connection_id=row.id,
        query_input=sql,
        token_payload={"sub": str(user.id)},
        **kwargs,
    )


class TestChartQueries:
    def test_goes_to_this_rows_connector(self, db, user, connector, gateway):
        run_query(db, connector, user)
        assert gateway.calls == [(str(connector.connector_id), "SELECT a, b FROM t")]

    def test_legacy_response(self, db, user, connector, gateway):
        gateway.answer = result(["store", "total"], [["Mall A", Decimal("12.50")], ["Mall B", 3]])
        assert run_query(db, connector, user) == {
            "result": [{"label": "Mall A", "value": Decimal("12.50")}, {"label": "Mall B", "value": 3}],
            "x_axis": "store",
            "y_axis": "total",
        }

    def test_tabular_response_is_json_safe_and_picks_the_numeric_axis(self, db, user, connector, gateway):
        gateway.answer = result(
            ["day", "note", "total", "at"],
            [[date(2026, 1, 2), "n", Decimal("1.5"), datetime(2026, 1, 2, 3, 4)]],
        )
        assert run_query(db, connector, user, response_format="tabular") == {
            "result": [{"day": "2026-01-02", "note": "n", "total": 1.5, "at": "2026-01-02T03:04:00"}],
            "x_axis": "day",
            "y_axis": "total",
        }

    def test_axis_hints_are_kept(self, db, user, connector, gateway):
        response = run_query(db, connector, user, response_format="tabular", x_axis="b", y_axis="a")
        assert (response["x_axis"], response["y_axis"]) == ("b", "a")

    def test_no_rows(self, db, user, connector, gateway):
        gateway.answer = result(["a", "b"], [])
        assert run_query(db, connector, user) == {"result": [], "x_axis": None, "y_axis": None}
        assert run_query(db, connector, user, response_format="tabular") == {"result": [], "x_axis": None, "y_axis": None}

    def test_dates_are_filled_in_before_sending(self, db, user, connector, gateway):
        run_query(
            db, connector, user,
            sql="SELECT 1 WHERE d >= :from_date AND d <= [MAX_DATE]",
            from_date="2026-01-01", to_date="2026-02-01",
        )
        assert gateway.calls[0][1] == "SELECT 1 WHERE d >= '2026-01-01' AND d <= '2026-02-01'"

    def test_a_cut_off_result_still_shows_and_says_so(self, db, user, connector, gateway):
        gateway.answer = result(["a", "b"], [["x", 1]], truncated=True)
        assert run_query(db, connector, user)["truncated"] is True

    def test_a_complete_result_adds_no_key(self, db, user, connector, gateway):
        assert "truncated" not in run_query(db, connector, user)

    def test_a_database_error_answers_like_the_direct_path(self, db, user, connector, gateway):
        gateway.answer = QueryFailed('column "x" does not exist')
        assert run_query(db, connector, user) == {"error": 'column "x" does not exist'}

    def test_a_write_never_reaches_the_gateway(self, db, user, connector, gateway):
        with pytest.raises(ConnectorQueryError):
            run_query(db, connector, user, sql="DELETE FROM t")
        assert gateway.calls == []

    def test_gateway_off_is_503_not_a_crash(self, db, user, connector, monkeypatch):
        monkeypatch.setattr(gateway_host, "_gateway", None)
        with pytest.raises(ConnectorQueryError) as caught:
            run_query(db, connector, user)
        assert caught.value.status_code == 503


@pytest.fixture
def client(db, user):
    app = FastAPI()
    app.include_router(backend_routes.backend_router)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_user] = lambda: {"sub": str(user.id)}
    return TestClient(app, raise_server_exceptions=False)


class TestTheRoute:
    def post(self, client, row, sql="SELECT 1"):
        return client.post(f"/api/v1/backend/excecute-query/{row.id}/", json={"query": sql})

    @pytest.mark.parametrize(
        "failure, code",
        [
            (ConnectorOffline("connector is offline"), 503),
            (ConnectorTimeout("timed out"), 504),
            (QueryRejected("not allowed", error_code="REJECTED_TABLE_NOT_ALLOWED"), 400),
            (RateLimited("too many", retry_after_s=1.2), 429),
        ],
    )
    def test_connector_failures_keep_their_status(self, client, connector, gateway, failure, code):
        gateway.answer = failure
        response = self.post(client, connector)
        assert response.status_code == code
        assert response.json()["detail"] == failure.message

    def test_rate_limit_says_when_to_retry(self, client, connector, gateway):
        gateway.answer = RateLimited("too many", retry_after_s=1.2)
        assert self.post(client, connector).headers["retry-after"] == "2"

    def test_success(self, client, connector, gateway):
        response = self.post(client, connector)
        assert response.status_code == 200 and response.json()["result"] == [{"label": "x", "value": 1}]

    def test_direct_errors_are_unchanged(self, client, project):
        """The route's catch-all still turns other HTTP errors into a 500, as it always has."""
        response = client.post(
            "/api/v1/backend/excecute-query/00000000-0000-0000-0000-000000000000/", json={"query": "SELECT 1"}
        )
        assert response.status_code == 500 and response.json()["detail"] == "Unexpected error occurred"


class TestTheDirectFlowIsUntouched:
    def test_runs_on_its_own_engine(self, db, user, direct, no_gateway_allowed):
        response = run_query(db, direct, user, sql="SELECT 'x' AS a, 1 AS b")
        assert response == {"result": [{"label": "x", "value": 1}], "x_axis": "a", "y_axis": "b"}

    def test_tabular(self, db, user, direct, no_gateway_allowed):
        response = run_query(db, direct, user, sql="SELECT 'x' AS a, 1 AS b", response_format="tabular")
        assert response == {"result": [{"a": "x", "b": 1}], "x_axis": "a", "y_axis": "b"}

    def test_the_connector_sql_check_does_not_apply(self, db, user, direct, no_gateway_allowed):
        """The connector path refuses two statements. The direct path still runs them as before."""
        response = run_query(db, direct, user, sql="SELECT 1 AS a, 2 AS b; SELECT 3 AS a, 4 AS b")
        assert response == {"result": [{"label": "3", "value": 4}], "x_axis": "a", "y_axis": "b"}

    def test_sql_errors_keep_their_shape(self, db, user, direct, no_gateway_allowed):
        assert "error" in run_query(db, direct, user, sql="SELECT nope_nope")

    def test_sample_rows_still_go_to_the_llm_with_consent(self, db, project, user, direct, monkeypatch):
        direct.db_schema = json.dumps({"tables": []})
        db.commit()
        seen = {}

        async def fake_llm(url, payload):
            seen.update(payload)
            return {"queries": []}

        monkeypatch.setattr(generate_queries, "post_to_llm", fake_llm)
        monkeypatch.setattr(generate_queries, "get_sample_data", lambda conn: f"rows from {conn}")
        asyncio.run(generate_chart_suggestions(db, direct, project, user))
        assert seen["sample_data"] == f"rows from {decrypt_string(direct.db_connection_string)}"


def generate_chart_suggestions(db, row, project, user):
    request = generate_queries.QueryRequest(
        db_type="postgres", role="r", domain="d", min_date="2026-01-01", max_date="2026-02-01", api_key="k"
    )
    return generate_queries.generate_and_store_charts(db, row.id, project.id, request, {"sub": str(user.id)})


class TestChartSuggestions:
    def test_no_credentials_and_no_sample_rows(self, db, project, user, connector, monkeypatch):
        connector.db_schema = json.dumps({"tables": []})
        db.commit()
        seen = {}

        async def fake_llm(url, payload):
            seen.update(payload)
            return {"queries": []}

        def must_not_run(_):
            raise AssertionError("sample rows must not leave a connector datasource")

        monkeypatch.setattr(generate_queries, "post_to_llm", fake_llm)
        monkeypatch.setattr(generate_queries, "get_sample_data", must_not_run)
        asyncio.run(generate_chart_suggestions(db, connector, project, user))
        assert seen["sample_data"] == "" and seen["db_type"] == "postgres"

    def test_before_the_schema_arrives(self, db, project, user, connector):
        with pytest.raises(HTTPException) as caught:
            asyncio.run(generate_chart_suggestions(db, connector, project, user))
        assert caught.value.status_code == 409


KPIS = [
    {"kpi_title": "One", "description": "d1", "sql_query": "SELECT 1"},
    {"kpi_title": "Two", "description": "d2", "sql_query": "SELECT bad"},
]


class TestKpiQueries:
    def test_same_entries_as_the_direct_path(self, connector, gateway):
        def answer(sql):
            if "bad" in sql:
                return QueryFailed("no such column")
            return result(["total", "day", "none"], [[Decimal("2.5"), date(2026, 1, 2), None]])

        gateway.answer = answer
        first, second = asyncio.run(business_insights.execute_kpi_queries(connector, KPIS))
        assert first == {
            "kpi_title": "One",
            "description": "d1",
            "query": "SELECT 1",
            "success": True,
            "data": [{"total": "2.5", "day": "2026-01-02", "none": None}],
            "row_count": 1,
        }
        assert second == {
            "kpi_title": "Two",
            "description": "d2",
            "query": "SELECT bad",
            "success": False,
            "error": "no such column",
            "data": [],
            "row_count": 0,
        }

    def test_one_rejected_query_does_not_stop_the_others(self, connector, gateway):
        gateway.answer = lambda sql: QueryRejected("no", error_code="X") if "bad" in sql else result(["a"], [[1]])
        first, second = asyncio.run(business_insights.execute_kpi_queries(connector, KPIS))
        assert first["success"] is True and second["success"] is False and second["error"] == "no"

    def test_offline_fails_fast(self, connector, gateway):
        gateway.online = False
        with pytest.raises(ConnectorQueryError) as caught:
            asyncio.run(business_insights.execute_kpi_queries(connector, KPIS))
        assert caught.value.status_code == 503 and gateway.calls == []

    def test_direct_kpis_use_the_engine(self, direct, no_gateway_allowed):
        kpi = {"kpi_title": "One", "description": "d", "sql_query": "SELECT 1 AS a"}
        [entry] = asyncio.run(business_insights.execute_kpi_queries(direct, [kpi]))
        assert entry["success"] is True and entry["data"] == [{"a": 1}]

    def test_live_schema_reading_is_refused_cleanly(self, connector):
        with pytest.raises(ConnectorQueryError) as caught:
            asyncio.run(business_insights.extract_database_schema(connector))
        assert caught.value.status_code == 400


@pytest.fixture
def embed_chart(db, project, user):
    def make(row, query="SELECT day, total FROM sales;", x_axis="day"):
        dashboard = DashboardModel(title="D", project_id=project.id, created_by=user.id)
        chart = ChartModel(
            title="Sales", query=query, type="aggregate", relevance=1.0, chart_type="line",
            created_by=user.id, x_axis=x_axis, y_axis="total", is_time_based=True,
        )
        db.add_all([dashboard, chart])
        db.flush()
        db.add(DashboardChartsModel(dashboard_id=dashboard.id, chart_id=chart.id, database_connection_id=row.id))
        db.commit()
        return dashboard, chart

    return make


class TestEmbed:
    def test_chart_data(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector)
        gateway.answer = result(["day", "total"], [[date(2026, 1, 2), Decimal("1.5")]])
        response = asyncio.run(embed.get_embed_chart_data_by_dashboard(dashboard.id, chart.id, db))
        assert response == {
            "success": True,
            "data": [{"day": "2026-01-02", "total": 1.5}],
            "chart_id": str(chart.id),
            "chart_type": "line",
            "title": "Sales",
            "x_axis": "day",
            "y_axis": "total",
            "is_time_based": True,
        }
        assert gateway.calls[0][1] == chart.query

    def test_date_filter_is_inlined_after_validation(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector)
        asyncio.run(
            embed.get_embed_chart_data_by_dashboard(
                dashboard.id, chart.id, db, start_date="2026-01-01", end_date="2026-02-01"
            )
        )
        sql = gateway.calls[0][1]
        assert "BETWEEN '2026-01-01' AND '2026-02-01'" in sql and ":start_date" not in sql and ";" not in sql

    def test_a_bad_date_never_reaches_the_sql(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector)
        with pytest.raises(HTTPException) as caught:
            asyncio.run(
                embed.get_embed_chart_data_by_dashboard(
                    dashboard.id, chart.id, db, start_date="2026-01-01' OR 1=1 --", end_date="2026-02-01"
                )
            )
        assert caught.value.status_code == 400 and gateway.calls == []

    def test_date_column_is_worked_out_when_not_stored(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector, x_axis=None)
        gateway.answer = result(["day", "total"], [[date(2026, 1, 2), 1]])
        asyncio.run(
            embed.get_embed_chart_data_by_dashboard(
                dashboard.id, chart.id, db, start_date="2026-01-01", end_date="2026-02-01"
            )
        )
        assert "LIMIT 1" in gateway.calls[0][1] and '"day"::date' in gateway.calls[1][1]

    def test_a_sql_error_answers_like_the_direct_path(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector)
        gateway.answer = QueryFailed("boom")
        with pytest.raises(HTTPException) as caught:
            asyncio.run(embed.get_embed_chart_data_by_dashboard(dashboard.id, chart.id, db))
        assert caught.value.status_code == 500 and caught.value.detail == "Failed to fetch chart data"

    def test_offline_keeps_its_status(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector)
        gateway.answer = ConnectorOffline("connector is offline")
        with pytest.raises(HTTPException) as caught:
            asyncio.run(embed.get_embed_chart_data_by_dashboard(dashboard.id, chart.id, db))
        assert caught.value.status_code == 503

    def test_date_range(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector)
        gateway.answer = result(["min_date", "max_date"], [[date(2026, 1, 1), date(2026, 3, 1)]])
        assert asyncio.run(embed.get_embed_chart_date_range(dashboard.id, chart.id, db)) == {
            "min_date": "2026-01-01",
            "max_date": "2026-03-01",
            "date_column": "day",
            "is_time_based": True,
        }

    def test_date_range_failure_is_not_an_error(self, db, connector, gateway, embed_chart):
        dashboard, chart = embed_chart(connector)
        gateway.answer = QueryFailed("boom")
        response = asyncio.run(embed.get_embed_chart_date_range(dashboard.id, chart.id, db))
        assert response["min_date"] is None and response["date_column"] == "day"

    def test_direct_embed_uses_the_engine(self, db, direct, no_gateway_allowed, embed_chart):
        dashboard, chart = embed_chart(direct, query="SELECT 'x' AS day, 1 AS total")
        response = asyncio.run(embed.get_embed_chart_data_by_dashboard(dashboard.id, chart.id, db))
        assert response["data"] == [{"day": "x", "total": 1}]


class TestOntology:
    def test_uses_the_saved_schema_without_opening_anything(self, db, connector):
        connector.db_schema = json.dumps(
            {"tables": [{"name": "stores", "table": "stores", "columns": [{"name": "id", "type": "integer", "enum_values": ["a"]}]}]}
        )
        db.commit()
        assert _fetch_physical_schema(db, connector.id) == {"stores": [{"name": "id", "type": "integer"}]}

    def test_before_the_schema_arrives(self, connector):
        with pytest.raises(HTTPException) as caught:
            saved_physical_schema(connector)
        assert caught.value.status_code == 409


class TestUpdatingAConnectorRow:
    def update(self, db, row, user, **fields):
        return asyncio.run(
            update_db_connection(
                connection_id=row.id,
                data=UpdateDBConnectionRequest(**fields),
                db=db,
                token_payload={"sub": str(user.id)},
            )
        )

    @pytest.mark.parametrize(
        "fields",
        [
            {"db_connection_string": "postgresql://u:p@h/d"},
            {"db_username": "u"},
            {"db_password": "p"},
            {"db_host_link": "h"},
            {"db_name": "d"},
            {"db_type": "mysql"},
        ],
    )
    def test_it_can_never_gain_connection_details(self, db, user, connector, fields):
        with pytest.raises(HTTPException) as caught:
            self.update(db, connector, user, **fields)
        assert caught.value.status_code == 400
        db.refresh(connector)
        assert connector.db_connection_string is None and connector.db_password is None
        assert connector.db_type == "postgres"

    def test_it_can_be_renamed(self, db, user, connector):
        self.update(db, connector, user, connection_name="New name")
        db.refresh(connector)
        assert connector.connection_name == "New name"

    def test_a_direct_row_still_updates(self, db, user, direct):
        self.update(db, direct, user, db_name="other", connection_name="renamed")
        db.refresh(direct)
        assert direct.db_name == "other" and direct.connection_name == "renamed"


def test_require_direct_lets_direct_rows_through(direct):
    require_direct(direct, "This")


class TestRowLimit:
    def test_the_configured_cap_is_sent_to_the_gateway(self, db, user, connector, gateway):
        from app.core.settings import settings

        run_query(db, connector, user)
        assert gateway.kwargs[0]["max_rows"] == settings.CONNECTOR_QUERY_MAX_ROWS == 50000

    def test_async_callers_send_it_too(self, connector, gateway):
        asyncio.run(business_insights.execute_kpi_queries(connector, KPIS[:1]))
        assert gateway.kwargs[0]["max_rows"] == 50000

    def test_zero_means_the_gateway_default(self, db, user, connector, gateway, monkeypatch):
        from app.core.settings import settings

        monkeypatch.setattr(settings, "CONNECTOR_QUERY_MAX_ROWS", 0)
        run_query(db, connector, user)
        assert gateway.kwargs[0]["max_rows"] is None


class TestProjectAccessOnExecuteQuery:
    """/excecute-query used to accept any logged-in user who knew a datasource id."""

    @pytest.fixture
    def member(self, db, project):
        from app.models.schema_models import RoleModel, UserModel, UserProjectRoleModel

        role = RoleModel(name="viewer")
        person = UserModel(username="member", password="x", email="member@example.com", is_super=False)
        db.add_all([role, person])
        db.flush()
        db.add(UserProjectRoleModel(user_id=person.id, project_id=project.id, role_id=role.id))
        db.commit()
        return person

    @pytest.fixture
    def outsider(self, db):
        from app.models.schema_models import UserModel

        person = UserModel(username="outsider", password="x", email="outsider@example.com", is_super=False)
        db.add(person)
        db.commit()
        return person

    def client_for(self, db, person):
        app = FastAPI()
        app.include_router(backend_routes.backend_router)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_current_user] = lambda: {"sub": str(person.id)}
        return TestClient(app, raise_server_exceptions=False)

    def post(self, client, row):
        return client.post(f"/api/v1/backend/excecute-query/{row.id}/", json={"query": "SELECT 1 AS a, 2 AS b"})

    def test_an_outsider_is_refused_on_a_connector_datasource(self, db, connector, gateway, outsider):
        response = self.post(self.client_for(db, outsider), connector)
        assert response.status_code == 403 and gateway.calls == []

    def test_an_outsider_is_refused_on_a_direct_datasource(self, db, direct, outsider, no_gateway_allowed):
        assert self.post(self.client_for(db, outsider), direct).status_code == 403

    def test_a_project_member_still_gets_their_data(self, db, connector, gateway, member):
        assert self.post(self.client_for(db, member), connector).status_code == 200

    def test_a_member_of_the_project_can_query_a_direct_datasource(self, db, direct, member, no_gateway_allowed):
        response = self.post(self.client_for(db, member), direct)
        assert response.status_code == 200 and response.json()["result"] == [{"label": "1", "value": 2}]

    def test_super_users_are_unchanged(self, db, user, connector, gateway):
        assert self.post(self.client_for(db, user), connector).status_code == 200

    def test_an_unknown_datasource_answers_as_before(self, db, outsider):
        response = self.client_for(db, outsider).post(
            "/api/v1/backend/excecute-query/00000000-0000-0000-0000-000000000000/", json={"query": "SELECT 1"}
        )
        assert response.status_code == 500 and response.json()["detail"] == "Unexpected error occurred"
