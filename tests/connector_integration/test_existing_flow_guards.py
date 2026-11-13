"""The two places in the existing datasource code that a connector row would have broken, and
proof that direct datasources still behave as before."""

from __future__ import annotations

import asyncio
import time

import pytest

from app.connector_integration import gateway_host, service
from app.models.schema_models import DatabaseConnectionModel
from app.services.db_connection import _test_single_connection, get_connections
from app.utils.crypt import encrypt_string


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def direct(db, project):
    row = DatabaseConnectionModel(
        connection_name="legacy",
        db_connection_string=encrypt_string("postgresql://u:p@127.0.0.1:1/nothing"),  # nothing listens on port 1
        db_username="u",
        db_password=encrypt_string("p"),
        db_host_link="127.0.0.1",
        db_name="nothing",
        project_id=project.id,
        db_type="postgres",
    )
    db.add(row)
    db.commit()
    return row


@pytest.fixture
def connector(db, project, user):
    return service.create_connector(db, project_id=project.id, user_id=user.id, name="Acme Malls", consent_given=None)


class FakeGateway:
    def __init__(self, online):
        self.online = online

    def is_online(self, connector_id):
        return connector_id in self.online


class TestListingDatasources:
    def test_a_connector_row_does_not_break_the_projects_list(self, db, project, user, direct, connector):
        """Before the guard, decrypting the missing connection string made the whole list a 400."""
        result = run(get_connections(project.id, db, {"sub": str(user.id)}))
        by_name = {c["name"]: c for c in result["connections"]}
        assert set(by_name) == {"legacy", "Acme Malls"}

        mine = by_name["Acme Malls"]
        assert mine["connection_mode"] == "connector" and mine["connector_id"] == str(connector.connector.id)
        assert mine["db_type"] == "postgres"
        assert mine["db_connection_string"] is None and mine["db_password"] is None and mine["db_username"] is None

    def test_a_direct_row_is_listed_exactly_as_before(self, db, project, user, direct):
        entry = run(get_connections(project.id, db, {"sub": str(user.id)}))["connections"][0]
        assert entry["db_connection_string"] == "postgresql://u:p@127.0.0.1:1/nothing"
        assert entry["db_password"] == "p"
        assert "connection_mode" not in entry and "connector_id" not in entry  # no new keys for direct rows


class TestConnectionCheck:
    def test_connector_online_follows_the_gateway(self, db, connector, monkeypatch):
        cid = str(connector.connector.id)
        monkeypatch.setattr(gateway_host, "_gateway", FakeGateway({cid}))
        result = run(_test_single_connection(connector.connection, db))
        assert result["status"] is True and result["error_message"] is None
        db.refresh(connector.connection)
        assert connector.connection.status is True

    def test_connector_offline_says_so_and_opens_no_engine(self, db, connector, monkeypatch):
        monkeypatch.setattr(gateway_host, "_gateway", FakeGateway(set()))
        started = time.monotonic()
        result = run(_test_single_connection(connector.connection, db))
        assert result["status"] is False and "offline" in result["error_message"]
        assert time.monotonic() - started < 1  # no connection attempt, no decrypt of a missing string
        db.refresh(connector.connection)
        assert connector.connection.status is False

    def test_connector_without_a_gateway_is_offline_not_an_error(self, db, connector, monkeypatch):
        monkeypatch.setattr(gateway_host, "_gateway", None)
        assert run(_test_single_connection(connector.connection, db))["status"] is False

    def test_a_direct_row_still_goes_through_its_engine(self, db, direct):
        result = run(_test_single_connection(direct, db))
        assert result["status"] is False  # nothing listens on port 1: the engine path ran and failed cleanly
        assert result["error_message"] and "offline" not in result["error_message"]
