"""The onboarding API over HTTP, with the real routes, permissions and database."""

from __future__ import annotations

import json
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.connector_integration import gateway_host, progress, service
from app.connector_integration.routes import connector_router
from app.core.db import get_db
from app.models.schema_models import UserModel
from app.utils.token_parser import get_current_user
from vizai_connector_gateway import ConnectorOffline, InstanceStatus

from .payloads import mall_schema

BASE = "/api/v1/backend/connectors"


class FakeGateway:
    def __init__(self):
        self.online: set[str] = set()
        self.disconnected: list[tuple[str, str]] = []
        self.refreshed: list[str] = []

    def is_online(self, connector_id):
        return connector_id in self.online

    def instances(self, connector_id):
        if connector_id not in self.online:
            return []
        now = time.time()
        return [InstanceStatus(connector_id, "i1", "0.5.0", "10.0.0.5:4000", now - 60, now, 2)]

    def disconnect(self, connector_id, reason=""):
        self.disconnected.append((connector_id, reason))
        return 1

    async def refresh_schema(self, connector_id, reason=""):
        if connector_id not in self.online:
            raise ConnectorOffline("connector is offline (no agent instance connected)", connector_id=connector_id)
        self.refreshed.append(connector_id)
        return "i1"


@pytest.fixture
def gateway(monkeypatch):
    fake = FakeGateway()
    monkeypatch.setattr(gateway_host, "_gateway", fake)
    return fake


def make_client(db, as_user):
    app = FastAPI()
    app.include_router(connector_router)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_user] = lambda: {"sub": str(as_user.id)}
    return TestClient(app)


@pytest.fixture
def client(db, user, gateway):
    return make_client(db, user)


@pytest.fixture
def stranger(db, project):
    """Logged in, but has no role in the project."""
    row = UserModel(username="eve", password="x", email="eve@example.com", is_super=False)
    db.add(row)
    db.commit()
    return row


def create(client, project, name="Acme Malls"):
    return client.post(f"{BASE}/projects/{project.id}", json={"connection_name": name})


class TestCreate:
    def test_returns_everything_the_install_screen_needs_once(self, client, project):
        r = create(client, project)
        assert r.status_code == 201
        body = r.json()
        assert set(body) == {"connectorId", "connectionId", "taskId", "gatewayAddress", "dialect", "token", "install"}
        assert set(body["install"]) == {"downloadUrl", "version", "image", "env"} and body["install"]["image"] == "vizai/connector:0.7.0"
        assert body["token"].startswith("vzc_") and body["dialect"] == "postgres"
        assert body["gatewayAddress"] == "localhost:50051"
        assert r.headers["cache-control"] == "no-store"
        assert body["taskId"] in progress.progress_queues  # the UI can subscribe right away
        progress.forget(body["connectorId"])

    def test_the_token_works_and_is_never_shown_again(self, client, project, db):
        body = create(client, project).json()
        assert service.authenticate(db, body["connectorId"], body["token"]).ok
        for url in (f"{BASE}/{body['connectorId']}", f"{BASE}/projects/{project.id}"):
            shown = client.get(url).text
            assert body["token"] not in shown
        progress.forget(body["connectorId"])

    def test_a_duplicate_name_is_rejected(self, client, project):
        first = create(client, project).json()
        assert create(client, project).status_code == 400
        progress.forget(first["connectorId"])

    def test_empty_name_is_a_validation_error(self, client, project):
        assert client.post(f"{BASE}/projects/{project.id}", json={"connection_name": ""}).status_code == 422

    def test_a_user_without_access_cannot_create(self, db, project, stranger, gateway):
        assert create(make_client(db, stranger), project).status_code == 403


class TestReadAndStatus:
    def test_list_and_detail_reflect_the_live_gateway(self, client, project, gateway, db):
        body = create(client, project).json()
        cid = body["connectorId"]

        detail = client.get(f"{BASE}/{cid}").json()
        assert detail["status"] == "offline" and detail["instances"] == [] and detail["hasSchema"] is False

        gateway.online.add(cid)
        detail = client.get(f"{BASE}/{cid}").json()
        assert detail["status"] == "online"
        assert detail["instances"][0]["agentVersion"] == "0.5.0" and detail["instances"][0]["inflight"] == 2

        service.store_schema(db, cid, mall_schema(cid, schema_hash="h9"))
        listed = client.get(f"{BASE}/projects/{project.id}").json()["connectors"]
        assert [c["name"] for c in listed] == ["Acme Malls"]
        assert listed[0]["hasSchema"] is True and listed[0]["schemaHash"] == "h9"
        assert listed[0]["instances"] == []  # the list stays light
        progress.forget(cid)

    def test_unknown_connector_is_404(self, client):
        assert client.get(f"{BASE}/00000000-0000-0000-0000-000000000000").status_code == 404

    def test_other_projects_users_cannot_read_it(self, db, project, client, stranger, gateway):
        cid = create(client, project).json()["connectorId"]
        assert make_client(db, stranger).get(f"{BASE}/{cid}").status_code == 403
        progress.forget(cid)


class TestRotateAndRevoke:
    def test_rotate_gives_a_new_token_and_cuts_the_old_one(self, client, project, gateway, db):
        body = create(client, project).json()
        cid = body["connectorId"]
        r = client.post(f"{BASE}/{cid}/rotate-token")
        assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
        new = r.json()["token"]
        assert new != body["token"]
        assert r.json()["token"] != body["token"] and r.json()["install"]["image"] == "vizai/connector:0.7.0"
        assert r.json()["gatewayAddress"] == body["gatewayAddress"]
        assert not service.authenticate(db, cid, body["token"]).ok and service.authenticate(db, cid, new).ok
        assert gateway.disconnected == [(cid, "token rotated")]
        progress.forget(cid)

    def test_revoke_is_final(self, client, project, gateway, db):
        body = create(client, project).json()
        cid = body["connectorId"]
        r = client.post(f"{BASE}/{cid}/revoke")
        assert r.status_code == 200 and r.json()["status"] == "revoked"
        assert gateway.disconnected == [(cid, "connector revoked")]
        assert not service.authenticate(db, cid, body["token"]).ok
        assert client.post(f"{BASE}/{cid}/rotate-token").status_code == 409
        assert client.post(f"{BASE}/{cid}/refresh-schema").status_code == 409
        assert body["taskId"] not in progress.progress_queues

    def test_a_user_without_access_cannot_rotate_or_revoke(self, db, project, client, stranger, gateway):
        cid = create(client, project).json()["connectorId"]
        other = make_client(db, stranger)
        assert other.post(f"{BASE}/{cid}/rotate-token").status_code == 403
        assert other.post(f"{BASE}/{cid}/revoke").status_code == 403
        assert gateway.disconnected == []
        progress.forget(cid)


class TestRefresh:
    def test_asks_a_connected_agent(self, client, project, gateway):
        cid = create(client, project).json()["connectorId"]
        gateway.online.add(cid)
        r = client.post(f"{BASE}/{cid}/refresh-schema")
        assert r.status_code == 202 and r.json() == {"requested": True, "instanceId": "i1"}
        assert gateway.refreshed == [cid]
        progress.forget(cid)

    def test_an_offline_connector_fails_fast_with_the_gateways_status(self, client, project):
        cid = create(client, project).json()["connectorId"]
        r = client.post(f"{BASE}/{cid}/refresh-schema")
        assert r.status_code == 503 and "offline" in r.json()["detail"]
        progress.forget(cid)

    def test_without_a_running_gateway(self, db, user, project, monkeypatch):
        monkeypatch.setattr(gateway_host, "_gateway", None)
        client = make_client(db, user)
        cid = create(client, project).json()["connectorId"]
        assert client.post(f"{BASE}/{cid}/refresh-schema").status_code == 503
        assert client.get(f"{BASE}/{cid}").json()["status"] == "offline"  # falls back to the stored status
        progress.forget(cid)


def test_the_response_models_never_contain_a_token_hash(client, project, db):
    cid = create(client, project).json()["connectorId"]
    row = service.get_connector(db, __import__("uuid").UUID(cid))
    text = client.get(f"{BASE}/{cid}").text + json.dumps(client.get(f"{BASE}/projects/{project.id}").json())
    assert row.token_hash not in text
    progress.forget(cid)
