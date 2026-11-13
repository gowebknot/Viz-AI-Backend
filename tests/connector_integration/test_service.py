from __future__ import annotations

import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from app.connector_integration import service
from app.connector_integration.tokens import hash_token
from app.models.schema_models import DatabaseConnectionModel, ExternalConnectorModel

from .payloads import mall_schema


@pytest.fixture
def created(db, project, user):
    return service.create_connector(db, project_id=project.id, user_id=user.id, name="Acme Malls", consent_given=True)


def test_creating_a_connector_makes_a_credential_free_datasource(db, created):
    connector, connection = created.connector, created.connection
    assert connection.connection_mode == "connector" and connection.connector_id == connector.id
    assert connection.db_type == "postgres"  # the real dialect, not a made-up one
    assert connection.db_connection_string is None and connection.db_password is None
    assert connection.db_schema is None and connection.status is False
    assert connector.status == "offline" and connector.schema_hash is None


def test_only_a_hash_of_the_token_is_stored(db, created):
    row = db.get(ExternalConnectorModel, created.connector.id)
    assert row.token_hash == hash_token(created.token) and created.token not in row.token_hash
    columns = [str(getattr(row, c.name)) for c in ExternalConnectorModel.__table__.columns]
    assert all(created.token not in value for value in columns)


def test_names_are_unique_like_in_the_direct_flow(db, project, user, created):
    with pytest.raises(HTTPException) as raised:
        service.create_connector(db, project_id=project.id, user_id=user.id, name="Acme Malls", consent_given=None)
    assert raised.value.status_code == 400


def test_unknown_project_is_404(db, user):
    with pytest.raises(HTTPException) as raised:
        service.create_connector(db, project_id=uuid4(), user_id=user.id, name="x", consent_given=None)
    assert raised.value.status_code == 404


def test_existing_style_rows_default_to_direct(db, project):
    row = DatabaseConnectionModel(connection_name="old", db_connection_string="enc", project_id=project.id, db_type="postgres")
    db.add(row)
    db.commit()
    db.refresh(row)
    assert row.connection_mode == "direct" and row.connector_id is None


class TestAuthenticate:
    def test_right_token(self, db, created):
        result = service.authenticate(db, str(created.connector.id), created.token)
        assert result.ok and result.known_schema_hash == ""

    @pytest.mark.parametrize("token", ["", "vzc_wrong", "x" * 200])
    def test_wrong_token(self, db, created, token):
        assert not service.authenticate(db, str(created.connector.id), token).ok

    @pytest.mark.parametrize("connector_id", [str(uuid4()), "not-a-uuid", "", "c-local"])
    def test_unknown_or_malformed_id(self, db, created, connector_id):
        assert not service.authenticate(db, connector_id, created.token).ok

    def test_another_connectors_token_is_not_accepted(self, db, project, user, created):
        other = service.create_connector(db, project_id=project.id, user_id=user.id, name="Other", consent_given=None)
        assert not service.authenticate(db, str(created.connector.id), other.token).ok

    def test_revoked(self, db, created):
        service.revoke(db, created.connector)
        result = service.authenticate(db, str(created.connector.id), created.token)
        assert not result.ok and "revoked" in result.reason

    def test_a_deleted_datasource_makes_the_token_useless(self, db, created):
        db.delete(created.connection)
        db.commit()
        result = service.authenticate(db, str(created.connector.id), created.token)
        assert not result.ok and "deleted" in result.reason

    def test_known_schema_hash_is_only_reported_for_a_saved_schema(self, db, created):
        cid = str(created.connector.id)
        service.store_schema(db, cid, mall_schema(cid, schema_hash="abc"))
        assert service.authenticate(db, cid, created.token).known_schema_hash == "abc"
        created.connection.db_schema = None  # e.g. cleared by hand: the agent must push again
        db.commit()
        assert service.authenticate(db, cid, created.token).known_schema_hash == ""

    def test_the_reason_never_contains_the_token(self, db, created):
        assert created.token not in service.authenticate(db, str(created.connector.id), "vzc_wrong").reason


def test_rotating_replaces_the_token(db, created):
    new = service.rotate_token(db, created.connector)
    cid = str(created.connector.id)
    assert new != created.token
    assert not service.authenticate(db, cid, created.token).ok
    assert service.authenticate(db, cid, new).ok


def test_a_revoked_connector_cannot_be_rotated(db, created):
    service.revoke(db, created.connector)
    with pytest.raises(HTTPException) as raised:
        service.rotate_token(db, created.connector)
    assert raised.value.status_code == 409


def test_revoking_marks_both_rows(db, created):
    service.revoke(db, created.connector)
    db.refresh(created.connector)
    db.refresh(created.connection)
    assert created.connector.status == "revoked" and created.connector.revoked_at is not None
    assert created.connection.status is False


class TestStatus:
    def test_connect_and_disconnect(self, db, created):
        cid = str(created.connector.id)
        service.mark_connected(db, cid, dialect="postgres", agent_version="0.5.0", protocol_version="1.0")
        db.refresh(created.connector)
        db.refresh(created.connection)
        assert created.connector.status == "online" and created.connector.agent_version == "0.5.0"
        assert created.connection.status is True and created.connection.last_checked is not None

        service.mark_disconnected(db, cid, remaining_instances=1)  # another instance is still there
        db.refresh(created.connector)
        assert created.connector.status == "online"

        service.mark_disconnected(db, cid, remaining_instances=0)
        db.refresh(created.connector)
        db.refresh(created.connection)
        assert created.connector.status == "offline" and created.connection.status is False

    def test_a_revoked_connector_stays_revoked(self, db, created):
        cid = str(created.connector.id)
        service.revoke(db, created.connector)
        service.mark_connected(db, cid, dialect="postgres", agent_version="x", protocol_version="1.0")
        service.mark_disconnected(db, cid, remaining_instances=0)
        db.refresh(created.connector)
        assert created.connector.status == "revoked"

    def test_unknown_connector_is_ignored(self, db):
        service.mark_connected(db, str(uuid4()), dialect="postgres", agent_version="x", protocol_version="1.0")
        service.mark_disconnected(db, str(uuid4()), remaining_instances=0)

    def test_startup_resets_stale_online_flags(self, db, created):
        service.mark_connected(db, str(created.connector.id), dialect="postgres", agent_version="x", protocol_version="1.0")
        service.reset_online_status(db)
        db.refresh(created.connector)
        db.refresh(created.connection)
        assert created.connector.status == "offline" and created.connection.status is False


class TestStoreSchema:
    def test_saves_the_direct_flows_json_and_the_graph(self, db, created):
        cid = str(created.connector.id)
        stored = service.store_schema(db, cid, mall_schema(cid, schema_hash="h1"))
        assert stored.table_count == 3 and stored.schema_hash == "h1"
        db.refresh(created.connection)
        db.refresh(created.connector)
        schema = json.loads(created.connection.db_schema)
        assert [t["name"] for t in schema["tables"]] == ["malls", "stores", "v_mall_revenue"]
        assert schema["min_date"] == "2023-01-01"
        graph = json.loads(created.connection.ds_graph_json)
        assert graph["stats"]["table_count"] == 3 and graph["stats"]["relation_count"] == 1
        assert created.connector.schema_hash == "h1"

    def test_a_new_push_replaces_the_old_schema(self, db, created):
        cid = str(created.connector.id)
        service.store_schema(db, cid, mall_schema(cid, schema_hash="h1"))
        newer = mall_schema(cid, schema_hash="h2")
        service.store_schema(db, cid, replace(newer, tables=newer.tables[:1]))
        db.refresh(created.connection)
        assert len(json.loads(created.connection.db_schema)["tables"]) == 1

    def test_refuses_a_revoked_connector(self, db, created):
        service.revoke(db, created.connector)
        with pytest.raises(LookupError):
            service.store_schema(db, str(created.connector.id), mall_schema())

    def test_refuses_when_the_datasource_is_gone(self, db, created):
        db.delete(created.connection)
        db.commit()
        with pytest.raises(LookupError):
            service.store_schema(db, str(created.connector.id), mall_schema())


def test_list_connectors_is_per_project(db, project, user, created):
    from app.models.schema_models import ProjectModel

    other = ProjectModel(name="Other", super_user_id=user.id, primary_domain="x")
    db.add(other)
    db.commit()
    service.create_connector(db, project_id=other.id, user_id=user.id, name="Elsewhere", consent_given=None)
    mine = service.list_connectors(db, project.id)
    assert [c.name for c, _ in mine] == ["Acme Malls"]
    assert isinstance(mine[0][0].id, UUID) and mine[0][1].id == created.connection.id
