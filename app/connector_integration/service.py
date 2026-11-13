"""Database operations for connectors. Synchronous, one Session per call.

The gateway hooks run on the application's event loop, so hooks_impl.py calls these in a worker
thread with their own Session; the onboarding routes call them with the request's Session.
Nothing here logs a token, and nothing stores or returns a database credential: a connector
datasource has no connection string.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import HTTPException, status
from sqlalchemy import update
from sqlalchemy.orm import Session

from app.connector_integration import schema_mapper
from app.connector_integration.tokens import hash_token, new_token, token_matches
from app.models.schema_models import DatabaseConnectionModel, ExternalConnectorModel, ProjectModel
from app.services.ds_graph import DSGraphBuilder

logger = logging.getLogger(__name__)

DEFAULT_DIALECT = "postgres"
def _utcnow() -> datetime:
    """Naive UTC, like every other DateTime column in this schema."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# Checked when the connector id is unknown, so an unknown id costs as much as a wrong token.
_DUMMY_HASH = hash_token("no-such-connector")


@dataclass(frozen=True)
class Authentication:
    ok: bool
    known_schema_hash: str = ""
    reason: str = ""


@dataclass(frozen=True)
class CreatedConnector:
    connector: ExternalConnectorModel
    connection: DatabaseConnectionModel
    token: str  # the only time it exists in clear text


@dataclass(frozen=True)
class StoredSchema:
    table_count: int
    schema_hash: str


# ---- onboarding ----------------------------------------------------------------


def create_connector(
    db: Session, *, project_id: UUID, user_id: UUID | None, name: str, consent_given: bool | None
) -> CreatedConnector:
    """A connector plus its datasource row (no connection string, no schema yet, offline)."""
    if db.query(ProjectModel.id).filter(ProjectModel.id == project_id).first() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    # Same rule as the direct flow: datasource names are unique.
    if db.query(DatabaseConnectionModel.id).filter(DatabaseConnectionModel.connection_name == name).first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Connection already exists")

    token = new_token()
    connector = ExternalConnectorModel(
        id=uuid4(), project_id=project_id, name=name, token_hash=hash_token(token), created_by=user_id
    )
    connection = DatabaseConnectionModel(
        id=uuid4(),
        connection_name=name,
        db_connection_string=None,
        db_schema=None,
        project_id=project_id,
        db_type=DEFAULT_DIALECT,  # the real dialect: the LLM service picks SQL syntax from it
        connection_mode="connector",
        connector_id=connector.id,
        consent_given=bool(consent_given) if consent_given is not None else False,
        status=False,
    )
    db.add(connector)
    db.flush()  # the connector row must exist before the datasource that points at it
    db.add(connection)
    db.commit()
    return CreatedConnector(connector=connector, connection=connection, token=token)


def get_connector(db: Session, connector_id: UUID) -> ExternalConnectorModel | None:
    return db.get(ExternalConnectorModel, connector_id)


def get_connection_for(db: Session, connector_id: UUID) -> DatabaseConnectionModel | None:
    return db.query(DatabaseConnectionModel).filter(DatabaseConnectionModel.connector_id == connector_id).first()


def list_connectors(db: Session, project_id: UUID) -> list[tuple[ExternalConnectorModel, DatabaseConnectionModel | None]]:
    rows = (
        db.query(ExternalConnectorModel)
        .filter(ExternalConnectorModel.project_id == project_id)
        .order_by(ExternalConnectorModel.created_at)
        .all()
    )
    return [(row, get_connection_for(db, row.id)) for row in rows]


def rotate_token(db: Session, connector: ExternalConnectorModel) -> str:
    if connector.status == "revoked":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Connector is revoked")
    token = new_token()
    connector.token_hash = hash_token(token)
    db.commit()
    return token


def revoke(db: Session, connector: ExternalConnectorModel) -> None:
    now = _utcnow()
    connector.status = "revoked"
    connector.revoked_at = now
    connection = get_connection_for(db, connector.id)
    if connection is not None:
        connection.status = False
        connection.last_checked = now
    db.commit()


# ---- hooks: authentication and status ----------------------------------------------


def authenticate(db: Session, connector_id: str, token: str) -> Authentication:
    """Check the token. The reason is for VizAI's log only; the agent is just told "rejected"."""
    try:
        parsed = UUID(connector_id)
    except ValueError:
        token_matches(token, _DUMMY_HASH)
        return Authentication(False, reason="malformed connector id")

    connector = db.get(ExternalConnectorModel, parsed)
    token_ok = token_matches(token, connector.token_hash if connector else _DUMMY_HASH)
    if connector is None or not token_ok:
        return Authentication(False, reason="unknown connector or wrong token")
    if connector.status == "revoked" or connector.revoked_at is not None:
        return Authentication(False, reason="connector revoked")
    connection = get_connection_for(db, parsed)
    if connection is None:
        # The datasource was deleted: the token must not stay usable.
        return Authentication(False, reason="its datasource was deleted")
    # Only claim to hold a schema that was really saved.
    known = connector.schema_hash if connection.db_schema and connector.schema_hash else ""
    return Authentication(True, known_schema_hash=known)


def mark_connected(
    db: Session, connector_id: str, *, dialect: str, agent_version: str, protocol_version: str
) -> None:
    connector = db.get(ExternalConnectorModel, UUID(connector_id))
    if connector is None or connector.status == "revoked":
        return
    now = _utcnow()
    connector.status = "online"
    connector.dialect = dialect
    connector.agent_version = agent_version
    connector.protocol_version = protocol_version
    connector.last_seen_at = now
    connection = get_connection_for(db, connector.id)
    if connection is not None:
        connection.status = True
        connection.last_checked = now
    db.commit()


def mark_disconnected(db: Session, connector_id: str, *, remaining_instances: int) -> None:
    connector = db.get(ExternalConnectorModel, UUID(connector_id))
    if connector is None:
        return
    now = _utcnow()
    connector.last_seen_at = now
    if remaining_instances == 0 and connector.status != "revoked":
        connector.status = "offline"
        connection = get_connection_for(db, connector.id)
        if connection is not None:
            connection.status = False
            connection.last_checked = now
    db.commit()


def reset_online_status(db: Session) -> None:
    """At startup nobody is connected yet, whatever the database says from the last run."""
    db.execute(update(ExternalConnectorModel).where(ExternalConnectorModel.status == "online").values(status="offline"))
    db.execute(
        update(DatabaseConnectionModel)
        .where(DatabaseConnectionModel.connection_mode == "connector", DatabaseConnectionModel.status.is_(True))
        .values(status=False)
    )
    db.commit()


# ---- hooks: the schema ----------------------------------------------------------------


def store_schema(db: Session, connector_id: str, payload: Any) -> StoredSchema:
    """Map the pushed schema to the direct flow's shape and save it on the connector's datasource.

    Saved exactly like extract_tables_in_background does: db_schema (JSON text) and ds_graph_json.
    Raises LookupError when the connector or its datasource no longer exists.
    """
    connector = db.get(ExternalConnectorModel, UUID(connector_id))
    connection = get_connection_for(db, UUID(connector_id)) if connector else None
    if connector is None or connection is None or connector.status == "revoked":
        raise LookupError("connector or its datasource no longer exists")

    structure = schema_mapper.map_schema(payload)
    graph = DSGraphBuilder(db_type=connection.db_type).build(structure)
    connection.db_schema = json.dumps(structure)
    connection.ds_graph_json = json.dumps(graph)
    connector.schema_hash = payload.schema_hash
    db.commit()
    logger.info(
        "saved connector schema connector_id=%s tables=%d hash=%s",
        connector_id, len(structure["tables"]), payload.schema_hash[:12],
    )
    return StoredSchema(table_count=len(structure["tables"]), schema_hash=payload.schema_hash)
