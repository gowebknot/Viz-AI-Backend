"""Onboarding API for connectors: create (token shown once), list, status, rotate the token,
revoke, refresh the schema.

Registered only when CONNECTOR_GATEWAY_ENABLED is true. Authorization uses the existing
check_access with ADD_DATASOURCE (mutations) or VIEW_DATASOURCE (reads) on the connector's
project. EDIT_/DELETE_DATASOURCE are deliberately not used: check_access treats those two as
not project-specific.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Response, status
from starlette.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

from app.connector_integration import gateway_host, progress, service
from app.connector_integration.schemas import (
    ConnectorListResponse,
    ConnectorStatus,
    CreateConnectorRequest,
    CreateConnectorResponse,
    InstanceInfo,
    RotateTokenResponse,
)
from app.core.db import get_db
from app.models.schema_models import DatabaseConnectionModel, ExternalConnectorModel
from app.utils.access import check_access
from app.utils.constants import Permissions as Permission
from app.utils.token_parser import get_current_user

logger = logging.getLogger(__name__)

connector_router = APIRouter(prefix="/api/v1/backend/connectors", tags=["connectors"])


def _no_store(response: Response) -> None:
    # The response carries a token: never let a browser or proxy cache it.
    response.headers["Cache-Control"] = "no-store"


def _user_id(token_payload: dict) -> UUID | None:
    try:
        return UUID(token_payload.get("sub"))
    except (TypeError, ValueError):
        return None


async def _load(db: Session, connector_id: UUID, token_payload: dict, permission: Permission) -> ExternalConnectorModel:
    """The connector, after checking the caller may act on its project (404 if it does not
    exist, 403 if the caller has no access to its project; connector ids are random UUIDs)."""
    connector = await run_in_threadpool(service.get_connector, db, connector_id)
    if connector is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connector not found")
    await check_access(db, token_payload, connector.project_id, permission.value)
    return connector


def _status_of(
    connector: ExternalConnectorModel, connection: DatabaseConnectionModel | None, *, with_instances: bool
) -> ConnectorStatus:
    gateway = gateway_host.get_gateway()
    instances: list[InstanceInfo] = []
    online = connector.status == "online"
    if gateway is not None:
        live = gateway.instances(str(connector.id))
        online = bool(live)
        if with_instances:
            def when(ts: float) -> datetime:
                return datetime.fromtimestamp(ts, tz=timezone.utc)

            instances = [
                InstanceInfo(
                    instanceId=i.instance_id,
                    agentVersion=i.agent_version,
                    peer=i.peer,
                    connectedAt=when(i.connected_at),
                    lastSeenAt=when(i.last_seen_at),
                    inflight=i.inflight,
                )
                for i in live
            ]
    state = "revoked" if connector.status == "revoked" else ("online" if online else "offline")
    return ConnectorStatus(
        connectorId=str(connector.id),
        connectionId=str(connection.id) if connection else None,
        projectId=str(connector.project_id),
        name=connector.name,
        status=state,
        dialect=connector.dialect,
        agentVersion=connector.agent_version,
        protocolVersion=connector.protocol_version,
        hasSchema=bool(connection and connection.db_schema),
        schemaHash=connector.schema_hash,
        lastSeenAt=connector.last_seen_at,
        createdAt=connector.created_at,
        instances=instances,
    )


@connector_router.post("/projects/{project_id}", response_model=CreateConnectorResponse, status_code=status.HTTP_201_CREATED)
async def create_connector(
    project_id: UUID,
    data: CreateConnectorRequest,
    response: Response,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """Create a connector and its datasource. The token is in this response only."""
    await check_access(db, token_payload, project_id, Permission.ADD_DATASOURCE.value)
    created = await run_in_threadpool(
        lambda: service.create_connector(
            db,
            project_id=project_id,
            user_id=_user_id(token_payload),
            name=data.connection_name,
            consent_given=data.consent_given,
        )
    )
    task_id = str(uuid4())
    progress.register(str(created.connector.id), task_id)
    _no_store(response)
    logger.info("connector created connector_id=%s project_id=%s", created.connector.id, project_id)
    return CreateConnectorResponse(
        connectorId=str(created.connector.id),
        connectionId=str(created.connection.id),
        taskId=task_id,
        gatewayAddress=gateway_host.public_gateway_address(),
        dialect=service.DEFAULT_DIALECT,
        token=created.token,
        install=gateway_host.install_info(),
    )


@connector_router.get("/projects/{project_id}", response_model=ConnectorListResponse)
async def list_connectors(
    project_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    await check_access(db, token_payload, project_id, Permission.VIEW_DATASOURCE.value)
    rows = await run_in_threadpool(service.list_connectors, db, project_id)
    return ConnectorListResponse(connectors=[_status_of(c, conn, with_instances=False) for c, conn in rows])


@connector_router.get("/{connector_id}", response_model=ConnectorStatus)
async def connector_status(
    connector_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    connector = await _load(db, connector_id, token_payload, Permission.VIEW_DATASOURCE)
    connection = await run_in_threadpool(service.get_connection_for, db, connector.id)
    return _status_of(connector, connection, with_instances=True)


@connector_router.post("/{connector_id}/rotate-token", response_model=RotateTokenResponse)
async def rotate_token(
    connector_id: UUID,
    response: Response,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """New token (shown once). The old one stops working and connected agents are disconnected."""
    connector = await _load(db, connector_id, token_payload, Permission.ADD_DATASOURCE)
    token = await run_in_threadpool(service.rotate_token, db, connector)
    gateway = gateway_host.get_gateway()
    if gateway is not None:
        gateway.disconnect(str(connector.id), "token rotated")
    _no_store(response)
    logger.info("connector token rotated connector_id=%s", connector.id)
    return RotateTokenResponse(
        connectorId=str(connector.id),
        gatewayAddress=gateway_host.public_gateway_address(),
        token=token,
        install=gateway_host.install_info(),
    )


@connector_router.post("/{connector_id}/revoke", response_model=ConnectorStatus)
async def revoke_connector(
    connector_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """Cut the connector off for good: its token stops working and its agents are disconnected."""
    connector = await _load(db, connector_id, token_payload, Permission.ADD_DATASOURCE)
    await run_in_threadpool(service.revoke, db, connector)
    gateway = gateway_host.get_gateway()
    if gateway is not None:
        gateway.disconnect(str(connector.id), "connector revoked")
    progress.forget(str(connector.id))
    logger.info("connector revoked connector_id=%s", connector.id)
    connection = await run_in_threadpool(service.get_connection_for, db, connector.id)
    return _status_of(connector, connection, with_instances=False)


@connector_router.post("/{connector_id}/refresh-schema", status_code=status.HTTP_202_ACCEPTED)
async def refresh_schema(
    connector_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """Ask a connected agent to rediscover its database. The new schema arrives through the
    gateway like the first one; check /connectors/{id} for hasSchema and schemaHash."""
    connector = await _load(db, connector_id, token_payload, Permission.ADD_DATASOURCE)
    gateway = gateway_host.get_gateway()
    if gateway is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Connector gateway is not running")
    if connector.status == "revoked":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Connector is revoked")
    try:
        instance_id = await gateway.refresh_schema(str(connector.id), reason="requested from VizAI")
    except Exception as exc:  # the gateway's errors carry their HTTP status
        code = getattr(exc, "http_status", 502)
        raise HTTPException(status_code=code, detail=getattr(exc, "message", "Could not reach the connector")) from exc
    return {"requested": True, "instanceId": instance_id}
