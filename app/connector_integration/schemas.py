"""Request and response models for the connector onboarding API."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class CreateConnectorRequest(BaseModel):
    connection_name: str = Field(..., min_length=1, max_length=200)
    consent_given: Optional[bool] = None


class InstallInfo(BaseModel):
    """What the install screen needs to build the client's container settings."""

    downloadUrl: str
    version: str
    image: str  # the connector image with its version, e.g. vizai/connector:0.7.0
    env: list[str] = []  # extra VIZAI_NAME=value settings to put before the command (local testing)


class CreateConnectorResponse(BaseModel):
    connectorId: str
    connectionId: str
    taskId: str
    gatewayAddress: str
    dialect: str
    # Shown once. VizAI keeps only a hash, so it cannot be shown again; rotate to get a new one.
    token: str
    install: InstallInfo


class RotateTokenResponse(BaseModel):
    connectorId: str
    gatewayAddress: str
    token: str
    install: InstallInfo


class InstanceInfo(BaseModel):
    instanceId: str
    agentVersion: str
    peer: str
    connectedAt: datetime
    lastSeenAt: datetime
    inflight: int


class ConnectorStatus(BaseModel):
    connectorId: str
    connectionId: Optional[str] = None
    projectId: str
    name: str
    status: str  # "online" | "offline" | "revoked"
    dialect: Optional[str] = None
    agentVersion: Optional[str] = None
    protocolVersion: Optional[str] = None
    hasSchema: bool = False
    schemaHash: Optional[str] = None
    lastSeenAt: Optional[datetime] = None
    createdAt: Optional[datetime] = None
    instances: list[InstanceInfo] = []


class ConnectorListResponse(BaseModel):
    connectors: list[ConnectorStatus]
