"""Runs the connector gateway inside the backend, on the FastAPI event loop.

Off unless CONNECTOR_GATEWAY_ENABLED is true. The gateway's session registry lives in this
process, so the backend must run ONE uvicorn worker while it is on (plan, Phase 9).
`vizai_connector_gateway` is imported only when the gateway is enabled, so the backend runs
without it installed.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from app.connector_integration import service
from app.connector_integration.install_info import InvalidInstallSettings, build_install_info
from app.connector_integration.hooks_impl import VizAIConnectorHooks
from app.core.settings import settings

logger = logging.getLogger(__name__)

_gateway: Any = None


def gateway_enabled() -> bool:
    return bool(settings.CONNECTOR_GATEWAY_ENABLED)


def get_gateway() -> Any:
    """The running gateway, or None when the feature is off or not started."""
    return _gateway


def public_gateway_address() -> str:
    """The address clients put in their connector settings."""
    if settings.CONNECTOR_PUBLIC_GATEWAY_ADDRESS:
        return settings.CONNECTOR_PUBLIC_GATEWAY_ADDRESS
    return f"{settings.CONNECTOR_GATEWAY_HOST or 'localhost'}:{settings.CONNECTOR_GATEWAY_PORT or 50051}"


def install_info() -> dict:
    """The install screen's download URL, image, version and extra environment (validated: see install_info.py)."""
    return build_install_info(
        settings.CONNECTOR_DOWNLOAD_URL,
        settings.CONNECTOR_RELEASE_VERSION,
        settings.CONNECTOR_INSTALL_ENV,
        settings.CONNECTOR_IMAGE,
    )


def _config_environment() -> dict[str, str]:
    """GatewayConfig.from_env input: the process environment, with the values the backend's own
    settings (.env) define taking precedence."""
    env = dict(os.environ)
    for key in (
        "HOST", "PORT", "TLS_CERT_FILE", "TLS_KEY_FILE", "REQUIRE_TLS", "METRICS_PORT",
    ):
        value = getattr(settings, f"CONNECTOR_GATEWAY_{key}", None)
        if value not in (None, ""):
            env[f"CONNECTOR_GATEWAY_{key}"] = str(value)
    return env


async def start_gateway() -> Any:
    """Start the gateway if enabled. A failure to start is an error, not a silent skip: the
    operator asked for the connector flow."""
    global _gateway
    if not gateway_enabled():
        return None
    if _gateway is not None:
        return _gateway
    try:
        install_info()  # a bad operator setting stops startup instead of producing an unsafe command later
    except InvalidInstallSettings as exc:
        raise RuntimeError(f"invalid connector install settings: {exc}") from exc
    try:
        from vizai_connector_gateway import ConnectorGateway, GatewayConfig
    except ImportError as exc:
        raise RuntimeError(
            "CONNECTOR_GATEWAY_ENABLED is true but the vizai_connector_gateway package is not installed "
            "(pip install vizai-connector/gateway)"
        ) from exc

    from app.core.db import SessionLocal

    def reset() -> None:
        db = SessionLocal()
        try:
            service.reset_online_status(db)
        finally:
            db.close()

    await asyncio.to_thread(reset)

    gateway = ConnectorGateway(VizAIConnectorHooks(), GatewayConfig.from_env(_config_environment()))
    await gateway.start()
    _gateway = gateway
    logger.info("connector gateway listening on port %s", gateway.port)
    return gateway


async def stop_gateway() -> None:
    global _gateway
    gateway, _gateway = _gateway, None
    if gateway is not None:
        await gateway.stop()
        logger.info("connector gateway stopped")
