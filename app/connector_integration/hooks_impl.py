"""VizAI's implementation of the gateway's GatewayHooks.

The gateway is transport only and never touches VizAI's database. It calls these hooks, which
run on the application's event loop, so each database step runs in a worker thread with its own
Session (a blocking query here would stall every request and every connector).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session

from app.connector_integration import progress, schema_mapper, service

logger = logging.getLogger(__name__)


class VizAIConnectorHooks:
    def __init__(self, session_factory: Callable[[], Session] | None = None) -> None:
        if session_factory is None:
            from app.core.db import SessionLocal  # imported late: tests pass their own factory

            session_factory = SessionLocal
        self._session_factory = session_factory

    async def _run(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        def work() -> Any:
            db = self._session_factory()
            try:
                return function(db, *args, **kwargs)
            finally:
                db.close()

        return await asyncio.to_thread(work)

    async def verify_connector(self, connector_id: str, token: str) -> Any:
        from vizai_connector_gateway import ConnectorAuth

        # An exception here (database down) makes the gateway answer UNAVAILABLE, so the agent
        # retries instead of treating its token as revoked. Never log the token.
        result = await self._run(service.authenticate, connector_id, token)
        return ConnectorAuth(ok=result.ok, known_schema_hash=result.known_schema_hash, reason=result.reason)

    async def on_connected(self, connector_id: str, info: Any) -> None:
        await self._run(
            service.mark_connected,
            connector_id,
            dialect=info.dialect,
            agent_version=info.agent_version,
            protocol_version=info.protocol_version,
        )
        progress.emit(
            connector_id,
            {"type": "info", "message": f"Connector connected (agent {info.agent_version}, {info.dialect})"},
        )

    async def on_schema(self, connector_id: str, schema: Any) -> Any:
        from vizai_connector_gateway import SchemaVerdict

        problem = schema_mapper.validate(schema)
        if problem:
            logger.warning("rejected connector schema connector_id=%s reason=%s", connector_id, problem)
            progress.emit(connector_id, {"type": "error", "message": f"Schema rejected: {problem}"})
            return SchemaVerdict(accepted=False, schema_hash=schema.schema_hash, message=problem)

        progress.emit(
            connector_id,
            {"type": "started", "message": f"Received a schema with {len(schema.tables)} tables"},
        )
        try:
            stored = await self._run(service.store_schema, connector_id, schema)
        except LookupError as exc:
            progress.emit(connector_id, {"type": "error", "message": str(exc)})
            return SchemaVerdict(accepted=False, schema_hash=schema.schema_hash, message=str(exc))
        except Exception as exc:
            # Saving failed on VizAI's side: say so, and let the agent retry on its next refresh.
            logger.exception("saving connector schema failed connector_id=%s", connector_id)
            progress.emit(connector_id, {"type": "error", "message": "Saving the schema failed"})
            return SchemaVerdict(accepted=False, schema_hash=schema.schema_hash, message=f"saving failed ({type(exc).__name__})")

        progress.emit(connector_id, {"type": "info", "message": f"Successfully saved schema with {stored.table_count} tables"})
        progress.emit(connector_id, {"type": "graph_completed", "message": "Data source graph generated"})
        progress.finish(connector_id)
        return SchemaVerdict(accepted=True, schema_hash=stored.schema_hash, message=f"saved {stored.table_count} tables")

    async def on_disconnected(self, connector_id: str, instance_id: str, reason: str, remaining_instances: int) -> None:
        await self._run(service.mark_disconnected, connector_id, remaining_instances=remaining_instances)
