"""VizAIConnectorHooks: what the gateway calls. Async, with a real database."""

from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import replace

import pytest

from app.connector_integration import progress, service
from app.connector_integration.hooks_impl import VizAIConnectorHooks
from app.core.db import SessionLocal
from vizai_connector_gateway import ConnectorInfo

from .payloads import mall_schema


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def hooks():
    return VizAIConnectorHooks(SessionLocal)


@pytest.fixture
def created(db, project, user):
    return service.create_connector(db, project_id=project.id, user_id=user.id, name="Acme Malls", consent_given=None)


def info(connector_id: str, instances: int = 1) -> ConnectorInfo:
    return ConnectorInfo(
        connector_id=connector_id, instance_id="i1", agent_version="0.5.0", protocol_version="1.0",
        dialect="postgres", capabilities=("schema.v1",), peer="10.0.0.5:4000", connected_instances=instances,
    )


def drain(task_id: str) -> list:
    queue = progress.progress_queues.get(task_id)
    out = []
    while queue is not None and not queue.empty():
        out.append(queue.get_nowait())
    return out


class TestVerify:
    def test_accepts_the_right_token(self, hooks, created):
        auth = run(hooks.verify_connector(str(created.connector.id), created.token))
        assert auth.ok and auth.known_schema_hash == ""

    def test_rejects_a_wrong_token_without_echoing_it(self, hooks, created):
        auth = run(hooks.verify_connector(str(created.connector.id), "vzc_wrong"))
        assert not auth.ok and "vzc_wrong" not in auth.reason

    def test_a_broken_database_raises_so_the_gateway_answers_unavailable(self, created):
        def broken():
            raise ConnectionError("database down")

        with pytest.raises(ConnectionError):
            run(VizAIConnectorHooks(broken).verify_connector(str(created.connector.id), created.token))

    def test_database_work_leaves_the_event_loop_free(self, hooks, created):
        """The hooks run on the app's event loop: a blocking query would stall every request."""
        main_thread = threading.get_ident()
        seen = []

        def factory():
            seen.append(threading.get_ident())
            return SessionLocal()

        async def scenario():
            ticks = 0

            async def ticker():
                nonlocal ticks
                while True:
                    await asyncio.sleep(0.001)
                    ticks += 1

            task = asyncio.create_task(ticker())
            await VizAIConnectorHooks(factory).verify_connector(str(created.connector.id), created.token)
            task.cancel()
            return ticks

        ticks = run(scenario())
        assert seen and all(t != main_thread for t in seen)
        assert ticks >= 1


class TestConnectAndDisconnect:
    def test_connected_marks_online_and_tells_the_ui(self, hooks, db, created):
        cid = str(created.connector.id)

        async def scenario():
            progress.register(cid, "task-1")
            await hooks.on_connected(cid, info(cid))

        run(scenario())
        db.refresh(created.connector)
        assert created.connector.status == "online" and created.connector.agent_version == "0.5.0"
        assert any("connected" in m["message"] for m in drain("task-1"))
        progress.forget(cid)

    def test_last_instance_leaving_marks_offline(self, hooks, db, created):
        cid = str(created.connector.id)
        run(hooks.on_connected(cid, info(cid)))
        run(hooks.on_disconnected(cid, "i1", "agent closed the stream", 1))
        db.refresh(created.connector)
        assert created.connector.status == "online"
        run(hooks.on_disconnected(cid, "i1", "agent closed the stream", 0))
        db.refresh(created.connector)
        assert created.connector.status == "offline"


class TestSchema:
    def test_accepted_schema_is_saved_and_the_ui_is_told_in_order(self, hooks, db, created):
        cid = str(created.connector.id)

        queue = {}

        async def scenario():
            progress.register(cid, "task-2")
            queue["q"] = progress.progress_queues["task-2"]  # finishing removes it from the registry
            return await hooks.on_schema(cid, mall_schema(cid, schema_hash="h1"))

        verdict = run(scenario())
        assert verdict.accepted and verdict.schema_hash == "h1"
        db.refresh(created.connection)
        assert len(json.loads(created.connection.db_schema)["tables"]) == 3
        assert created.connection.ds_graph_json

        messages = []
        while not queue["q"].empty():
            messages.append(queue["q"].get_nowait())
        kinds = [m["type"] if m else None for m in messages]
        assert kinds == ["started", "info", "graph_completed", "completed", None]
        assert messages[3]["taskId"] == "task-2"
        assert "task-2" not in progress.progress_queues  # cleaned up once finished

    def test_a_refresh_without_a_task_just_saves(self, hooks, db, created):
        cid = str(created.connector.id)
        assert run(hooks.on_schema(cid, mall_schema(cid))).accepted

    def test_a_bad_schema_is_refused_and_nothing_is_saved(self, hooks, db, created):
        cid = str(created.connector.id)

        async def scenario():
            progress.register(cid, "task-3")
            return await hooks.on_schema(cid, replace(mall_schema(cid), tables=()))

        verdict = run(scenario())
        assert not verdict.accepted and "no tables" in verdict.message
        db.refresh(created.connection)
        assert created.connection.db_schema is None
        assert [m["type"] for m in drain("task-3")] == ["error"]
        progress.forget(cid)

    def test_a_revoked_connector_cannot_push_a_schema(self, hooks, db, created):
        cid = str(created.connector.id)
        service.revoke(db, created.connector)
        verdict = run(hooks.on_schema(cid, mall_schema(cid)))
        assert not verdict.accepted

    def test_a_save_failure_is_reported_not_raised(self, created):
        class Boom(VizAIConnectorHooks):
            async def _run(self, function, *args, **kwargs):
                if function is service.store_schema:
                    raise RuntimeError("disk full")
                return await super()._run(function, *args, **kwargs)

        verdict = run(Boom(SessionLocal).on_schema(str(created.connector.id), mall_schema()))
        assert not verdict.accepted and "RuntimeError" in verdict.message and "disk full" not in verdict.message
