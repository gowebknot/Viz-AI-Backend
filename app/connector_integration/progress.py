"""Onboarding progress for the UI, over the existing /ws/progress/{task_id} socket.

Creating a connector returns a task_id. The UI opens that socket and sees the connector connect
and the schema arrive, with the same message types the direct flow uses (info, error,
graph_completed, completed). Later schema refreshes have no task and emit nothing.

Queues are asyncio objects: call these from the event loop only (the hooks and routes do).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from app.services.db_connection import progress_queues

logger = logging.getLogger(__name__)

# A UI that never opens the socket must not leak a queue forever.
_MAX_AGE_S = 24 * 3600
_tasks: dict[str, tuple[str, float]] = {}  # connector_id -> (task_id, registered at)


def register(connector_id: str, task_id: str) -> None:
    _expire_old()
    progress_queues.setdefault(task_id, asyncio.Queue())
    _tasks[connector_id] = (task_id, time.monotonic())


def emit(connector_id: str, message: dict[str, Any]) -> None:
    entry = _tasks.get(connector_id)
    queue = progress_queues.get(entry[0]) if entry else None
    if queue is not None:
        queue.put_nowait(message)


def finish(connector_id: str) -> None:
    """The first schema is saved: tell the UI it is done and close the socket's loop."""
    entry = _tasks.pop(connector_id, None)
    if entry is None:
        return
    task_id = entry[0]
    queue = progress_queues.get(task_id)
    if queue is not None:
        queue.put_nowait({"type": "completed", "taskId": task_id})
        queue.put_nowait(None)  # the socket's end-of-stream marker
        progress_queues.pop(task_id, None)


def forget(connector_id: str) -> None:
    entry = _tasks.pop(connector_id, None)
    if entry is not None:
        progress_queues.pop(entry[0], None)


def _expire_old() -> None:
    cutoff = time.monotonic() - _MAX_AGE_S
    for connector_id, (task_id, registered) in list(_tasks.items()):
        if registered < cutoff:
            _tasks.pop(connector_id, None)
            progress_queues.pop(task_id, None)
