"""The /ws/progress socket. A connector's task can wait days for the client to install, so the
handler must notice the client leaving. A handler stuck on the queue hangs the server's shutdown."""

from __future__ import annotations

import asyncio

from fastapi import WebSocketDisconnect


class FakeSocket:
    def __init__(self):
        self.sent: list[dict] = []
        self.closed = False
        self.incoming: asyncio.Queue = asyncio.Queue()
        self.fail_send = False

    async def accept(self):
        pass

    async def receive(self):
        return await self.incoming.get()

    async def send_json(self, message):
        if self.fail_send:
            raise WebSocketDisconnect()
        self.sent.append(message)

    async def close(self):
        self.closed = True


def run(scenario):
    return asyncio.run(asyncio.wait_for(scenario(), 5))


def handler():
    from app.routes.backend import progress_queues, ws_progress

    return ws_progress, progress_queues


def test_messages_arrive_in_order_until_the_end_marker():
    ws_progress, queues = handler()

    async def scenario():
        queues["t1"] = queue = asyncio.Queue()
        socket = FakeSocket()
        for message in ({"type": "info"}, {"type": "completed"}, None):
            queue.put_nowait(message)
        await ws_progress(socket, "t1")
        return socket

    socket = run(scenario)
    queues.pop("t1", None)
    assert [m["type"] for m in socket.sent] == ["info", "completed"] and socket.closed


def test_the_client_leaving_ends_the_handler_while_waiting():
    """Before the fix this waited forever on an empty queue."""
    ws_progress, queues = handler()

    async def scenario():
        queues["t2"] = asyncio.Queue()
        socket = FakeSocket()
        task = asyncio.create_task(ws_progress(socket, "t2"))
        await asyncio.sleep(0.05)
        assert not task.done()
        socket.incoming.put_nowait({"type": "websocket.disconnect"})
        await task
        return socket

    assert run(scenario).closed
    assert "t2" in queues  # a reloaded page can subscribe again
    queues.pop("t2", None)


def test_nothing_is_lost_when_a_page_reloads():
    ws_progress, queues = handler()

    async def scenario():
        queues["t3"] = queue = asyncio.Queue()
        first = FakeSocket()
        first.incoming.put_nowait({"type": "websocket.disconnect"})
        await ws_progress(first, "t3")

        queue.put_nowait({"type": "info", "message": "connector connected"})
        queue.put_nowait(None)
        second = FakeSocket()
        await ws_progress(second, "t3")
        return second

    assert [m["type"] for m in run(scenario).sent] == ["info"]
    queues.pop("t3", None)


def test_a_message_from_the_client_is_not_mistaken_for_leaving():
    ws_progress, queues = handler()

    async def scenario():
        queues["t4"] = queue = asyncio.Queue()
        socket = FakeSocket()
        socket.incoming.put_nowait({"type": "websocket.receive", "text": "ping"})
        task = asyncio.create_task(ws_progress(socket, "t4"))
        await asyncio.sleep(0.05)
        queue.put_nowait({"type": "info"})
        queue.put_nowait(None)
        await task
        return socket

    assert [m["type"] for m in run(scenario).sent] == ["info"]
    queues.pop("t4", None)


def test_a_send_failure_ends_the_handler():
    ws_progress, queues = handler()

    async def scenario():
        queues["t5"] = queue = asyncio.Queue()
        socket = FakeSocket()
        socket.fail_send = True
        queue.put_nowait({"type": "info"})
        await ws_progress(socket, "t5")
        return socket

    assert run(scenario).closed
    queues.pop("t5", None)


def test_an_unknown_task_is_refused():
    ws_progress, _ = handler()

    async def scenario():
        socket = FakeSocket()
        await ws_progress(socket, "no-such-task")
        return socket

    socket = run(scenario)
    assert socket.sent == [{"type": "error", "message": "Invalid taskId"}] and socket.closed
