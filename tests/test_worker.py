import asyncio
from collections import defaultdict

import pytest

from channels.worker import Worker


class MockChannelLayer:
    def __init__(self):
        self.receive_calls = []
        self.queues = defaultdict(asyncio.Queue)

    async def receive(self, channel):
        self.receive_calls.append(channel)
        return await self.queues[channel].get()

    async def send(self, channel, message):
        await self.queues[channel].put(message)


@pytest.mark.asyncio
async def test_worker_listens_on_configured_channel_and_receives_message():
    channel_layer = MockChannelLayer()
    forwarded_messages = asyncio.Queue()
    message_forwarded = asyncio.Event()

    async def application(scope, receive, send):
        await forwarded_messages.put(await receive())
        message_forwarded.set()

    worker = Worker(
        application=application,
        channels=["test-channel"],
        channel_layer=channel_layer,
    )

    listener_task = asyncio.create_task(worker.listener("test-channel"))

    message = {"type": "test.message", "value": "payload"}
    await channel_layer.send("test-channel", message)

    await asyncio.wait_for(message_forwarded.wait(), timeout=1)

    assert channel_layer.receive_calls == ["test-channel"]
    assert await asyncio.wait_for(forwarded_messages.get(), timeout=1) == message

    listener_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await listener_task


@pytest.mark.asyncio
async def test_worker_dispatches_messages_to_asgi_application():
    channel_layer = MockChannelLayer()
    received = {}
    application_called = asyncio.Event()

    async def application(scope, receive, send):
        received["scope"] = scope
        received["message"] = await receive()
        application_called.set()

    worker = Worker(
        application=application,
        channels=["channel-one"],
        channel_layer=channel_layer,
    )

    listener_task = asyncio.create_task(worker.listener("channel-one"))

    message = {"type": "example.message", "text": "hello"}
    await channel_layer.send("channel-one", message)

    await asyncio.wait_for(application_called.wait(), timeout=1)

    assert received == {
        "scope": {"type": "channel", "channel": "channel-one"},
        "message": message,
    }

    listener_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await listener_task


@pytest.mark.asyncio
async def test_worker_shutdown_releases_application_resources():
    channel_layer = MockChannelLayer()
    application_started = asyncio.Event()

    async def application(scope, receive, send):
        application_started.set()
        await receive()

    worker = Worker(
        application=application,
        channels=["channel-one"],
        channel_layer=channel_layer,
    )

    worker.get_or_create_application_instance(
        "channel-one", {"type": "channel", "channel": "channel-one"}
    )

    await asyncio.wait_for(application_started.wait(), timeout=1)

    application_future = worker.application_instances["channel-one"]["future"]
    assert not application_future.done()

    worker.delete_application_instance("channel-one")
    await asyncio.sleep(0)

    assert "channel-one" not in worker.application_instances
    assert application_future.cancelled()
