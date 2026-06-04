import asyncio

import pytest

from channels.worker import Worker


class MockChannelLayer:
    """Mock channel layer for testing Worker without external dependencies."""

    def __init__(self, messages_by_channel=None):
        self.messages_by_channel = messages_by_channel or {}
        self.receive_calls = []
        self._receive_counters = {}

    async def receive(self, channel):
        self.receive_calls.append(channel)
        messages = self.messages_by_channel.get(channel, [])
        counter = self._receive_counters.get(channel, 0)
        if counter < len(messages):
            msg = messages[counter]
            self._receive_counters[channel] = counter + 1
            return msg
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_worker_listens_on_specified_channel():
    """
    Test that Worker correctly listens on the specified channel and calls
    channel_layer.receive() with the correct channel name.
    """
    test_message = {"type": "test.message", "data": "hello"}
    channel_layer = MockChannelLayer(
        messages_by_channel={"test_channel": [test_message]}
    )

    received_messages = []
    message_received = asyncio.Event()

    async def test_app(scope, receive, send):
        message = await receive()
        received_messages.append(message)
        message_received.set()

    worker = Worker(
        application=test_app,
        channels=["test_channel"],
        channel_layer=channel_layer,
    )

    task = asyncio.ensure_future(worker.handle())
    await message_received.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert "test_channel" in channel_layer.receive_calls
    assert len(received_messages) == 1
    assert received_messages[0] == test_message


@pytest.mark.asyncio
async def test_worker_dispatches_messages_to_asgi_app():
    """
    Test that Worker correctly dispatches received messages to the
    corresponding ASGI application instance.
    """
    messages = [
        {"type": "msg.one", "value": 1},
        {"type": "msg.two", "value": 2},
        {"type": "msg.three", "value": 3},
    ]
    channel_layer = MockChannelLayer(
        messages_by_channel={"dispatch_channel": messages}
    )

    received_messages = []
    all_received = asyncio.Event()

    async def test_app(scope, receive, send):
        while True:
            try:
                message = await receive()
                received_messages.append(message)
                if len(received_messages) == len(messages):
                    all_received.set()
            except asyncio.CancelledError:
                break

    worker = Worker(
        application=test_app,
        channels=["dispatch_channel"],
        channel_layer=channel_layer,
    )

    task = asyncio.ensure_future(worker.handle())
    await all_received.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert received_messages == messages


@pytest.mark.asyncio
async def test_worker_multiple_channels():
    """
    Test that Worker can listen on multiple channels simultaneously and
    each channel's messages are properly dispatched.
    """
    channel_layer = MockChannelLayer(
        messages_by_channel={
            "channel_a": [{"type": "msg.a", "data": "A"}],
            "channel_b": [{"type": "msg.b", "data": "B"}],
        }
    )

    received_messages = []
    all_received = asyncio.Event()

    async def test_app(scope, receive, send):
        message = await receive()
        received_messages.append((scope["channel"], message))
        if len(received_messages) == 2:
            all_received.set()

    worker = Worker(
        application=test_app,
        channels=["channel_a", "channel_b"],
        channel_layer=channel_layer,
    )

    task = asyncio.ensure_future(worker.handle())
    await all_received.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    channels_seen = {ch for ch, _ in received_messages}
    assert channels_seen == {"channel_a", "channel_b"}


@pytest.mark.asyncio
async def test_worker_creates_application_instance_per_channel():
    """
    Test that Worker creates a distinct application instance for each
    unique channel it listens on.
    """
    channel_layer = MockChannelLayer(
        messages_by_channel={
            "channel_a": [{"type": "msg.1", "data": "a"}],
            "channel_b": [{"type": "msg.2", "data": "b"}],
        }
    )

    processed_count = 0
    all_processed = asyncio.Event()

    async def test_app(scope, receive, send):
        nonlocal processed_count
        await receive()
        processed_count += 1
        if processed_count == 2:
            all_processed.set()

    worker = Worker(
        application=test_app,
        channels=["channel_a", "channel_b"],
        channel_layer=channel_layer,
    )

    task = asyncio.ensure_future(worker.handle())
    await all_processed.wait()

    assert len(worker.application_instances) == 2
    assert "channel_a" in worker.application_instances
    assert "channel_b" in worker.application_instances

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_worker_shutdown_cleans_up_instances():
    """
    Test that Worker.application_instances are populated during message
    processing and that delete_application_instance() properly cancels
    the application future and removes the entry, simulating a clean
    shutdown resource release.
    """
    channel_layer = MockChannelLayer(
        messages_by_channel={"shutdown_ch": [{"type": "test.msg", "data": "x"}]}
    )

    message_processed = asyncio.Event()

    async def test_app(scope, receive, send):
        await receive()
        message_processed.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass

    worker = Worker(
        application=test_app,
        channels=["shutdown_ch"],
        channel_layer=channel_layer,
    )

    task = asyncio.ensure_future(worker.handle())
    await message_processed.wait()

    assert len(worker.application_instances) == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    for scope_id in list(worker.application_instances.keys()):
        worker.delete_application_instance(scope_id)

    assert len(worker.application_instances) == 0


@pytest.mark.asyncio
async def test_worker_rejects_none_channel_layer():
    """
    Test that Worker raises ValueError when initialized with channel_layer=None.
    """
    with pytest.raises(ValueError, match="Channel layer is not valid"):
        Worker(
            application=lambda s, r, send: None,
            channels=["test"],
            channel_layer=None,
        )


@pytest.mark.asyncio
async def test_worker_raises_on_message_without_type():
    """
    Test that Worker raises ValueError when a received message has no 'type' field.
    """
    channel_layer = MockChannelLayer(
        messages_by_channel={"bad_channel": [{"data": "no type here"}]}
    )

    async def test_app(scope, receive, send):
        pass

    worker = Worker(
        application=test_app,
        channels=["bad_channel"],
        channel_layer=channel_layer,
    )

    with pytest.raises(ValueError, match="Worker received message with no type"):
        await worker.handle()