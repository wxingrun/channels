import asyncio
from unittest import mock

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from channels.worker import Worker


class MockChannelLayer:
    def __init__(self):
        self.queues = {}
        
    async def receive(self, channel):
        if channel not in self.queues:
            self.queues[channel] = asyncio.Queue()
        return await self.queues[channel].get()

    async def send(self, channel, message):
        if channel not in self.queues:
            self.queues[channel] = asyncio.Queue()
        await self.queues[channel].put(message)


class MockApplication:
    def __init__(self):
        self.messages = []
        self.scopes = []

    async def __call__(self, scope, receive, send):
        self.scopes.append(scope)
        while True:
            try:
                message = await receive()
                self.messages.append(message)
                if message.get("type") == "stop":
                    break
            except asyncio.CancelledError:
                break


@pytest.mark.asyncio
async def test_worker_listen_and_dispatch():
    channel_layer = MockChannelLayer()
    app = MockApplication()
    
    worker = Worker(app, ["test-channel"], channel_layer)
    
    # Run the worker in a task
    task = asyncio.create_task(worker.handle())
    
    # Send a valid message
    await channel_layer.send("test-channel", {"type": "test.message", "text": "hello"})
    
    # Wait for the message to be processed
    for _ in range(10):
        if app.messages:
            break
        await asyncio.sleep(0.05)
        
    assert len(app.messages) > 0
    assert app.messages[0] == {"type": "test.message", "text": "hello"}
    assert len(app.scopes) == 1
    assert app.scopes[0]["type"] == "channel"
    assert app.scopes[0]["channel"] == "test-channel"
    
    # Clean up
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_worker_invalid_message_type():
    channel_layer = MockChannelLayer()
    app = MockApplication()
    
    worker = Worker(app, ["test-channel"], channel_layer)
    task = asyncio.create_task(worker.handle())
    
    # Send a message without a type
    await channel_layer.send("test-channel", {"text": "hello"})
    
    # The listener should raise a ValueError and handle() will re-raise it
    with pytest.raises(ValueError, match="Worker received message with no type."):
        await task


@pytest.mark.asyncio
async def test_worker_graceful_shutdown():
    channel_layer = MockChannelLayer()
    app = MockApplication()
    
    worker = Worker(app, ["test-channel"], channel_layer)
    task = asyncio.create_task(worker.handle())
    
    # Give it a moment to start listeners
    await asyncio.sleep(0.01)
    
    # Cancel the task to trigger graceful shutdown logic
    task.cancel()
    
    with pytest.raises(asyncio.CancelledError):
        await task


def test_worker_invalid_channel_layer():
    with pytest.raises(ValueError, match="Channel layer is not valid"):
        Worker(MockApplication(), ["test-channel"], None)


def test_runworker_command_parses_args():
    with mock.patch("channels.management.commands.runworker.Worker") as MockWorker, \
         mock.patch("channels.management.commands.runworker.get_channel_layer") as mock_get_channel_layer, \
         mock.patch("channels.management.commands.runworker.get_default_application") as mock_get_default_application:
         
        mock_get_channel_layer.return_value = "fake_layer"
        mock_get_default_application.return_value = "fake_app"
        
        call_command("runworker", "channel1", "channel2", layer="default")
        
        mock_get_channel_layer.assert_called_once_with("default")
        MockWorker.assert_called_once_with(
            application="fake_app",
            channels=["channel1", "channel2"],
            channel_layer="fake_layer",
        )
        MockWorker.return_value.run.assert_called_once()


def test_runworker_command_no_channel_layer():
    with mock.patch("channels.management.commands.runworker.get_channel_layer") as mock_get_channel_layer:
        mock_get_channel_layer.return_value = None
        
        with pytest.raises(CommandError, match="You do not have any CHANNEL_LAYERS configured."):
            call_command("runworker", "channel1")
