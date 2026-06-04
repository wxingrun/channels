import asyncio
from unittest import mock

import pytest

from channels.worker import Worker


class MockChannelLayer:
    def __init__(self):
        self._queues = {}

    async def receive(self, channel):
        if channel not in self._queues:
            self._queues[channel] = asyncio.Queue()
        return await self._queues[channel].get()

    async def send(self, channel, message):
        if channel not in self._queues:
            self._queues[channel] = asyncio.Queue()
        await self._queues[channel].put(message)


@pytest.fixture
def mock_channel_layer():
    return MockChannelLayer()


@pytest.fixture
def dummy_application():
    async def app(scope, receive, send):
        pass

    return app


class TestWorkerInit:
    def test_raises_on_none_channel_layer(self, dummy_application):
        with pytest.raises(ValueError, match="Channel layer is not valid"):
            Worker(dummy_application, channels=["test-channel"], channel_layer=None)

    def test_sets_channels(self, dummy_application, mock_channel_layer):
        worker = Worker(
            dummy_application, channels=["ch1", "ch2"], channel_layer=mock_channel_layer
        )
        assert worker.channels == ["ch1", "ch2"]

    def test_sets_channel_layer(self, dummy_application, mock_channel_layer):
        worker = Worker(
            dummy_application, channels=["ch1"], channel_layer=mock_channel_layer
        )
        assert worker.channel_layer is mock_channel_layer

    def test_default_max_applications(self, dummy_application, mock_channel_layer):
        worker = Worker(
            dummy_application, channels=["ch1"], channel_layer=mock_channel_layer
        )
        assert worker.max_applications == 1000

    def test_custom_max_applications(self, dummy_application, mock_channel_layer):
        worker = Worker(
            dummy_application,
            channels=["ch1"],
            channel_layer=mock_channel_layer,
            max_applications=50,
        )
        assert worker.max_applications == 50


class TestWorkerListener:
    @pytest.mark.asyncio
    async def test_listener_receives_from_correct_channel(
        self, dummy_application, mock_channel_layer
    ):
        worker = Worker(
            dummy_application, channels=["test-channel"], channel_layer=mock_channel_layer
        )
        await mock_channel_layer.send("test-channel", {"type": "test.message"})

        mock_queue = mock.AsyncMock()
        with mock.patch.object(
            worker, "get_or_create_application_instance", return_value=mock_queue
        ):
            task = asyncio.create_task(worker.listener("test-channel"))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        mock_queue.put.assert_called_once_with({"type": "test.message"})

    @pytest.mark.asyncio
    async def test_listener_dispatches_message_to_app(self, mock_channel_layer):
        received = []

        async def app(scope, receive, send):
            message = await receive()
            received.append({"scope": scope, "message": message})

        worker = Worker(app, channels=["test-channel"], channel_layer=mock_channel_layer)
        await mock_channel_layer.send(
            "test-channel", {"type": "test.message", "data": "hello"}
        )

        task = asyncio.create_task(worker.listener("test-channel"))
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert len(received) == 1
        assert received[0]["scope"]["type"] == "channel"
        assert received[0]["scope"]["channel"] == "test-channel"
        assert received[0]["message"]["type"] == "test.message"
        assert received[0]["message"]["data"] == "hello"

    @pytest.mark.asyncio
    async def test_listener_raises_on_message_without_type(
        self, dummy_application, mock_channel_layer
    ):
        worker = Worker(
            dummy_application, channels=["test-channel"], channel_layer=mock_channel_layer
        )
        await mock_channel_layer.send("test-channel", {"data": "no-type"})

        with pytest.raises(ValueError, match="Worker received message with no type"):
            await worker.listener("test-channel")

    @pytest.mark.asyncio
    async def test_listener_creates_correct_scope(
        self, dummy_application, mock_channel_layer
    ):
        worker = Worker(
            dummy_application, channels=["test-channel"], channel_layer=mock_channel_layer
        )
        await mock_channel_layer.send("test-channel", {"type": "test.message"})

        mock_queue = mock.AsyncMock()
        with mock.patch.object(
            worker,
            "get_or_create_application_instance",
            return_value=mock_queue,
        ) as mock_get:
            task = asyncio.create_task(worker.listener("test-channel"))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

            mock_get.assert_called_with(
                "test-channel", {"type": "channel", "channel": "test-channel"}
            )

    @pytest.mark.asyncio
    async def test_listener_reuses_application_instance_for_same_channel(
        self, dummy_application, mock_channel_layer
    ):
        worker = Worker(
            dummy_application, channels=["test-channel"], channel_layer=mock_channel_layer
        )
        await mock_channel_layer.send("test-channel", {"type": "test.message.1"})
        await mock_channel_layer.send("test-channel", {"type": "test.message.2"})

        mock_queue = mock.AsyncMock()
        with mock.patch.object(
            worker,
            "get_or_create_application_instance",
            return_value=mock_queue,
        ) as mock_get:
            task = asyncio.create_task(worker.listener("test-channel"))
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert mock_get.call_count == 2
        assert mock_queue.put.call_count == 2


class TestWorkerHandle:
    @pytest.mark.asyncio
    async def test_handle_listens_on_all_channels(
        self, dummy_application, mock_channel_layer
    ):
        worker = Worker(
            dummy_application, channels=["ch1", "ch2"], channel_layer=mock_channel_layer
        )
        await mock_channel_layer.send("ch1", {"type": "test.msg1"})
        await mock_channel_layer.send("ch2", {"type": "test.msg2"})

        mock_queue = mock.AsyncMock()
        with mock.patch.object(
            worker, "get_or_create_application_instance", return_value=mock_queue
        ):
            task = asyncio.create_task(worker.handle())
            await asyncio.sleep(0.3)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, ValueError):
                pass

        assert mock_queue.put.call_count == 2

    @pytest.mark.asyncio
    async def test_handle_propagates_listener_error(
        self, dummy_application, mock_channel_layer
    ):
        worker = Worker(
            dummy_application, channels=["ch1"], channel_layer=mock_channel_layer
        )
        await mock_channel_layer.send("ch1", {"data": "no-type"})

        with pytest.raises(ValueError, match="Worker received message with no type"):
            await worker.handle()


class TestWorkerShutdown:
    @pytest.mark.asyncio
    async def test_clean_shutdown_cancels_listeners(
        self, dummy_application, mock_channel_layer
    ):
        worker = Worker(
            dummy_application, channels=["test-channel"], channel_layer=mock_channel_layer
        )

        task = asyncio.create_task(worker.handle())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, ValueError):
            pass

        assert task.done()
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_shutdown_releases_application_instances(self, mock_channel_layer):
        app_instances = []

        async def app(scope, receive, send):
            app_instances.append(scope)
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass

        worker = Worker(app, channels=["test-channel"], channel_layer=mock_channel_layer)
        await mock_channel_layer.send("test-channel", {"type": "test.message"})

        task = asyncio.create_task(worker.handle())
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, ValueError):
            pass

        assert task.done()
        assert len(app_instances) == 1
        assert app_instances[0]["type"] == "channel"
        assert app_instances[0]["channel"] == "test-channel"
