import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from channels.worker import Worker


@pytest.mark.asyncio
async def test_worker_listens_and_receives_message():
    """
    Test that Worker can listen on specified channel and receive messages.
    """
    test_channel = "test.channel"
    test_message = {"type": "test.message"}

    mock_channel_layer = AsyncMock()
    mock_channel_layer.receive = AsyncMock(side_effect=[test_message])

    mock_application = Mock()

    worker = Worker(
        application=mock_application,
        channels=[test_channel],
        channel_layer=mock_channel_layer
    )

    worker.get_or_create_application_instance = Mock()
    mock_instance_queue = AsyncMock()
    worker.get_or_create_application_instance.return_value = mock_instance_queue

    async def stop_worker_after_receive():
        await asyncio.sleep(0.1)
        for task in asyncio.all_tasks():
            if task != asyncio.current_task():
                task.cancel()

    await asyncio.gather(
        worker.handle(),
        stop_worker_after_receive()
    )

    mock_channel_layer.receive.assert_called_once_with(test_channel)


@pytest.mark.asyncio
async def test_worker_distributes_message_to_application():
    """
    Test that Worker distributes received messages to the corresponding ASGI application.
    """
    test_channel = "test.channel"
    test_message = {"type": "test.message"}
    expected_scope = {"type": "channel", "channel": test_channel}

    mock_channel_layer = AsyncMock()
    mock_channel_layer.receive = AsyncMock(side_effect=[test_message])

    mock_application = Mock()

    worker = Worker(
        application=mock_application,
        channels=[test_channel],
        channel_layer=mock_channel_layer
    )

    mock_instance_queue = AsyncMock()
    worker.get_or_create_application_instance = Mock(return_value=mock_instance_queue)

    async def stop_worker_after_receive():
        await asyncio.sleep(0.1)
        for task in asyncio.all_tasks():
            if task != asyncio.current_task():
                task.cancel()

    await asyncio.gather(
        worker.handle(),
        stop_worker_after_receive()
    )

    worker.get_or_create_application_instance.assert_called_once_with(
        test_channel, expected_scope
    )
    mock_instance_queue.put.assert_called_once_with(test_message)


@pytest.mark.asyncio
async def test_worker_handles_no_type_message():
    """
    Test that Worker raises ValueError when receiving message without type.
    """
    test_channel = "test.channel"
    invalid_message = {"not_type": "test.message"}

    mock_channel_layer = AsyncMock()
    mock_channel_layer.receive = AsyncMock(side_effect=[invalid_message])

    mock_application = Mock()

    worker = Worker(
        application=mock_application,
        channels=[test_channel],
        channel_layer=mock_channel_layer
    )

    worker.get_or_create_application_instance = Mock()
    mock_instance_queue = AsyncMock()
    worker.get_or_create_application_instance.return_value = mock_instance_queue

    with pytest.raises(ValueError, match="Worker received message with no type."):
        await worker.listener(test_channel)


@pytest.mark.asyncio
async def test_worker_cleans_up_on_shutdown():
    """
    Test that Worker properly cleans up resources when shutdown.
    """
    test_channel = "test.channel"

    mock_channel_layer = AsyncMock()
    mock_channel_layer.receive = AsyncMock(side_effect=[asyncio.CancelledError()])

    mock_application = Mock()

    worker = Worker(
        application=mock_application,
        channels=[test_channel],
        channel_layer=mock_channel_layer
    )

    worker.get_or_create_application_instance = Mock()

    try:
        await worker.handle()
    except asyncio.CancelledError:
        pass

    mock_channel_layer.receive.assert_called_once_with(test_channel)


def test_worker_raises_error_without_channel_layer():
    """
    Test that Worker raises ValueError when channel layer is None.
    """
    with pytest.raises(ValueError, match="Channel layer is not valid"):
        Worker(
            application=Mock(),
            channels=["test.channel"],
            channel_layer=None
        )
