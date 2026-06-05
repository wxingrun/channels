import asyncio
from unittest.mock import patch

import pytest
from django.test import override_settings

from channels.generic.websocket import (
    AsyncJsonWebsocketConsumer,
    AsyncWebsocketConsumer,
    JsonWebsocketConsumer,
    WebsocketConsumer,
)
from channels.layers import get_channel_layer
from channels.sessions import SessionMiddlewareStack
from channels.testing import WebsocketCommunicator


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_websocket_consumer():
    """
    Tests that WebsocketConsumer is implemented correctly.
    """
    results = {}

    class TestConsumer(WebsocketConsumer):
        def connect(self):
            results["connected"] = True
            self.accept()

        def receive(self, text_data=None, bytes_data=None):
            results["received"] = (text_data, bytes_data)
            self.send(text_data=text_data, bytes_data=bytes_data)

        def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()

    # Test a normal connection
    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert "connected" in results
    # Test sending text
    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"
    assert results["received"] == ("hello", None)
    # Test sending bytes
    await communicator.send_to(bytes_data=b"w\0\0\0")
    response = await communicator.receive_from()
    assert response == b"w\0\0\0"
    assert results["received"] == (None, b"w\0\0\0")
    # Close out
    await communicator.disconnect()
    assert "disconnected" in results


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_default_no_heartbeat():
    """
    Tests that AsyncWebsocketConsumer does not start heartbeat when
    ping_interval is 0 (default).
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        async def connect(self):
            results["connected"] = True
            await self.accept()

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = text_data
            await self.send(text_data=text_data)

        async def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()
    assert app._ping_task is None

    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert app._ping_task is None

    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"

    await communicator.disconnect()
    assert "disconnected" in results


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_ping_heartbeat():
    """
    Tests that AsyncWebsocketConsumer sends ping frames at the configured
    interval when ping_interval > 0.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        ping_interval = 0.01

        async def connect(self):
            results["connected"] = True
            await self.accept()

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = text_data
            await self.send(text_data=text_data)

        async def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()

    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert app._ping_task is not None

    output = await communicator.receive_output(timeout=0.5)
    assert output["type"] == "websocket.ping"

    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"

    await communicator.disconnect()
    assert "disconnected" in results


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_on_ping_hook():
    """
    Tests that the on_ping hook is called when the client sends a ping frame.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        async def connect(self):
            await self.accept()

        async def on_ping(self):
            results["ping_received"] = True

        async def on_pong(self):
            results["pong_received"] = True

    app = TestConsumer()

    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected

    await communicator.send_input({"type": "websocket.ping"})
    assert results.get("ping_received") is True

    await communicator.send_input({"type": "websocket.pong"})
    assert results.get("pong_received") is True

    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_ping_timeout():
    """
    Tests that the connection is closed when ping_timeout is exceeded
    without receiving a pong response.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        ping_interval = 1
        ping_timeout = 2

        async def connect(self):
            await self.accept()

        async def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()

    communicator = WebsocketCommunicator(app, "/testws/")

    _real_sleep = asyncio.sleep
    sleep_count = 0

    async def controlled_sleep(delay):
        nonlocal sleep_count
        sleep_count += 1
        if delay == 0:
            await _real_sleep(0)

    with patch("asyncio.sleep", controlled_sleep):
        connected, _ = await communicator.connect()
        assert connected

        await _real_sleep(0)

    assert results.get("disconnected") == 4001


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_pong_resets_timeout():
    """
    Tests that receiving a pong frame cancels the pending timeout and
    prevents the connection from being closed.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        ping_interval = 1
        ping_timeout = 0.5

        async def connect(self):
            await self.accept()

        async def disconnect(self, code):
            results["disconnected"] = code

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = text_data
            await self.send(text_data=text_data)

    app = TestConsumer()

    communicator = WebsocketCommunicator(app, "/testws/")

    _real_sleep = asyncio.sleep

    async def controlled_sleep(delay):
        if delay == 0:
            await _real_sleep(0)

    with patch("asyncio.sleep", controlled_sleep):
        connected, _ = await communicator.connect()
        assert connected

        await _real_sleep(0)

        await communicator.send_input({"type": "websocket.pong"})

        await _real_sleep(0)

    assert "disconnected" not in results

    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"

    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_ping_cleanup_on_disconnect():
    """
    Tests that the ping task and timeout task are properly cancelled
    when the connection is disconnected.
    """
    class TestConsumer(AsyncWebsocketConsumer):
        ping_interval = 1

        async def connect(self):
            await self.accept()

    app = TestConsumer()

    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert app._ping_task is not None
    assert not app._ping_task.done()

    await communicator.disconnect()

    assert app._ping_task is None
    assert app._timeout_task is None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_ping_interval_zero_no_timeout():
    """
    Tests that when ping_interval is 0, even if ping_timeout is set,
    no heartbeat is started and the connection is not affected.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        ping_interval = 0
        ping_timeout = 5

        async def connect(self):
            results["connected"] = True
            await self.accept()

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = text_data
            await self.send(text_data=text_data)

        async def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()
    assert app._ping_task is None

    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert app._ping_task is None

    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"

    await communicator.disconnect()
    assert "disconnected" in results


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_ping_timeout_none():
    """
    Tests that when ping_timeout is None, the heartbeat sends pings
    but never times out.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        ping_interval = 0.01
        ping_timeout = None

        async def connect(self):
            results["connected"] = True
            await self.accept()

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = text_data
            await self.send(text_data=text_data)

        async def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()

    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert app._ping_task is not None

    output = await communicator.receive_output(timeout=0.5)
    assert output["type"] == "websocket.ping"

    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"

    await communicator.disconnect()
    assert "disconnected" in results


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_ping_inheritance():
    """
    Tests that AsyncJsonWebsocketConsumer inherits the ping/pong
    heartbeat functionality correctly.
    """
    results = {}

    class TestConsumer(AsyncJsonWebsocketConsumer):
        ping_interval = 0.01

        async def connect(self):
            results["connected"] = True
            await self.accept()

        async def receive_json(self, data=None):
            results["received"] = data
            await self.send_json(data)

        async def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()
    assert app.ping_interval == 0.01

    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert app._ping_task is not None

    output = await communicator.receive_output(timeout=0.5)
    assert output["type"] == "websocket.ping"

    await communicator.send_json_to({"hello": "world"})
    response = await communicator.receive_json_from()
    assert response == {"hello": "world"}

    await communicator.disconnect()
    assert "disconnected" in results


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_multiple_websocket_consumers_with_sessions():
    """
    Tests that multiple consumers use the correct scope when using
    SessionMiddleware.
    """

    class TestConsumer(WebsocketConsumer):
        def connect(self):
            self.accept()

        def receive(self, text_data=None, bytes_data=None):
            path = self.scope["path"]
            self.send(text_data=path)

    app = SessionMiddlewareStack(TestConsumer.as_asgi())

    # Create to communicators.
    communicator = WebsocketCommunicator(app, "/first/")
    second_communicator = WebsocketCommunicator(app, "/second/")

    connected, _ = await communicator.connect()
    assert connected
    connected, _ = await second_communicator.connect()
    assert connected

    # Test out of order
    await second_communicator.send_to(text_data="Echo Path")
    response = await second_communicator.receive_from()
    assert response == "/second/"

    await communicator.send_to(text_data="Echo Path")
    response = await communicator.receive_from()
    assert response == "/first/"

    # Close out
    await communicator.disconnect()
    await second_communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_websocket_consumer_subprotocol():
    """
    Tests that WebsocketConsumer correctly handles subprotocols.
    """

    class TestConsumer(WebsocketConsumer):
        def connect(self):
            assert self.scope["subprotocols"] == ["subprotocol1", "subprotocol2"]
            self.accept("subprotocol2")

    app = TestConsumer()

    # Test a normal connection with subprotocols
    communicator = WebsocketCommunicator(
        app, "/testws/", subprotocols=["subprotocol1", "subprotocol2"]
    )
    connected, subprotocol = await communicator.connect()
    assert connected
    assert subprotocol == "subprotocol2"


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_websocket_consumer_groups():
    """
    Tests that WebsocketConsumer adds and removes channels from groups.
    """
    results = {}

    class TestConsumer(WebsocketConsumer):
        groups = ["chat"]

        def receive(self, text_data=None, bytes_data=None):
            results["received"] = (text_data, bytes_data)
            self.send(text_data=text_data, bytes_data=bytes_data)

    app = TestConsumer()

    channel_layers_setting = {
        "default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}
    }
    with override_settings(CHANNEL_LAYERS=channel_layers_setting):
        communicator = WebsocketCommunicator(app, "/testws/")
        await communicator.connect()

        channel_layer = get_channel_layer()
        # Test that the websocket channel was added to the group on connect
        message = {"type": "websocket.receive", "text": "hello"}
        await channel_layer.group_send("chat", message)
        response = await communicator.receive_from()
        assert response == "hello"
        assert results["received"] == ("hello", None)
        # Test that the websocket channel was discarded from the group on disconnect
        await communicator.disconnect()
        assert channel_layer.groups == {}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer():
    """
    Tests that AsyncWebsocketConsumer is implemented correctly.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        async def connect(self):
            results["connected"] = True
            await self.accept()

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = (text_data, bytes_data)
            await self.send(text_data=text_data, bytes_data=bytes_data)

        async def disconnect(self, code):
            results["disconnected"] = code

    app = TestConsumer()

    # Test a normal connection
    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    assert "connected" in results
    # Test sending text
    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"
    assert results["received"] == ("hello", None)
    # Test sending bytes
    await communicator.send_to(bytes_data=b"w\0\0\0")
    response = await communicator.receive_from()
    assert response == b"w\0\0\0"
    assert results["received"] == (None, b"w\0\0\0")
    # Close out
    await communicator.disconnect()
    assert "disconnected" in results


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_subprotocol():
    """
    Tests that AsyncWebsocketConsumer correctly handles subprotocols.
    """

    class TestConsumer(AsyncWebsocketConsumer):
        async def connect(self):
            assert self.scope["subprotocols"] == ["subprotocol1", "subprotocol2"]
            await self.accept("subprotocol2")

    app = TestConsumer()

    # Test a normal connection with subprotocols
    communicator = WebsocketCommunicator(
        app, "/testws/", subprotocols=["subprotocol1", "subprotocol2"]
    )
    connected, subprotocol = await communicator.connect()
    assert connected
    assert subprotocol == "subprotocol2"


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_groups():
    """
    Tests that AsyncWebsocketConsumer adds and removes channels from groups.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        groups = ["chat"]

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = (text_data, bytes_data)
            await self.send(text_data=text_data, bytes_data=bytes_data)

    app = TestConsumer()

    channel_layers_setting = {
        "default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}
    }
    with override_settings(CHANNEL_LAYERS=channel_layers_setting):
        communicator = WebsocketCommunicator(app, "/testws/")
        await communicator.connect()

        channel_layer = get_channel_layer()
        # Test that the websocket channel was added to the group on connect
        message = {"type": "websocket.receive", "text": "hello"}
        await channel_layer.group_send("chat", message)
        response = await communicator.receive_from()
        assert response == "hello"
        assert results["received"] == ("hello", None)

        # Test that the websocket channel was discarded from the group on disconnect
        await communicator.disconnect()
        assert channel_layer.groups == {}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_websocket_consumer_specific_channel_layer():
    """
    Tests that AsyncWebsocketConsumer uses the specified channel layer.
    """
    results = {}

    class TestConsumer(AsyncWebsocketConsumer):
        channel_layer_alias = "testlayer"

        async def receive(self, text_data=None, bytes_data=None):
            results["received"] = (text_data, bytes_data)
            await self.send(text_data=text_data, bytes_data=bytes_data)

    app = TestConsumer()

    channel_layers_setting = {
        "testlayer": {"BACKEND": "channels.layers.InMemoryChannelLayer"}
    }
    with override_settings(CHANNEL_LAYERS=channel_layers_setting):
        communicator = WebsocketCommunicator(app, "/testws/")
        await communicator.connect()

        channel_layer = get_channel_layer("testlayer")
        # Test that the specific channel layer is retrieved
        assert channel_layer is not None

        channel_name = list(channel_layer.channels.keys())[0]
        message = {"type": "websocket.receive", "text": "hello"}
        await channel_layer.send(channel_name, message)
        response = await communicator.receive_from()
        assert response == "hello"
        assert results["received"] == ("hello", None)

        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_json_websocket_consumer():
    """
    Tests that JsonWebsocketConsumer is implemented correctly.
    """
    results = {}

    class TestConsumer(JsonWebsocketConsumer):
        def connect(self):
            self.accept()

        def receive_json(self, data=None):
            results["received"] = data
            self.send_json(data)

    app = TestConsumer()

    # Open a connection
    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    # Test sending
    await communicator.send_json_to({"hello": "world"})
    response = await communicator.receive_json_from()
    assert response == {"hello": "world"}
    assert results["received"] == {"hello": "world"}
    # Test sending bytes breaks it
    await communicator.send_to(bytes_data=b"w\0\0\0")
    with pytest.raises(ValueError):
        await communicator.wait()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_json_websocket_consumer():
    """
    Tests that AsyncJsonWebsocketConsumer is implemented correctly.
    """
    results = {}

    class TestConsumer(AsyncJsonWebsocketConsumer):
        async def connect(self):
            await self.accept()

        async def receive_json(self, data=None):
            results["received"] = data
            await self.send_json(data)

    app = TestConsumer()

    # Open a connection
    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected
    # Test sending
    await communicator.send_json_to({"hello": "world"})
    response = await communicator.receive_json_from()
    assert response == {"hello": "world"}
    assert results["received"] == {"hello": "world"}
    # Test sending bytes breaks it
    await communicator.send_to(bytes_data=b"w\0\0\0")
    with pytest.raises(ValueError):
        await communicator.wait()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_block_underscored_type_function_call():
    """
    Test that consumer prevent calling private functions as handler
    """

    class TestConsumer(AsyncWebsocketConsumer):
        channel_layer_alias = "testlayer"

        async def _my_private_handler(self, _):
            await self.send(text_data="should never be called")

    app = TestConsumer()

    channel_layers_setting = {
        "testlayer": {"BACKEND": "channels.layers.InMemoryChannelLayer"}
    }
    with override_settings(CHANNEL_LAYERS=channel_layers_setting):
        communicator = WebsocketCommunicator(app, "/testws/")
        await communicator.connect()

        channel_layer = get_channel_layer("testlayer")
        # Test that the specific channel layer is retrieved
        assert channel_layer is not None

        channel_name = list(channel_layer.channels.keys())[0]
        # Should block call to private functions handler and raise ValueError
        message = {"type": "_my_private_handler", "text": "hello"}
        await channel_layer.send(channel_name, message)
        with pytest.raises(
            ValueError, match=r"Malformed type in message \(leading underscore\)"
        ):
            await communicator.receive_from()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_block_leading_dot_type_function_call():
    """
    Test that consumer prevent calling private functions as handler
    """

    class TestConsumer(AsyncWebsocketConsumer):
        channel_layer_alias = "testlayer"

        async def _my_private_handler(self, _):
            await self.send(text_data="should never be called")

    app = TestConsumer()

    channel_layers_setting = {
        "testlayer": {"BACKEND": "channels.layers.InMemoryChannelLayer"}
    }
    with override_settings(CHANNEL_LAYERS=channel_layers_setting):
        communicator = WebsocketCommunicator(app, "/testws/")
        await communicator.connect()

        channel_layer = get_channel_layer("testlayer")
        # Test that the specific channel layer is retrieved
        assert channel_layer is not None

        channel_name = list(channel_layer.channels.keys())[0]
        # Should not replace dot by underscore and call private function (see
        # issue: #1430)
        message = {"type": ".my_private_handler", "text": "hello"}
        await channel_layer.send(channel_name, message)
        with pytest.raises(
            ValueError, match=r"Malformed type in message \(leading underscore\)"
        ):
            await communicator.receive_from()


@pytest.mark.parametrize("async_consumer", [False, True])
@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_accept_headers(async_consumer):
    """
    Tests that JsonWebsocketConsumer is implemented correctly.
    """

    class TestConsumer(WebsocketConsumer):
        def connect(self):
            self.accept(headers=[[b"foo", b"bar"]])

    class AsyncTestConsumer(AsyncWebsocketConsumer):
        async def connect(self):
            await self.accept(headers=[[b"foo", b"bar"]])

    app = AsyncTestConsumer() if async_consumer else TestConsumer()

    # Open a connection
    communicator = WebsocketCommunicator(app, "/testws/", spec_version="2.3")
    connected, _ = await communicator.connect()
    assert connected
    assert communicator.response_headers == [[b"foo", b"bar"]]


@pytest.mark.parametrize("async_consumer", [False, True])
@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_close_reason(async_consumer):
    """
    Tests that JsonWebsocketConsumer is implemented correctly.
    """

    class TestConsumer(WebsocketConsumer):
        def connect(self):
            self.accept()
            self.close(code=4007, reason="test reason")

    class AsyncTestConsumer(AsyncWebsocketConsumer):
        async def connect(self):
            await self.accept()
            await self.close(code=4007, reason="test reason")

    app = AsyncTestConsumer() if async_consumer else TestConsumer()

    # Open a connection
    communicator = WebsocketCommunicator(app, "/testws/", spec_version="2.3")
    connected, _ = await communicator.connect()
    msg = await communicator.receive_output()
    assert msg["type"] == "websocket.close"
    assert msg["code"] == 4007
    assert msg["reason"] == "test reason"


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_websocket_receive_with_none_text():
    """
    Tests that the receive method handles messages with None text data correctly.
    """

    class TestConsumer(WebsocketConsumer):
        def receive(self, text_data=None, bytes_data=None):
            if text_data:
                self.send(text_data="Received text: " + text_data)
            elif bytes_data:
                self.send(text_data=f"Received bytes of length: {len(bytes_data)}")

    app = TestConsumer()

    # Open a connection
    communicator = WebsocketCommunicator(app, "/testws/")
    connected, _ = await communicator.connect()
    assert connected

    # Simulate Hypercorn behavior
    # (both 'text' and 'bytes' keys present, but 'text' is None)
    await communicator.send_input(
        {
            "type": "websocket.receive",
            "text": None,
            "bytes": b"test data",
        }
    )
    response = await communicator.receive_output()
    assert response["type"] == "websocket.send"
    assert response["text"] == "Received bytes of length: 9"

    # Test with only 'bytes' key (simulating uvicorn/daphne behavior)
    await communicator.send_input({"type": "websocket.receive", "bytes": b"more data"})
    response = await communicator.receive_output()
    assert response["type"] == "websocket.send"
    assert response["text"] == "Received bytes of length: 9"

    # Test with valid text data
    await communicator.send_input(
        {"type": "websocket.receive", "text": "Hello, world!"}
    )
    response = await communicator.receive_output()
    assert response["type"] == "websocket.send"
    assert response["text"] == "Received text: Hello, world!"

    await communicator.disconnect()
