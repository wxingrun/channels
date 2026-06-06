import json

from asgiref.sync import async_to_sync

from ..consumer import AsyncConsumer, SyncConsumer
from ..db import aclose_old_connections
from ..exceptions import (
    AcceptConnection,
    DenyConnection,
    InvalidChannelLayerError,
    StopConsumer,
)


class BaseWebsocketConsumer:
    """
    Base WebSocket consumer mixin. Provides shared logic for both sync and
    async WebSocket consumers, including message construction, data extraction,
    and initialization.
    """

    groups = None

    def __init__(self, *args, **kwargs):
        if self.groups is None:
            self.groups = []

    def _build_accept_message(self, subprotocol=None, headers=None):
        message = {"type": "websocket.accept", "subprotocol": subprotocol}
        if headers:
            message["headers"] = list(headers)
        return message

    def _build_close_message(self, code=None, reason=None):
        message = {"type": "websocket.close"}
        if code is not None and code is not True:
            message["code"] = code
        if reason:
            message["reason"] = reason
        return message

    def _build_send_message(self, text_data=None, bytes_data=None):
        if text_data is not None:
            return {"type": "websocket.send", "text": text_data}
        elif bytes_data is not None:
            return {"type": "websocket.send", "bytes": bytes_data}
        else:
            raise ValueError("You must pass one of bytes_data or text_data")

    def _get_receive_data(self, message):
        if message.get("text") is not None:
            return {"text_data": message["text"]}
        else:
            return {"bytes_data": message["bytes"]}


class WebsocketConsumer(BaseWebsocketConsumer, SyncConsumer):
    """
    Base WebSocket consumer. Provides a general encapsulation for the
    WebSocket handling model that other applications can build on.
    """

    def websocket_connect(self, message):
        """
        Called when a WebSocket connection is opened.
        """
        try:
            for group in self.groups:
                async_to_sync(self.channel_layer.group_add)(group, self.channel_name)
        except AttributeError:
            raise InvalidChannelLayerError(
                "BACKEND is unconfigured or doesn't support groups"
            )
        try:
            self.connect()
        except AcceptConnection:
            self.accept()
        except DenyConnection:
            self.close()

    def connect(self):
        self.accept()

    def accept(self, subprotocol=None, headers=None):
        """
        Accepts an incoming socket
        """
        super().send(self._build_accept_message(subprotocol, headers))

    def websocket_receive(self, message):
        """
        Called when a WebSocket frame is received. Decodes it and passes it
        to receive().
        """
        self.receive(**self._get_receive_data(message))

    def receive(self, text_data=None, bytes_data=None):
        """
        Called with a decoded WebSocket frame.
        """
        pass

    def send(self, text_data=None, bytes_data=None, close=False):
        """
        Sends a reply back down the WebSocket
        """
        super().send(self._build_send_message(text_data, bytes_data))
        if close:
            self.close(close)

    def close(self, code=None, reason=None):
        """
        Closes the WebSocket from the server end
        """
        super().send(self._build_close_message(code, reason))

    def websocket_disconnect(self, message):
        """
        Called when a WebSocket connection is closed. Base level so you don't
        need to call super() all the time.
        """
        try:
            for group in self.groups:
                async_to_sync(self.channel_layer.group_discard)(
                    group, self.channel_name
                )
        except AttributeError:
            raise InvalidChannelLayerError(
                "BACKEND is unconfigured or doesn't support groups"
            )
        self.disconnect(message["code"])
        raise StopConsumer()

    def disconnect(self, code):
        """
        Called when a WebSocket connection is closed.
        """
        pass


class JsonWebsocketConsumer(WebsocketConsumer):
    """
    Variant of WebsocketConsumer that automatically JSON-encodes and decodes
    messages as they come in and go out. Expects everything to be text; will
    error on binary data.
    """

    def receive(self, text_data=None, bytes_data=None, **kwargs):
        if text_data:
            self.receive_json(self.decode_json(text_data), **kwargs)
        else:
            raise ValueError("No text section for incoming WebSocket frame!")

    def receive_json(self, content, **kwargs):
        """
        Called with decoded JSON content.
        """
        pass

    def send_json(self, content, close=False):
        """
        Encode the given content as JSON and send it to the client.
        """
        super().send(text_data=self.encode_json(content), close=close)

    @classmethod
    def decode_json(cls, text_data):
        return json.loads(text_data)

    @classmethod
    def encode_json(cls, content):
        return json.dumps(content)


class AsyncWebsocketConsumer(BaseWebsocketConsumer, AsyncConsumer):
    """
    Base WebSocket consumer, async version. Provides a general encapsulation
    for the WebSocket handling model that other applications can build on.
    """

    async def websocket_connect(self, message):
        """
        Called when a WebSocket connection is opened.
        """
        try:
            for group in self.groups:
                await self.channel_layer.group_add(group, self.channel_name)
        except AttributeError:
            raise InvalidChannelLayerError(
                "BACKEND is unconfigured or doesn't support groups"
            )
        try:
            await self.connect()
        except AcceptConnection:
            await self.accept()
        except DenyConnection:
            await self.close()

    async def connect(self):
        await self.accept()

    async def accept(self, subprotocol=None, headers=None):
        """
        Accepts an incoming socket
        """
        await super().send(self._build_accept_message(subprotocol, headers))

    async def websocket_receive(self, message):
        """
        Called when a WebSocket frame is received. Decodes it and passes it
        to receive().
        """
        await self.receive(**self._get_receive_data(message))

    async def receive(self, text_data=None, bytes_data=None):
        """
        Called with a decoded WebSocket frame.
        """
        pass

    async def send(self, text_data=None, bytes_data=None, close=False):
        """
        Sends a reply back down the WebSocket
        """
        await super().send(self._build_send_message(text_data, bytes_data))
        if close:
            await self.close(close)

    async def close(self, code=None, reason=None):
        """
        Closes the WebSocket from the server end
        """
        await super().send(self._build_close_message(code, reason))

    async def websocket_disconnect(self, message):
        """
        Called when a WebSocket connection is closed. Base level so you don't
        need to call super() all the time.
        """
        try:
            for group in self.groups:
                await self.channel_layer.group_discard(group, self.channel_name)
        except AttributeError:
            raise InvalidChannelLayerError(
                "BACKEND is unconfigured or doesn't support groups"
            )
        await self.disconnect(message["code"])
        await aclose_old_connections()
        raise StopConsumer()

    async def disconnect(self, code):
        """
        Called when a WebSocket connection is closed.
        """
        pass


class AsyncJsonWebsocketConsumer(AsyncWebsocketConsumer):
    """
    Variant of AsyncWebsocketConsumer that automatically JSON-encodes and decodes
    messages as they come in and go out. Expects everything to be text; will
    error on binary data.
    """

    async def receive(self, text_data=None, bytes_data=None, **kwargs):
        if text_data:
            await self.receive_json(await self.decode_json(text_data), **kwargs)
        else:
            raise ValueError("No text section for incoming WebSocket frame!")

    async def receive_json(self, content, **kwargs):
        """
        Called with decoded JSON content.
        """
        pass

    async def send_json(self, content, close=False):
        """
        Encode the given content as JSON and send it to the client.
        """
        await super().send(text_data=await self.encode_json(content), close=close)

    @classmethod
    async def decode_json(cls, text_data):
        return json.loads(text_data)

    @classmethod
    async def encode_json(cls, content):
        return json.dumps(content)
