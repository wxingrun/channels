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
    groups = None

    def __init__(self, *args, **kwargs):
        if self.groups is None:
            self.groups = []

    def _raise_invalid_channel_layer_error(self):
        raise InvalidChannelLayerError(
            "BACKEND is unconfigured or doesn't support groups"
        )

    def _build_accept_message(self, subprotocol=None, headers=None):
        message = {"type": "websocket.accept", "subprotocol": subprotocol}
        if headers:
            message["headers"] = list(headers)
        return message

    def _build_send_message(self, text_data=None, bytes_data=None):
        if text_data is not None:
            return {"type": "websocket.send", "text": text_data}
        if bytes_data is not None:
            return {"type": "websocket.send", "bytes": bytes_data}
        raise ValueError("You must pass one of bytes_data or text_data")

    def _build_close_message(self, code=None, reason=None):
        message = {"type": "websocket.close"}
        if code is not None and code is not True:
            message["code"] = code
        if reason:
            message["reason"] = reason
        return message

    def _get_receive_args(self, message):
        if message.get("text") is not None:
            return {"text_data": message["text"]}
        return {"bytes_data": message["bytes"]}

    def _sync_group_action(self, action, *args):
        try:
            async_to_sync(getattr(self.channel_layer, action))(*args)
        except AttributeError:
            self._raise_invalid_channel_layer_error()

    async def _async_group_action(self, action, *args):
        try:
            await getattr(self.channel_layer, action)(*args)
        except AttributeError:
            self._raise_invalid_channel_layer_error()

    def _sync_group_add(self, group):
        self._sync_group_action("group_add", group, self.channel_name)

    def _sync_group_discard(self, group):
        self._sync_group_action("group_discard", group, self.channel_name)

    def _sync_group_send(self, group, message):
        self._sync_group_action("group_send", group, message)

    async def _async_group_add(self, group):
        await self._async_group_action("group_add", group, self.channel_name)

    async def _async_group_discard(self, group):
        await self._async_group_action("group_discard", group, self.channel_name)

    async def _async_group_send(self, group, message):
        await self._async_group_action("group_send", group, message)

    def _sync_websocket_connect(self):
        for group in self.groups:
            self._sync_group_add(group)
        try:
            self.connect()
        except AcceptConnection:
            self.accept()
        except DenyConnection:
            self.close()

    async def _async_websocket_connect(self):
        for group in self.groups:
            await self._async_group_add(group)
        try:
            await self.connect()
        except AcceptConnection:
            await self.accept()
        except DenyConnection:
            await self.close()

    def _sync_websocket_receive(self, message):
        self.receive(**self._get_receive_args(message))

    async def _async_websocket_receive(self, message):
        await self.receive(**self._get_receive_args(message))

    def _sync_send(self, text_data=None, bytes_data=None, close=False):
        super().send(self._build_send_message(text_data=text_data, bytes_data=bytes_data))
        if close:
            self.close(close)

    async def _async_send(self, text_data=None, bytes_data=None, close=False):
        await super().send(
            self._build_send_message(text_data=text_data, bytes_data=bytes_data)
        )
        if close:
            await self.close(close)

    def _sync_close(self, code=None, reason=None):
        super().send(self._build_close_message(code=code, reason=reason))

    async def _async_close(self, code=None, reason=None):
        await super().send(self._build_close_message(code=code, reason=reason))

    def _sync_websocket_disconnect(self, message):
        for group in self.groups:
            self._sync_group_discard(group)
        self.disconnect(message["code"])
        raise StopConsumer()

    async def _async_websocket_disconnect(self, message):
        for group in self.groups:
            await self._async_group_discard(group)
        await self.disconnect(message["code"])
        await aclose_old_connections()
        raise StopConsumer()


class WebsocketConsumer(BaseWebsocketConsumer, SyncConsumer):
    """
    Base WebSocket consumer. Provides a general encapsulation for the
    WebSocket handling model that other applications can build on.
    """

    groups = None

    def websocket_connect(self, message):
        """
        Called when a WebSocket connection is opened.
        """
        self._sync_websocket_connect()

    def connect(self):
        self.accept()

    def accept(self, subprotocol=None, headers=None):
        """
        Accepts an incoming socket
        """
        super().send(self._build_accept_message(subprotocol=subprotocol, headers=headers))

    def websocket_receive(self, message):
        """
        Called when a WebSocket frame is received. Decodes it and passes it
        to receive().
        """
        self._sync_websocket_receive(message)

    def receive(self, text_data=None, bytes_data=None):
        """
        Called with a decoded WebSocket frame.
        """
        pass

    def send(self, text_data=None, bytes_data=None, close=False):
        """
        Sends a reply back down the WebSocket
        """
        self._sync_send(text_data=text_data, bytes_data=bytes_data, close=close)

    def close(self, code=None, reason=None):
        """
        Closes the WebSocket from the server end
        """
        self._sync_close(code=code, reason=reason)

    def websocket_disconnect(self, message):
        """
        Called when a WebSocket connection is closed. Base level so you don't
        need to call super() all the time.
        """
        self._sync_websocket_disconnect(message)

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

    groups = None

    async def websocket_connect(self, message):
        """
        Called when a WebSocket connection is opened.
        """
        await self._async_websocket_connect()

    async def connect(self):
        await self.accept()

    async def accept(self, subprotocol=None, headers=None):
        """
        Accepts an incoming socket
        """
        await super().send(
            self._build_accept_message(subprotocol=subprotocol, headers=headers)
        )

    async def websocket_receive(self, message):
        """
        Called when a WebSocket frame is received. Decodes it and passes it
        to receive().
        """
        await self._async_websocket_receive(message)

    async def receive(self, text_data=None, bytes_data=None):
        """
        Called with a decoded WebSocket frame.
        """
        pass

    async def send(self, text_data=None, bytes_data=None, close=False):
        """
        Sends a reply back down the WebSocket
        """
        await self._async_send(text_data=text_data, bytes_data=bytes_data, close=close)

    async def close(self, code=None, reason=None):
        """
        Closes the WebSocket from the server end
        """
        await self._async_close(code=code, reason=reason)

    async def websocket_disconnect(self, message):
        """
        Called when a WebSocket connection is closed. Base level so you don't
        need to call super() all the time.
        """
        await self._async_websocket_disconnect(message)

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
