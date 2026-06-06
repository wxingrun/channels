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
    Base WebSocket consumer. Provides a general encapsulation for the
    WebSocket handling model that other applications can build on.
    """

    groups = None

    def __init__(self, *args, **kwargs):
        if self.groups is None:
            self.groups = []

    def _get_channel_layer_method(self, method_name):
        try:
            if self.channel_layer is None:
                raise AttributeError
            return getattr(self.channel_layer, method_name)
        except AttributeError:
            raise InvalidChannelLayerError(
                "BACKEND is unconfigured or doesn't support groups"
            )

    def _create_accept_message(self, subprotocol=None, headers=None):
        message = {"type": "websocket.accept", "subprotocol": subprotocol}
        if headers:
            message["headers"] = list(headers)
        return message

    def _create_send_message(self, text_data=None, bytes_data=None):
        if text_data is not None:
            return {"type": "websocket.send", "text": text_data}
        elif bytes_data is not None:
            return {"type": "websocket.send", "bytes": bytes_data}
        else:
            raise ValueError("You must pass one of bytes_data or text_data")

    def _create_close_message(self, code=None, reason=None):
        message = {"type": "websocket.close"}
        if code is not None and code is not True:
            message["code"] = code
        if reason:
            message["reason"] = reason
        return message


class WebsocketConsumer(SyncConsumer, BaseWebsocketConsumer):
    """
    Base WebSocket consumer. Provides a general encapsulation for the
    WebSocket handling model that other applications can build on.
    """

    def websocket_connect(self, message):
        """
        Called when a WebSocket connection is opened.
        """
        for group in self.groups:
            self.group_add(group, self.channel_name)
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
        super().send(self._create_accept_message(subprotocol, headers))

    def websocket_receive(self, message):
        """
        Called when a WebSocket frame is received. Decodes it and passes it
        to receive().
        """
        if message.get("text") is not None:
            self.receive(text_data=message["text"])
        else:
            self.receive(bytes_data=message["bytes"])

    def receive(self, text_data=None, bytes_data=None):
        """
        Called with a decoded WebSocket frame.
        """
        pass

    def send(self, text_data=None, bytes_data=None, close=False):
        """
        Sends a reply back down the WebSocket
        """
        super().send(self._create_send_message(text_data, bytes_data))
        if close:
            self.close(close)

    def close(self, code=None, reason=None):
        """
        Closes the WebSocket from the server end
        """
        super().send(self._create_close_message(code, reason))

    def websocket_disconnect(self, message):
        """
        Called when a WebSocket connection is closed. Base level so you don't
        need to call super() all the time.
        """
        for group in self.groups:
            self.group_discard(group, self.channel_name)
        self.disconnect(message["code"])
        raise StopConsumer()

    def disconnect(self, code):
        """
        Called when a WebSocket connection is closed.
        """
        pass

    def group_add(self, group, channel):
        """
        Adds the channel to a group.
        """
        async_to_sync(self._get_channel_layer_method("group_add"))(group, channel)

    def group_discard(self, group, channel):
        """
        Removes the channel from a group.
        """
        async_to_sync(self._get_channel_layer_method("group_discard"))(group, channel)

    def group_send(self, group, message):
        """
        Sends a message to a group.
        """
        async_to_sync(self._get_channel_layer_method("group_send"))(group, message)


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


class AsyncWebsocketConsumer(AsyncConsumer, BaseWebsocketConsumer):
    """
    Base WebSocket consumer, async version. Provides a general encapsulation
    for the WebSocket handling model that other applications can build on.
    """

    async def websocket_connect(self, message):
        """
        Called when a WebSocket connection is opened.
        """
        for group in self.groups:
            await self.group_add(group, self.channel_name)
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
        await super().send(self._create_accept_message(subprotocol, headers))

    async def websocket_receive(self, message):
        """
        Called when a WebSocket frame is received. Decodes it and passes it
        to receive().
        """
        if message.get("text") is not None:
            await self.receive(text_data=message["text"])
        else:
            await self.receive(bytes_data=message["bytes"])

    async def receive(self, text_data=None, bytes_data=None):
        """
        Called with a decoded WebSocket frame.
        """
        pass

    async def send(self, text_data=None, bytes_data=None, close=False):
        """
        Sends a reply back down the WebSocket
        """
        await super().send(self._create_send_message(text_data, bytes_data))
        if close:
            await self.close(close)

    async def close(self, code=None, reason=None):
        """
        Closes the WebSocket from the server end
        """
        await super().send(self._create_close_message(code, reason))

    async def websocket_disconnect(self, message):
        """
        Called when a WebSocket connection is closed. Base level so you don't
        need to call super() all the time.
        """
        for group in self.groups:
            await self.group_discard(group, self.channel_name)
        await self.disconnect(message["code"])
        await aclose_old_connections()
        raise StopConsumer()

    async def disconnect(self, code):
        """
        Called when a WebSocket connection is closed.
        """
        pass

    async def group_add(self, group, channel):
        """
        Adds the channel to a group.
        """
        await self._get_channel_layer_method("group_add")(group, channel)

    async def group_discard(self, group, channel):
        """
        Removes the channel from a group.
        """
        await self._get_channel_layer_method("group_discard")(group, channel)

    async def group_send(self, group, message):
        """
        Sends a message to a group.
        """
        await self._get_channel_layer_method("group_send")(group, message)


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
