from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer

from .models import Message


class LiveMessageConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        if not self._is_authenticated():
            await self.close(code=401)
            return

        await self.channel_layer.group_add("live_message", self.channel_name)
        await self.accept()
        await self.send_current_state()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("live_message", self.channel_name)

    def _is_authenticated(self):
        user = self.scope.get("user")
        return bool(user and getattr(user, "is_authenticated", False))

    def _validate_text_field(self, value):
        if not isinstance(value, str):
            return None

        value = value.strip()
        if not value:
            return None

        return value

    def _validate_message_id(self, value):
        if value is None or isinstance(value, bool):
            return None

        if isinstance(value, str):
            value = value.strip()
            if not value or not value.isdigit():
                return None
            value = int(value)
        elif not isinstance(value, int):
            return None

        if value <= 0:
            return None

        return value

    async def _send_error(self, code, error, message):
        await self.send_json({"code": code, "error": error, "message": message})

    @database_sync_to_async
    def _fetch_state(self):
        qs = Message.objects.order_by("-created")
        return {
            "count": qs.count(),
            "messages": list(qs.values("id", "title", "message")),
        }

    @database_sync_to_async
    def _create_message(self, title, text):
        Message.objects.create(title=title, message=text)

    @database_sync_to_async
    def _delete_message(self, msg_id):
        Message.objects.filter(id=msg_id).delete()

    async def receive_json(self, content):
        if not self._is_authenticated():
            await self._send_error(
                401,
                "Authentication required",
                "You must be logged in to perform this action",
            )
            return

        if not isinstance(content, dict):
            await self._send_error(
                400,
                "Invalid request",
                "A JSON object payload is required",
            )
            return

        action = content.get("action", "create")

        if action == "create":
            title = self._validate_text_field(content.get("title"))
            text = self._validate_text_field(content.get("message"))
            if title is None or text is None:
                await self._send_error(
                    400,
                    "Invalid message payload",
                    "Both 'title' and 'message' are required and cannot be empty",
                )
                return
            await self._create_message(title=title, text=text)

        elif action == "delete":
            msg_id = self._validate_message_id(content.get("id"))
            if msg_id is None:
                await self._send_error(
                    400,
                    "Invalid message id",
                    "The 'id' field must be a positive integer",
                )
                return
            await self._delete_message(msg_id)

        else:
            await self._send_error(
                400,
                "Invalid action",
                "Only 'create' and 'delete' actions are allowed",
            )
            return

        # After any action, rebroadcast current state
        await self.send_current_state()

    async def send_current_state(self):
        state = await self._fetch_state()
        await self.channel_layer.group_send(
            "live_message", {"type": "broadcast_message", **state}
        )

    async def broadcast_message(self, event):
        await self.send_json(
            {
                "count": event["count"],
                "messages": event["messages"],
            }
        )
