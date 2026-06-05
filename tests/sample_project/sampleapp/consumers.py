from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer

from .models import Message


class LiveMessageConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        if not self.scope["user"].is_authenticated:
            await self.close(code=4401)
            return
        await self.channel_layer.group_add("live_message", self.channel_name)
        await self.accept()
        await self.send_current_state()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("live_message", self.channel_name)

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
        if not self.scope["user"].is_authenticated:
            await self.send_json(
                {
                    "code": 401,
                    "error": "Authentication required",
                    "message": "You must be logged in to perform this action.",
                }
            )
            return

        action = content.get("action", "create")

        if action == "create":
            title = content.get("title", "")
            text = content.get("message", "")
            if not title or not text:
                await self.send_json(
                    {
                        "code": 400,
                        "error": "Invalid request",
                        "message": "Title and message must not be empty.",
                    }
                )
                return
            await self._create_message(title=title, text=text)

        elif action == "delete":
            msg_id = content.get("id")
            if msg_id is None or isinstance(msg_id, bool) or not isinstance(msg_id, int) or msg_id <= 0:
                await self.send_json(
                    {
                        "code": 400,
                        "error": "Invalid msg_id",
                        "message": "msg_id must be a positive integer.",
                    }
                )
                return
            await self._delete_message(msg_id)

        else:
            await self.send_json(
                {
                    "code": 400,
                    "error": "Invalid action",
                    "message": "Only 'create' and 'delete' actions are allowed.",
                }
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
