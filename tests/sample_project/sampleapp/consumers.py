from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer

from .models import Message


class LiveMessageConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        user = self.scope.get("user")
        if not user or not user.is_authenticated:
            await self.close(code=401)
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
        user = self.scope.get("user")
        if not user or not user.is_authenticated:
            await self.send_json({
                "code": 401,
                "error": "Unauthorized",
                "message": "Authentication required"
            })
            return

        if not isinstance(content, dict):
            await self.send_json({
                "code": 400,
                "error": "Invalid format",
                "message": "Message must be a JSON object"
            })
            return

        action = content.get("action")
        if action not in ("create", "delete"):
            await self.send_json({
                "code": 400,
                "error": "Invalid action",
                "message": "Only 'create' and 'delete' actions are allowed"
            })
            return

        if action == "create":
            title = content.get("title", "")
            text = content.get("message", "")
            
            if not isinstance(title, str) or not isinstance(text, str) or not title.strip() or not text.strip():
                await self.send_json({
                    "code": 400,
                    "error": "Invalid input",
                    "message": "Title and message cannot be empty"
                })
                return
                
            await self._create_message(title=title, text=text)

        elif action == "delete":
            msg_id = content.get("id")
            
            try:
                msg_id_int = int(msg_id)
                if msg_id_int <= 0:
                    raise ValueError
            except (ValueError, TypeError):
                await self.send_json({
                    "code": 400,
                    "error": "Invalid msg_id",
                    "message": "msg_id must be a positive integer"
                })
                return
                
            await self._delete_message(msg_id_int)

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
