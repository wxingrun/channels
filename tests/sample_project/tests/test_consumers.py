import pytest
from django.contrib.auth import get_user_model
from django.test import override_settings
from asgiref.sync import sync_to_async

from channels.testing import WebsocketCommunicator
from tests.sample_project.sampleapp.consumers import LiveMessageConsumer
from tests.sample_project.sampleapp.models import Message


@pytest.fixture
def user():
    return get_user_model().objects.create_user(username="testuser", password="testpass")


@pytest.fixture
def channel_layers_setting():
    return {
        "default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}
    }


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_anonymous_user_cannot_connect():
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        connected, _ = await communicator.connect()
        assert not connected
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_authenticated_user_can_connect(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_anonymous_user_cannot_send_message():
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        connected, _ = await communicator.connect()
        assert not connected


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_invalid_action(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.receive_json_from()
        await communicator.send_json_to({"action": "invalid_action"})
        response = await communicator.receive_json_from()
        assert response["code"] == 400
        assert response["error"] == "Invalid action"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_invalid_msg_id(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.receive_json_from()
        await communicator.send_json_to({"action": "delete", "id": "invalid"})
        response = await communicator.receive_json_from()
        assert response["code"] == 400
        assert response["error"] == "Invalid msg_id"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_negative_msg_id(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.receive_json_from()
        await communicator.send_json_to({"action": "delete", "id": -1})
        response = await communicator.receive_json_from()
        assert response["code"] == 400
        assert response["error"] == "Invalid msg_id"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_zero_msg_id(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.receive_json_from()
        await communicator.send_json_to({"action": "delete", "id": 0})
        response = await communicator.receive_json_from()
        assert response["code"] == 400
        assert response["error"] == "Invalid msg_id"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_empty_msg_id(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.receive_json_from()
        await communicator.send_json_to({"action": "delete", "id": ""})
        response = await communicator.receive_json_from()
        assert response["code"] == 400
        assert response["error"] == "Invalid msg_id"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_authenticated_user_can_create_message(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.receive_json_from()
        await communicator.send_json_to({"action": "create", "title": "Test Title", "message": "Test Message"})
        await communicator.disconnect()
        assert await sync_to_async(Message.objects.count)() == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_authenticated_user_can_delete_message(user):
    with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}):
        await sync_to_async(Message.objects.create)(title="Test", message="Test Message")
        assert await sync_to_async(Message.objects.count)() == 1
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope['user'] = user
        connected, _ = await communicator.connect()
        assert connected
        await communicator.receive_json_from()
        await communicator.send_json_to({"action": "delete", "id": 1})
        await communicator.disconnect()
        assert await sync_to_async(Message.objects.count)() == 0
