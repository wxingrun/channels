from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from channels.testing import ChannelsLiveServerTestCase
from tests.sample_project.sampleapp.models import Message

from .selenium_mixin import SeleniumMixin

import pytest
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.urls import path

from channels.auth import AuthMiddlewareStack
from channels.db import database_sync_to_async
from channels.routing import URLRouter
from channels.testing import WebsocketCommunicator
from tests.sample_project.sampleapp.consumers import LiveMessageConsumer

User = get_user_model()


class TestSampleApp(SeleniumMixin, ChannelsLiveServerTestCase):
    serve_static = True

    def setUp(self):
        super().setUp()
        self.login()
        self.open_admin_message_page()

    def open_admin_message_page(self):
        self.open("/admin/sampleapp/message/")
        self.wait_for_websocket_connection()

    def _create_message(self, title="Test Title", message="Test Message"):
        return Message.objects.create(title=title, message=message)

    def _wait_for_exact_text(self, by, locator, exact, timeout=2):
        WebDriverWait(self.web_driver, timeout).until(
            lambda driver: driver.find_element(by, locator).text == str(exact)
        )

    def test_real_time_create_message(self):
        self.web_driver.switch_to.new_window("tab")
        tabs = self.web_driver.window_handles
        self.web_driver.switch_to.window(tabs[1])

        self.open_admin_message_page()
        titleInput = self.find_element(By.ID, "msgTitle")
        self.assertIsNotNone(titleInput, "Title input should be present")
        messageInput = self.find_element(By.ID, "msgTextArea")
        self.assertIsNotNone(messageInput, "Message input should be present")
        addMessageButton = self.find_element(By.ID, "sendBtn")
        self.assertIsNotNone(addMessageButton, "Send button should be present")
        titleInput.send_keys("Test Title")
        messageInput.send_keys("Test Message")
        addMessageButton.click()
        self._wait_for_exact_text(By.ID, "messageCount", 1)
        messageCount = self.find_element(By.ID, "messageCount")
        self.assertIsNotNone(messageCount, "Message count should be present")
        self.assertEqual(messageCount.text, "1")

        self.web_driver.switch_to.window(tabs[0])
        messageCount = self.find_element(By.ID, "messageCount")
        self.assertIsNotNone(messageCount, "Message count should be present")
        self.assertEqual(messageCount.text, "1")

    def test_real_time_delete_message(self):
        self._create_message()
        self.web_driver.refresh()
        self.wait_for_websocket_message_handled()

        messageCount = self.find_element(By.ID, "messageCount")
        self.assertIsNotNone(messageCount, "Message count should be present")
        self.assertEqual(messageCount.text, "1")

        self.web_driver.switch_to.new_window("tab")
        tabs = self.web_driver.window_handles
        self.web_driver.switch_to.window(tabs[1])

        self.open_admin_message_page()
        deleteButton = self.find_element(By.ID, "deleteBtn")
        self.assertIsNotNone(deleteButton, "Delete button should be present")
        deleteButton.click()
        self._wait_for_exact_text(By.ID, "messageCount", 0)

        messageCount = self.find_element(By.ID, "messageCount")
        self.assertIsNotNone(messageCount, "Message count should be present")
        self.assertEqual(messageCount.text, "0")

        self.web_driver.switch_to.window(tabs[0])
        messageCount = self.find_element(By.ID, "messageCount")
        self.assertIsNotNone(messageCount, "Message count should be present")
        self.assertEqual(messageCount.text, "0")


class ForceUserMiddleware:
    def __init__(self, app, user):
        self.app = app
        self.user = user

    async def __call__(self, scope, receive, send):
        scope["user"] = self.user
        return await self.app(scope, receive, send)


class TestableLiveMessageConsumer(LiveMessageConsumer):
    async def connect(self):
        await self.channel_layer.group_add("live_message", self.channel_name)
        await self.accept()
        await self.send_current_state()


def build_app(user=None, consumer_class=LiveMessageConsumer):
    url_router = URLRouter([
        path("ws/message/", consumer_class.as_asgi()),
    ])
    if user is not None:
        url_router = ForceUserMiddleware(url_router, user)
    return AuthMiddlewareStack(url_router)


@database_sync_to_async
def create_test_user():
    return User.objects.create_user(username="testuser", password="testpass")


def _format_error(code, error, message):
    return {"code": code, "error": error, "message": message}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_unauthenticated_connect():
    app = build_app(user=AnonymousUser())
    communicator = WebsocketCommunicator(app, "/ws/message/")
    connected, code = await communicator.connect()
    assert not connected
    assert code == 4401


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_unauthenticated_send_message():
    app = build_app(user=AnonymousUser(), consumer_class=TestableLiveMessageConsumer)
    communicator = WebsocketCommunicator(app, "/ws/message/")
    connected, _ = await communicator.connect()
    assert connected
    response = await communicator.receive_json_from()
    assert response["count"] == 0
    await communicator.send_json_to({"action": "create", "title": "Test", "message": "Test"})
    response = await communicator.receive_json_from()
    assert response == _format_error(401, "Authentication required", "You must be logged in to perform this action.")
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_invalid_action():
    user = await create_test_user()
    app = build_app(user=user)
    communicator = WebsocketCommunicator(app, "/ws/message/")
    connected, _ = await communicator.connect()
    assert connected
    response = await communicator.receive_json_from()
    await communicator.send_json_to({"action": "update", "title": "Test"})
    response = await communicator.receive_json_from()
    assert response == _format_error(400, "Invalid action", "Only 'create' and 'delete' actions are allowed.")
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_invalid_msg_id():
    user = await create_test_user()
    app = build_app(user=user)
    communicator = WebsocketCommunicator(app, "/ws/message/")
    connected, _ = await communicator.connect()
    assert connected
    response = await communicator.receive_json_from()
    for invalid_id in [None, -1, 0, "abc", ""]:
        await communicator.send_json_to({"action": "delete", "id": invalid_id})
        response = await communicator.receive_json_from()
        assert response == _format_error(400, "Invalid msg_id", "msg_id must be a positive integer.")
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_empty_title_or_message():
    user = await create_test_user()
    app = build_app(user=user)
    communicator = WebsocketCommunicator(app, "/ws/message/")
    connected, _ = await communicator.connect()
    assert connected
    response = await communicator.receive_json_from()
    await communicator.send_json_to({"action": "create", "title": "", "message": "Test"})
    response = await communicator.receive_json_from()
    assert response == _format_error(400, "Invalid request", "Title and message must not be empty.")
    await communicator.send_json_to({"action": "create", "title": "Test", "message": ""})
    response = await communicator.receive_json_from()
    assert response == _format_error(400, "Invalid request", "Title and message must not be empty.")
    await communicator.disconnect()
