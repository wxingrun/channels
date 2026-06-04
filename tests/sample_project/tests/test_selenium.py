from importlib import import_module
from uuid import uuid4

import pytest
from django.conf import settings
from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from channels.db import database_sync_to_async
from channels.testing import ChannelsLiveServerTestCase, WebsocketCommunicator
from tests.sample_project.config.asgi import application
from tests.sample_project.sampleapp.consumers import LiveMessageConsumer
from tests.sample_project.sampleapp.models import Message

from .selenium_mixin import SeleniumMixin


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


class ScopeUserMiddleware:
    def __init__(self, inner, user):
        self.inner = inner
        self.user = user

    async def __call__(self, scope, receive, send):
        scope = dict(scope)
        scope["user"] = self.user
        return await self.inner(scope, receive, send)


class AcceptedAnonymousLiveMessageConsumer(LiveMessageConsumer):
    async def connect(self):
        await self.accept()


WS_HEADERS = [(b"origin", b"http://localhost")]


@database_sync_to_async
def create_authenticated_headers():
    User = get_user_model()
    user = User.objects.create_user(
        username=f"ws-user-{uuid4().hex}",
        password="password",
    )
    session_store = import_module(settings.SESSION_ENGINE).SessionStore
    session = session_store()
    session[SESSION_KEY] = user._meta.pk.value_to_string(user)
    session[BACKEND_SESSION_KEY] = settings.AUTHENTICATION_BACKENDS[0]
    session[HASH_SESSION_KEY] = user.get_session_auth_hash()
    session.save()
    cookie = f"{settings.SESSION_COOKIE_NAME}={session.session_key}"
    return WS_HEADERS + [(b"cookie", cookie.encode("latin1"))]


@database_sync_to_async
def create_message(title="Test Title", message="Test Message"):
    return Message.objects.create(title=title, message=message)


@database_sync_to_async
def get_message_count():
    return Message.objects.count()


async def connect_authenticated_communicator():
    communicator = WebsocketCommunicator(
        application,
        "/ws/message/",
        headers=await create_authenticated_headers(),
    )
    connected, close_code = await communicator.connect()
    assert connected, close_code
    initial_state = await communicator.receive_json_from()
    return communicator, initial_state


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_websocket_rejects_unauthenticated_connections():
    communicator = WebsocketCommunicator(application, "/ws/message/", headers=WS_HEADERS)

    connected, close_code = await communicator.connect()

    assert not connected
    assert close_code == 401
    assert await communicator.receive_nothing()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_receive_json_rejects_unauthenticated_create_requests():
    communicator = WebsocketCommunicator(
        ScopeUserMiddleware(
            AcceptedAnonymousLiveMessageConsumer.as_asgi(),
            AnonymousUser(),
        ),
        "/ws/message/",
    )

    connected, _ = await communicator.connect()
    assert connected

    try:
        await communicator.send_json_to(
            {
                "action": "create",
                "title": "Blocked title",
                "message": "Blocked message",
            }
        )
        response = await communicator.receive_json_from()

        assert response == {
            "code": 401,
            "error": "Authentication required",
            "message": "You must be logged in to perform this action",
        }
        assert await get_message_count() == 0
    finally:
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_receive_json_rejects_invalid_action_without_closing_connection():
    communicator, initial_state = await connect_authenticated_communicator()

    try:
        assert initial_state == {"count": 0, "messages": []}

        await communicator.send_json_to({"action": "update"})
        response = await communicator.receive_json_from()

        assert response == {
            "code": 400,
            "error": "Invalid action",
            "message": "Only 'create' and 'delete' actions are allowed",
        }
        assert await get_message_count() == 0
        assert await communicator.receive_nothing()
    finally:
        await communicator.disconnect()


@pytest.mark.parametrize("msg_id", [None, "", "abc", -1, 0])
@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_receive_json_rejects_invalid_message_ids(msg_id):
    await create_message()
    communicator, initial_state = await connect_authenticated_communicator()

    try:
        assert initial_state["count"] == 1

        await communicator.send_json_to({"action": "delete", "id": msg_id})
        response = await communicator.receive_json_from()

        assert response == {
            "code": 400,
            "error": "Invalid message id",
            "message": "The 'id' field must be a positive integer",
        }
        assert await get_message_count() == 1
        assert await communicator.receive_nothing()
    finally:
        await communicator.disconnect()


@pytest.mark.parametrize(
    ("title", "message"),
    [("", "Valid message"), ("Valid title", ""), ("   ", "Valid message")],
)
@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_receive_json_rejects_empty_title_or_message(title, message):
    communicator, initial_state = await connect_authenticated_communicator()

    try:
        assert initial_state == {"count": 0, "messages": []}

        await communicator.send_json_to(
            {"action": "create", "title": title, "message": message}
        )
        response = await communicator.receive_json_from()

        assert response == {
            "code": 400,
            "error": "Invalid message payload",
            "message": "Both 'title' and 'message' are required and cannot be empty",
        }
        assert await get_message_count() == 0
        assert await communicator.receive_nothing()
    finally:
        await communicator.disconnect()
