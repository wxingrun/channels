from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from channels.testing import ChannelsLiveServerTestCase
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


from django.test import TransactionTestCase
from django.contrib.auth.models import AnonymousUser, User
from channels.testing import WebsocketCommunicator
from tests.sample_project.sampleapp.consumers import LiveMessageConsumer
from channels.db import database_sync_to_async

class DummyLiveMessageConsumer(LiveMessageConsumer):
    async def connect(self):
        await self.accept()

class TestLiveMessageConsumer(TransactionTestCase):
    async def test_unauth_connect(self):
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope["user"] = AnonymousUser()
        connected, close_code = await communicator.connect()
        self.assertFalse(connected)
        self.assertEqual(close_code, 401)

    @database_sync_to_async
    def create_user(self):
        return User.objects.create_user("testuser_ws", "test@test.com", "pass")

    async def get_auth_communicator(self):
        user = await self.create_user()
        communicator = WebsocketCommunicator(LiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope["user"] = user
        connected, _ = await communicator.connect()
        self.assertTrue(connected)
        # Consume the initial state broadcast
        await communicator.receive_json_from()
        return communicator

    async def test_unauth_send_message(self):
        communicator = WebsocketCommunicator(DummyLiveMessageConsumer.as_asgi(), "/ws/")
        communicator.scope["user"] = AnonymousUser()
        connected, _ = await communicator.connect()
        self.assertTrue(connected)
        
        await communicator.send_json_to({"action": "create", "title": "t", "message": "m"})
        response = await communicator.receive_json_from()
        self.assertEqual(response["code"], 401)
        self.assertEqual(response["error"], "Unauthorized")
        
        await communicator.disconnect()

    async def test_invalid_action(self):
        communicator = await self.get_auth_communicator()
        await communicator.send_json_to({"action": "invalid"})
        response = await communicator.receive_json_from()
        self.assertEqual(response["code"], 400)
        self.assertEqual(response["error"], "Invalid action")
        await communicator.disconnect()

    async def test_invalid_msg_id(self):
        communicator = await self.get_auth_communicator()
        
        invalid_ids = [None, "", "abc", -1, 0]
        for msg_id in invalid_ids:
            await communicator.send_json_to({"action": "delete", "id": msg_id})
            response = await communicator.receive_json_from()
            self.assertEqual(response["code"], 400)
            self.assertEqual(response["error"], "Invalid msg_id")
            
        await communicator.disconnect()

    async def test_empty_title_message(self):
        communicator = await self.get_auth_communicator()
        
        invalid_payloads = [
            {"action": "create", "title": "", "message": "msg"},
            {"action": "create", "title": "title", "message": ""},
            {"action": "create", "title": "   ", "message": "msg"},
        ]
        
        for payload in invalid_payloads:
            await communicator.send_json_to(payload)
            response = await communicator.receive_json_from()
            self.assertEqual(response["code"], 400)
            self.assertEqual(response["error"], "Invalid input")
            
        await communicator.disconnect()
