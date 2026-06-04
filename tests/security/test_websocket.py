import pytest

from channels.generic.websocket import AsyncWebsocketConsumer
from channels.security.websocket import OriginValidator
from channels.testing import WebsocketCommunicator


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_origin_validator():
    """
    Tests that OriginValidator correctly allows/denies connections.
    """
    # Make our test application
    application = OriginValidator(AsyncWebsocketConsumer(), ["allowed-domain.com"])
    # Test a normal connection
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://allowed-domain.com")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Test a bad connection
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://bad-domain.com")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Make our test application, bad pattern
    application = OriginValidator(AsyncWebsocketConsumer(), ["*.allowed-domain.com"])
    # Test a bad connection
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://allowed-domain.com")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Make our test application, good pattern
    application = OriginValidator(AsyncWebsocketConsumer(), [".allowed-domain.com"])
    # Test a normal connection
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://www.allowed-domain.com")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Make our test application, with scheme://domain[:port] for http
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://allowed-domain.com"]
    )
    # Test a normal connection
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://allowed-domain.com")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Test a bad connection
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://bad-domain.com:443")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Make our test application, with all hosts allowed
    application = OriginValidator(AsyncWebsocketConsumer(), ["*"])
    # Test a connection without any headers
    communicator = WebsocketCommunicator(application, "/", headers=[])
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Make our test application, with no hosts allowed
    application = OriginValidator(AsyncWebsocketConsumer(), [])
    # Test a connection without any headers
    communicator = WebsocketCommunicator(application, "/", headers=[])
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Test bug with subdomain and empty origin header
    application = OriginValidator(AsyncWebsocketConsumer(), [".allowed-domain.com"])
    communicator = WebsocketCommunicator(application, "/", headers=[(b"origin", b"")])
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Test bug with subdomain and invalid origin header
    application = OriginValidator(AsyncWebsocketConsumer(), [".allowed-domain.com"])
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"something-invalid")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_origin_validator_wildcard_port():
    # Wildcard port allows any port for the given scheme and domain
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["https://allowed-domain.com:*"]
    )
    # Default port (443) should be allowed
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://allowed-domain.com")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Non-default port should also be allowed
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://allowed-domain.com:8080")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Another non-default port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://allowed-domain.com:3000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Wrong scheme should still be denied
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://allowed-domain.com:8080")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Wrong domain should still be denied
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://other-domain.com:8080")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Exact port match still works (no wildcard)
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["https://allowed-domain.com:443"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://allowed-domain.com:443")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Different port denied without wildcard
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://allowed-domain.com:8080")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Wildcard port with http scheme
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://allowed-domain.com:*"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://allowed-domain.com:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_origin_validator_ipv6():
    # IPv6 with scheme and exact port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]:8000"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Wrong port should be denied
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:9000")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # IPv6 with scheme and default port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 scheme-less pattern matches any port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["[::1]"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 scheme-less pattern matches default port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 scheme-less pattern matches https too
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://[::1]:443")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 with wrong address should be denied
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::2]:8000")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # IPv6 with wildcard port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]:*"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 wildcard port with default port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 wildcard port with wrong scheme denied
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # IPv6 full address with port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[2001:db8::1]:8000"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[2001:db8::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
