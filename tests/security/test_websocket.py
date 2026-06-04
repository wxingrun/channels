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
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["https://example.com:*"]
    )
    # Wildcard port matches default https port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://example.com")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Wildcard port matches custom port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://example.com:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Wildcard port does not match different scheme
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://example.com:8000")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Wildcard port does not match different domain
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://other.com:443")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Wildcard port with http scheme
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://example.com:*"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://example.com:3000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Wildcard port with ws scheme
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["ws://example.com:*"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"ws://example.com:8080")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_origin_validator_ipv6():
    # IPv6 exact match
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]:8000"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 exact match with different port should be denied
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:9000")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # IPv6 with wildcard port matches any port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]:*"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 wildcard port matches different port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:3000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 wildcard port does not match different scheme
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # IPv6 from ALLOWED_HOSTS style (no scheme, bracket notation)
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["[::1]"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # IPv6 no port, default http port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
