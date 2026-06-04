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
    """
    Tests that OriginValidator correctly handles wildcard ports.
    """
    # Test with wildcard port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://example.com:*"]
    )
    # Test with port 80
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://example.com:80")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Test with port 8000
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://example.com:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Test with port 443 (HTTPS but pattern is HTTP - should not match)
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"https://example.com:443")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Test with different domain
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://different.com:8000")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_origin_validator_ipv6():
    """
    Tests that OriginValidator correctly handles IPv6 addresses.
    """
    # Test with IPv6 address
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]:8000"]
    )
    # Test with exact IPv6 match
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Test with different port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8080")]
    )
    connected, _ = await communicator.connect()
    assert not connected
    await communicator.disconnect()
    # Test with IPv6 wildcard port
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://[::1]:*"]
    )
    # Test with any port
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://[::1]:8080")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
    # Test with IPv4 for comparison
    application = OriginValidator(
        AsyncWebsocketConsumer(), ["http://127.0.0.1:8000"]
    )
    communicator = WebsocketCommunicator(
        application, "/", headers=[(b"origin", b"http://127.0.0.1:8000")]
    )
    connected, _ = await communicator.connect()
    assert connected
    await communicator.disconnect()
