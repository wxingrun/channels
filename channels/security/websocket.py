from urllib.parse import urlparse

from django.conf import settings
from django.http.request import is_same_domain

from ..generic.websocket import AsyncWebsocketConsumer


WILDCARD_PORT = object()
INVALID_PORT = object()


class OriginValidator:
    """
    Validates that the incoming connection has an Origin header that
    is in an allowed list.
    """

    def __init__(self, application, allowed_origins):
        self.application = application
        self.allowed_origins = allowed_origins

    async def __call__(self, scope, receive, send):
        if scope["type"] != "websocket":
            raise ValueError(
                "You cannot use OriginValidator on a non-WebSocket connection"
            )
        parsed_origin = None
        for header_name, header_value in scope.get("headers", []):
            if header_name == b"origin":
                try:
                    parsed_origin = urlparse(header_value.decode("latin1"))
                except UnicodeDecodeError:
                    pass
        if self.valid_origin(parsed_origin):
            return await self.application(scope, receive, send)
        denier = WebsocketDenier()
        return await denier(scope, receive, send)

    def valid_origin(self, parsed_origin):
        """
        Checks parsed origin is None.

        Pass control to the validate_origin function.

        Returns ``True`` if validation function was successful, ``False`` otherwise.
        """
        if parsed_origin is None and "*" not in self.allowed_origins:
            return False
        return self.validate_origin(parsed_origin)

    def validate_origin(self, parsed_origin):
        """
        Validate the given origin for this site.

        Check than the origin looks valid and matches the origin pattern in
        specified list ``allowed_origins``. Any pattern begins with a scheme.
        After the scheme there must be a domain. Any domain beginning with a
        period corresponds to the domain and all its subdomains (for example,
        ``http://.example.com``). After the domain there must be a port,
        but it can be omitted. ``*`` matches anything and anything
        else must match exactly.

        Note. This function assumes that the given origin has a schema, domain
        and port, but port is optional.

        Returns ``True`` for a valid host, ``False`` otherwise.
        """
        return any(
            pattern == "*" or self.match_allowed_origin(parsed_origin, pattern)
            for pattern in self.allowed_origins
        )

    def match_allowed_origin(self, parsed_origin, pattern):
        """
        Returns ``True`` if the origin is either an exact match or a match
        to the wildcard pattern. Compares scheme, domain, port of origin and pattern.

        Any pattern can be begins with a scheme. After the scheme must be a domain,
        or just domain without scheme.
        Any domain beginning with a period corresponds to the domain and all
        its subdomains (for example, ``.example.com`` ``example.com``
        and any subdomain). Also with scheme (for example, ``http://.example.com``
        ``http://example.com``). After the domain there must be a port,
        but it can be omitted.

        Note. This function assumes that the given origin is either None, a
        schema-domain-port string, or just a domain string
        """
        if parsed_origin is None:
            return False

        parsed_pattern = urlparse(pattern.lower())
        origin_hostname = self.get_origin_hostname(parsed_origin)
        if origin_hostname is None:
            return False
        if not parsed_pattern.scheme:
            pattern_hostname = self.get_origin_hostname(urlparse("//" + pattern))
            return is_same_domain(origin_hostname, pattern_hostname or pattern)

        pattern_hostname = self.get_origin_hostname(parsed_pattern)
        if pattern_hostname is None:
            return False

        origin_port = self.get_origin_port(parsed_origin)
        pattern_port = self.get_origin_port(parsed_pattern, allow_wildcard=True)
        return (
            parsed_pattern.scheme == parsed_origin.scheme
            and self.match_allowed_port(origin_port, pattern_port)
            and is_same_domain(origin_hostname, pattern_hostname)
        )

    def get_origin_hostname(self, origin):
        try:
            return origin.hostname
        except ValueError:
            return None

    def match_allowed_port(self, origin_port, pattern_port):
        if origin_port is INVALID_PORT or pattern_port is INVALID_PORT:
            return False
        if pattern_port is WILDCARD_PORT:
            return origin_port is not None
        return origin_port == pattern_port

    def get_origin_port(self, origin, allow_wildcard=False):
        """
        Returns the origin.port or port for this schema by default.
        Otherwise, it returns None.
        """
        try:
            if origin.port is not None:
                return origin.port
        except ValueError:
            if allow_wildcard and self.origin_has_wildcard_port(origin):
                return WILDCARD_PORT
            return INVALID_PORT
        if origin.scheme == "http" or origin.scheme == "ws":
            return 80
        elif origin.scheme == "https" or origin.scheme == "wss":
            return 443
        else:
            return None

    def origin_has_wildcard_port(self, origin):
        netloc = origin.netloc.rsplit("@", 1)[-1]
        if not netloc:
            return False
        if netloc.startswith("["):
            closing = netloc.find("]")
            if closing == -1:
                return False
            return netloc[closing + 1 :] == ":*"
        if ":" not in netloc:
            return False
        return netloc.rsplit(":", 1)[1] == "*"


def AllowedHostsOriginValidator(application):
    """
    Factory function which returns an OriginValidator configured to use
    settings.ALLOWED_HOSTS.
    """
    allowed_hosts = settings.ALLOWED_HOSTS
    if settings.DEBUG and not allowed_hosts:
        allowed_hosts = ["localhost", "127.0.0.1", "[::1]"]
    return OriginValidator(application, allowed_hosts)


class WebsocketDenier(AsyncWebsocketConsumer):
    """
    Simple application which denies all requests to it.
    """

    async def connect(self):
        await self.close()
