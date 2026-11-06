"""Connector tokens: random, shown once, stored only as a SHA-256 hash.

A fast hash is right here (unlike for passwords): the token is 256 bits of randomness, so
there is nothing to brute-force, and every connect and reconnect must verify cheaply.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets

TOKEN_PREFIX = "vzc_"


def new_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def token_matches(token: str, expected_hash: str) -> bool:
    """Constant-time comparison of the token's hash with the stored hash."""
    return hmac.compare_digest(hash_token(token), expected_hash)


def connection_code(gateway_address: str, connector_id: str, token: str) -> str:
    """The one value a client gives the install script: gateway, connector id and token together.

    It contains the token, so like the token it is shown once and never stored or logged.
    connect.sh in the release decodes this format; change them together.
    """
    payload = f"{gateway_address}|{connector_id}|{token}".encode("utf-8")
    return "vzc1." + base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
