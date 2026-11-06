import base64

from app.connector_integration.tokens import TOKEN_PREFIX, connection_code, hash_token, new_token, token_matches


def test_tokens_are_random_prefixed_and_long():
    a, b = new_token(), new_token()
    assert a != b and a.startswith(TOKEN_PREFIX) and len(a) >= 40


def test_only_the_hash_is_kept_and_it_is_not_the_token():
    token = new_token()
    stored = hash_token(token)
    assert len(stored) == 64 and token not in stored
    assert token_matches(token, stored)


def test_a_wrong_or_empty_token_does_not_match():
    stored = hash_token(new_token())
    assert not token_matches(new_token(), stored)
    assert not token_matches("", stored)


def test_connection_code_holds_the_three_values_connect_sh_expects():
    token = new_token()
    code = connection_code("connect.example.com:443", "3809f5a8-aa96-457e-8d41-c08d0096b086", token)
    assert code.startswith("vzc1.") and "=" not in code
    encoded = code.removeprefix("vzc1.")
    decoded = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode()
    assert decoded.split("|") == ["connect.example.com:443", "3809f5a8-aa96-457e-8d41-c08d0096b086", token]
