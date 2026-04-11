"""Unit tests for outbound URL validation (SSRF-style blocking)."""

import pytest

from outbound_http import validate_outbound_url


@pytest.mark.parametrize(
    "url,expect_block",
    [
        ("https://example.com/path", False),
        ("http://8.8.8.8/", False),
        ("http://127.0.0.1/", True),
        ("http://192.168.1.1/", True),
        ("http://10.0.0.1/", True),
        ("http://[::1]/", True),
        ("ftp://example.com/", True),
        ("file:///etc/passwd", True),
    ],
)
def test_validate_outbound_url_scheme_and_literals(url, expect_block):
    err = validate_outbound_url(url)
    if expect_block:
        assert err is not None
    else:
        assert err is None
