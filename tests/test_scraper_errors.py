"""Unit tests for crawl error row helpers in ``scraper``."""

from urllib.robotparser import RobotFileParser

import scraper


def test_failure_class_for_message_timeout():
    assert scraper._failure_class_for_message(
        "Timeout (attempt 1/4)", "fetch_failed",
    ) == "timeout"


def test_failure_class_for_message_ssl():
    assert scraper._failure_class_for_message(
        "SSLError: certificate verify failed", "fetch_failed",
    ) == "ssl"


def test_format_robots_rule_hint_disallow():
    rp = RobotFileParser()
    rp.set_url("https://example.com/robots.txt")
    rp.parse(
        [
            "User-agent: *",
            "Disallow: /private",
        ],
    )
    hint = scraper._format_robots_rule_hint(
        "https://example.com/private/secret",
        "TestBot/1.0",
        rp,
    )
    assert "Disallow" in hint
    assert "/private" in hint


def test_error_row_base_includes_url_alias():
    row = scraper._error_row_base(
        requested_url="https://a.com/x",
        final_url="https://a.com/y",
        referrer="https://a.com/",
        depth=2,
        error_type="fetch_failed",
        message="oops",
        http_status=404,
        content_type="text/html",
        failure_class="http_error",
        redirect_count=1,
        last_redirect_url="https://a.com/z",
        attempt_number=2,
        robots_txt_rule="",
        worker_id=3,
    )
    assert row["url"] == "https://a.com/x"
    assert row["final_url"] == "https://a.com/y"
    assert row["worker_id"] == 3
