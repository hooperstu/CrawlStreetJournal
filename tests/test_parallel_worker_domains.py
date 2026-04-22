"""Parallel crawl scheduling: prefer distinct hostnames across workers."""

import threading

import scraper
from config import CrawlConfig


def test_project_has_multiple_seed_hostnames():
    cfg = CrawlConfig.from_module()
    assert scraper._project_has_multiple_seed_hostnames(
        cfg, ["https://a.com/", "https://b.com/"],
    )
    assert not scraper._project_has_multiple_seed_hostnames(
        cfg, ["https://a.com/1", "https://a.com/2"],
    )


def test_queue_has_multiple_nonempty_hostnames():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/1", "", 0, is_seed=True)
    assert not scraper._queue_has_multiple_nonempty_hostnames(pq)
    pq.push("https://b.com/1", "", 0, is_seed=True)
    assert scraper._queue_has_multiple_nonempty_hostnames(pq)


def test_try_pop_respecting_busy_hosts_skips_busy_when_multiple_origins():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/1", "", 0, is_seed=True)
    pq.push("https://b.com/1", "", 0, is_seed=True)
    lock = threading.Lock()
    busy = {"a.com"}
    out = pq.try_pop_respecting_busy_hosts(
        lock, busy, enforce_distinct_hosts=True, strict_same_host_exclusion=False,
    )
    assert out is not None
    url, _ref, _depth, reserved = out
    assert "b.com" in url
    assert reserved == "b.com"
    assert "b.com" in busy


def test_try_pop_respecting_busy_hosts_strict_waits_when_only_busy_host_queued():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/1", "", 0, is_seed=True)
    pq.push("https://a.com/2", "", 1, is_seed=False)
    lock = threading.Lock()
    busy = {"a.com"}
    out = pq.try_pop_respecting_busy_hosts(
        lock, busy, enforce_distinct_hosts=True, strict_same_host_exclusion=True,
    )
    assert out is None


def test_try_pop_respecting_busy_hosts_legacy_single_host_queue_ignores_busy():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/1", "", 0, is_seed=True)
    pq.push("https://a.com/2", "", 1, is_seed=False)
    lock = threading.Lock()
    busy = {"a.com"}
    out = pq.try_pop_respecting_busy_hosts(
        lock, busy, enforce_distinct_hosts=True, strict_same_host_exclusion=False,
    )
    assert out is not None
    _url, _ref, _depth, reserved = out
    assert reserved is None


def test_try_pop_respecting_busy_hosts_no_enforce_matches_try_pop():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/z", "", 0, is_seed=True)
    lock = threading.Lock()
    busy = set()
    a = pq.try_pop_respecting_busy_hosts(
        lock, busy, enforce_distinct_hosts=False, strict_same_host_exclusion=False,
    )
    pq.push("https://a.com/z", "", 0, is_seed=True)
    b = pq.try_pop()
    assert a is not None and b is not None
    assert a[0] == b[0]
    assert a[1] == b[1] and a[2] == b[2]
    assert a[3] is None
