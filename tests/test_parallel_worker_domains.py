"""Parallel crawl scheduling: prefer distinct hostnames across workers."""

import threading

import scraper


def test_try_pop_respecting_busy_hosts_skips_busy_when_multiple_origins():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/1", "", 0, is_seed=True)
    pq.push("https://b.com/1", "", 0, is_seed=True)
    lock = threading.Lock()
    busy = {"a.com"}
    out = pq.try_pop_respecting_busy_hosts(lock, busy, enforce_distinct_hosts=True)
    assert out is not None
    url, _ref, _depth, reserved = out
    assert "b.com" in url
    assert reserved == "b.com"
    assert "b.com" in busy


def test_try_pop_respecting_busy_hosts_collapses_when_single_origin_in_queue():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/1", "", 0, is_seed=True)
    pq.push("https://a.com/2", "", 1, is_seed=False)
    lock = threading.Lock()
    busy = {"a.com"}
    out = pq.try_pop_respecting_busy_hosts(lock, busy, enforce_distinct_hosts=True)
    assert out is not None
    _url, _ref, _depth, reserved = out
    assert reserved is None


def test_try_pop_respecting_busy_hosts_no_enforce_matches_try_pop():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/z", "", 0, is_seed=True)
    lock = threading.Lock()
    busy = set()
    a = pq.try_pop_respecting_busy_hosts(lock, busy, enforce_distinct_hosts=False)
    pq.push("https://a.com/z", "", 0, is_seed=True)
    b = pq.try_pop()
    assert a is not None and b is not None
    assert a[0] == b[0]
    assert a[1] == b[1] and a[2] == b[2]
    assert a[3] is None
