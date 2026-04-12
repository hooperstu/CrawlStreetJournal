"""
Outbound HTTP hardening for crawlers: block non-public destinations (SSRF-style),
enforce response size limits, and follow redirects with per-hop checks.

Uses :mod:`ipaddress` and :func:`socket.getaddrinfo` — resolve hostnames and reject
if any resolved address is not globally routable (RFC 1918, loopback, link-local,
metadata ranges, etc.).
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests

logger = logging.getLogger(__name__)

_ALLOWED_SCHEMES = frozenset({"http", "https"})


def validate_outbound_url(url: str) -> Optional[str]:
    """
    Return ``None`` if *url* may be fetched, or a short human-readable reason if not.

    Checks scheme (http/https only) and that the host resolves only to globally
    routable addresses.
    """
    if not url or not isinstance(url, str):
        return "empty URL"
    url = url.strip()
    try:
        p = urlparse(url)
    except Exception as exc:
        return f"invalid URL: {exc}"

    scheme = (p.scheme or "").lower()
    if scheme not in _ALLOWED_SCHEMES:
        return f"unsupported scheme {scheme!r}"

    host = p.hostname
    if not host:
        return "missing host"

    host = host.strip().lower()
    if host.endswith("."):
        host = host[:-1]

    # Literal IP — check directly (including IPv6 zone id stripped)
    if host.startswith("["):
        host = host[1:].split("%", 1)[0]
        if host.endswith("]"):
            host = host[:-1]

    try:
        addr = ipaddress.ip_address(host)
        if not _ip_is_allowed_global(addr):
            return f"blocked address {addr}"
        return None
    except ValueError:
        pass

    return _dns_host_safe(host)


def _ip_is_allowed_global(addr: ipaddress._BaseAddress) -> bool:
    """True if *addr* is suitable for fetching public web content."""
    if addr.version == 4:
        if addr in ipaddress.ip_network("0.0.0.0/8"):
            return False
        if addr in ipaddress.ip_network("127.0.0.0/8"):
            return False
        if addr in ipaddress.ip_network("10.0.0.0/8"):
            return False
        if addr in ipaddress.ip_network("172.16.0.0/12"):
            return False
        if addr in ipaddress.ip_network("192.168.0.0/16"):
            return False
        if addr in ipaddress.ip_network("169.254.0.0/16"):
            return False
        if addr in ipaddress.ip_network("192.0.0.0/24"):
            return False
        if addr in ipaddress.ip_network("192.0.2.0/24"):
            return False
        if addr in ipaddress.ip_network("198.51.100.0/24"):
            return False
        if addr in ipaddress.ip_network("203.0.113.0/24"):
            return False
        if addr in ipaddress.ip_network("240.0.0.0/4"):
            return False
        if addr.is_multicast or addr.is_reserved or addr.is_link_local:
            return False
        return True

    # IPv6
    if addr.is_loopback or addr.is_link_local or addr.is_multicast:
        return False
    if addr.is_private:
        return False
    if addr in ipaddress.ip_network("::ffff:0:0/96"):
        # IPv4-mapped — check embedded IPv4
        mapped = ipaddress.IPv4Address(int(addr) & 0xFFFFFFFF)
        return _ip_is_allowed_global(mapped)
    if addr in ipaddress.ip_network("64:ff9b::/96"):
        mapped = ipaddress.IPv4Address(int(addr) & 0xFFFFFFFF)
        return _ip_is_allowed_global(mapped)
    if addr in ipaddress.ip_network("100::/64"):
        return False
    if addr in ipaddress.ip_network("2001:db8::/32"):
        return False
    # Unique local (fc00::/7)
    if (int(addr) & 0xFE00000000000000) == 0xFC00000000000000:
        return False
    # IPv4-compatible ::/96 (deprecated but reject)
    if int(addr) < 2**32:
        return False
    return True


def _dns_host_safe(hostname: str) -> Optional[str]:
    """Resolve *hostname*; return error string if any address is not allowed."""
    try:
        infos = socket.getaddrinfo(
            hostname,
            None,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as exc:
        return f"DNS resolution failed: {exc}"

    if not infos:
        return "no addresses from DNS"

    for _fam, _type, _proto, _canon, sockaddr in infos:
        ip_str = sockaddr[0]
        try:
            addr = ipaddress.ip_address(ip_str)
        except ValueError:
            return f"invalid resolved address {ip_str!r}"
        if not _ip_is_allowed_global(addr):
            return f"blocked resolved address {addr} for host {hostname!r}"

    return None


def _read_body_capped(resp: requests.Response, max_bytes: int) -> bytes:
    """Read at most *max_bytes* from a streaming response; always closes *resp*."""
    buf = bytearray()
    try:
        for chunk in resp.iter_content(chunk_size=65536):
            if not chunk:
                continue
            need = max_bytes - len(buf)
            if need <= 0:
                break
            buf.extend(chunk[:need])
            if len(buf) >= max_bytes:
                break
    finally:
        resp.close()
    return bytes(buf)


def get_redirect_target(resp: requests.Response, current_url: str) -> Optional[str]:
    """If *resp* is a redirect with a Location header, return absolute next URL."""
    if resp.status_code not in (301, 302, 303, 307, 308):
        return None
    loc = (resp.headers.get("Location") or "").strip()
    if not loc:
        return None
    return urljoin(current_url, loc)


def request_get_streaming(
    sess: requests.Session,
    url: str,
    *,
    timeout: float,
    max_redirects: int,
    max_body_bytes: int,
    block_private: bool,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[
    Optional[bytes],
    int,
    str,
    str,
    Optional[str],
    Dict[str, str],
    int,
    str,
]:
    """
    GET *url* without auto-redirects; validate each hop; cap body size.

    Returns
    ``(body_or_none, status, final_url, content_type, error, response_headers,
    redirect_count, last_redirect_url)``.
    *response_headers* is populated from the final response (lowercase keys).
    *error* is set on validation or network failure.
    *redirect_count* is the number of HTTP redirect hops followed before the
    terminal response (or before failure).
    *last_redirect_url* is the destination URL of the last redirect hop, or
    empty when *redirect_count* is 0.
    """
    current = url
    hdrs = dict(headers) if headers else {}
    redirect_count = 0
    last_redirect_url = ""

    for _hop in range(max(0, max_redirects) + 1):
        if block_private:
            verr = validate_outbound_url(current)
            if verr:
                return (
                    None, 0, current, "", f"blocked: {verr}", {},
                    redirect_count, last_redirect_url,
                )

        try:
            resp = sess.get(
                current,
                timeout=timeout,
                allow_redirects=False,
                stream=True,
                headers=hdrs,
            )
        except requests.exceptions.RequestException as exc:
            return (
                None, 0, current, "", str(exc)[:400], {},
                redirect_count, last_redirect_url,
            )

        try:
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
            status = resp.status_code
            rh = {k.lower(): v for k, v in resp.headers.items()}

            nxt = get_redirect_target(resp, current)
            if nxt is not None:
                resp.close()
                redirect_count += 1
                last_redirect_url = nxt
                current = nxt
                continue

            if status >= 400:
                resp.close()
                return (
                    None, status, current, ctype, f"HTTP {status}", rh,
                    redirect_count, last_redirect_url,
                )

            body = _read_body_capped(resp, max_body_bytes)
            return (
                body, status, current, ctype, None, rh,
                redirect_count, last_redirect_url,
            )
        except Exception as exc:
            try:
                resp.close()
            except Exception:
                pass
            return (
                None, 0, current, "", str(exc)[:400], {},
                redirect_count, last_redirect_url,
            )

    return (
        None, 0, current, "", "redirect limit exceeded", {},
        redirect_count, last_redirect_url,
    )


def request_head_follow(
    sess: requests.Session,
    url: str,
    *,
    timeout: float,
    max_redirects: int,
    block_private: bool,
) -> Tuple[int, str, str, Optional[str], Dict[str, str]]:
    """
    HEAD *url* with manual redirects.

    Returns ``(status, final_url, content_type, error, response_headers)``.
    """
    current = url

    for _hop in range(max(0, max_redirects) + 1):
        if block_private:
            verr = validate_outbound_url(current)
            if verr:
                return 0, current, "", f"blocked: {verr}", {}

        try:
            resp = sess.head(
                current,
                timeout=timeout,
                allow_redirects=False,
            )
        except requests.exceptions.RequestException as exc:
            return 0, current, "", str(exc)[:400], {}

        try:
            status = resp.status_code
            rh = {k.lower(): v for k, v in resp.headers.items()}
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
            nxt = get_redirect_target(resp, current)
            if nxt is not None:
                resp.close()
                current = nxt
                continue
            resp.close()
            return status, current, ctype, None, rh
        except Exception as exc:
            try:
                resp.close()
            except Exception:
                pass
            return 0, current, "", str(exc)[:400], {}

    return 0, current, "", "redirect limit exceeded", {}
