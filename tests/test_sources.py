"""Tests for proxy source fetchers and parsers."""

import pytest

from swarm.proxies.sources import parse_proxy_lines, ProxyEntry


def test_parse_alltxt_format():
    text = """http://1.2.3.4:8080
socks5://5.6.7.8:1080
9.9.9.9:3128
*10.0.0.1:1080
user:pass@11.11.11.11:8080
garbage line
http://1.2.3.4:8080
"""
    entries = parse_proxy_lines(text)
    urls = [e.url for e in entries]
    assert "http://1.2.3.4:8080" in urls
    assert "socks5://5.6.7.8:1080" in urls
    assert "http://9.9.9.9:3128" in urls          # bare -> http
    assert "socks5://10.0.0.1:1080" in urls       # legacy * prefix
    assert "http://user:pass@11.11.11.11:8080" in urls
    assert len(urls) == len(set(urls))            # deduped


def test_parse_skips_malformed():
    entries = parse_proxy_lines("http://\nno-port-here\nhttp://ok.proxy:80")
    assert [e.url for e in entries] == ["http://ok.proxy:80"]


def test_entry_protocol_split():
    entries = parse_proxy_lines("socks5://a.b:1\nhttp://c.d:2")
    assert entries[0].protocol == "socks5"
    assert entries[1].protocol == "http"


def test_parse_megabasterd_style_auth():
    # user:pass@host:port with socks marker
    entries = parse_proxy_lines("*u:p@1.1.1.1:1080")
    assert entries[0].url == "socks5://u:p@1.1.1.1:1080"
    assert isinstance(entries[0], ProxyEntry)
