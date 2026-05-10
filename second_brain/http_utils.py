"""Shared HTTP helper utilities."""

from __future__ import annotations


def cors_headers(extra_allow_headers: str | None = None) -> dict[str, str]:
    allow_headers = "Content-Type"
    if extra_allow_headers:
        allow_headers = extra_allow_headers
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": allow_headers,
    }
