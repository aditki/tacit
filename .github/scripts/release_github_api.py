#!/usr/bin/env python3
"""Proxy-isolated, origin-bound GitHub API request construction for releases."""

from __future__ import annotations

from typing import Any
from urllib.parse import SplitResult, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, OpenerDirector, ProxyHandler, Request, build_opener


class ReleaseGitHubAPIError(RuntimeError):
    """A credential-bearing GitHub API request violated its trust boundary."""


def _parsed_https_url(url: str, *, label: str) -> SplitResult:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise ReleaseGitHubAPIError(f"{label} is invalid") from exc
    if (
        parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ReleaseGitHubAPIError(f"{label} must use an authenticated HTTPS origin")
    return parsed


def _origin(parsed: SplitResult) -> tuple[str, str, int]:
    return (parsed.scheme.casefold(), (parsed.hostname or "").casefold(), parsed.port or 443)


def validate_github_api_url(api_url: str) -> str:
    parsed = _parsed_https_url(api_url.strip(), label="GITHUB_API_URL")
    if parsed.query or parsed.fragment:
        raise ReleaseGitHubAPIError("GITHUB_API_URL must not contain a query or fragment")
    path = parsed.path.rstrip("/")
    if any(segment in {".", ".."} for segment in path.split("/")):
        raise ReleaseGitHubAPIError("GITHUB_API_URL contains an invalid path")
    return urlunsplit(("https", parsed.netloc, path, "", ""))


def authenticated_github_request(
    url: str,
    *,
    api_url: str,
    token: str,
    accept: str,
) -> Request:
    base_url = validate_github_api_url(api_url)
    base = _parsed_https_url(base_url, label="GITHUB_API_URL")
    target = _parsed_https_url(url, label="GitHub API request URL")
    base_path = base.path.rstrip("/")
    if _origin(target) != _origin(base) or (
        base_path and target.path != base_path and not target.path.startswith(f"{base_path}/")
    ):
        raise ReleaseGitHubAPIError("GitHub API request URL does not match the expected HTTPS API origin")
    if not token:
        raise ReleaseGitHubAPIError("GITHUB_TOKEN is required")
    return Request(
        url,
        headers={
            "Accept": accept,
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Request,
        fp: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        return None


class _StripAuthorizationOnHTTPSRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Request,
        fp: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> Request | None:
        if urlsplit(new_url).scheme.casefold() != "https":
            return None
        redirected = super().redirect_request(request, fp, code, message, headers, new_url)
        if redirected is not None:
            redirected.remove_header("Authorization")
        return redirected


def build_github_api_opener(*, allow_https_redirects: bool) -> OpenerDirector:
    redirect_handler: HTTPRedirectHandler
    if allow_https_redirects:
        redirect_handler = _StripAuthorizationOnHTTPSRedirect()
    else:
        redirect_handler = _RejectRedirects()
    return build_opener(ProxyHandler({}), redirect_handler)
