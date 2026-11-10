from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue
from threading import Thread
from urllib.parse import urlparse

import pytest
from playwright.sync_api import Route, sync_playwright

from tacit import demo_flow


class _StaticTacitServer:
    def __init__(self, html: str) -> None:
        self.api_keys: list[str] = []
        self.root_requests: list[tuple[str, dict[str, str]]] = []
        api_keys = self.api_keys
        root_requests = self.root_requests
        page = html.encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/":
                    root_requests.append((self.path, dict(self.headers.items())))
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(page)))
                    self.end_headers()
                    self.wfile.write(page)
                    return
                if self.path == "/api/v1/signals":
                    api_keys.append(self.headers.get("X-API-Key", ""))
                    body = b'{"signal_types":[]}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_error(404)

            def log_message(self, _format: str, *_args: object) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://localhost:{self._server.server_port}"

    def __enter__(self) -> _StaticTacitServer:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)


@pytest.mark.e2e
def test_zero_config_demo_handoff_authenticates_only_the_exact_target_origin():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "zero-config-demo-browser-secret"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    with _StaticTacitServer(html) as target:

        def run_handoff() -> None:
            try:
                demo_flow.open_authenticated_demo_ui(
                    target.url,
                    api_key=api_key,
                    open_browser=open_browser,
                    timeout_s=10.0,
                )
            except BaseException as exc:  # pragma: no cover - asserted in caller
                handoff_errors.append(exc)

        handoff_thread = Thread(target=run_handoff, daemon=True)
        handoff_thread.start()
        bootstrap_url = bootstrap_urls.get(timeout=5.0)
        assert api_key not in bootstrap_url

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()
            page.add_init_script("window.open = () => null")
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
            )

            assert page.url == f"{target.url}/"
            assert api_key not in page.url
            assert api_key not in page.content()
            assert page.locator("#api-key").input_value() == api_key
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("window.name") == ""
            assert page.evaluate("localStorage.length") == 0
            assert (
                page.evaluate(
                    "fetch('/api/v1/signals', { headers: knowledgeHeaders() })" ".then(response => response.status)"
                )
                == 200
            )
            handoff_thread.join(timeout=5.0)
            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            assert target.api_keys == [api_key]
            assert len(target.root_requests) == 1
            assert api_key not in repr(target.root_requests)
            assert "tacit-demo-auth-v3" not in repr(target.root_requests)
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_consumer_rejects_malformed_wrong_origin_and_replay():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    accepted_key = "accepted-one-time-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:

        def run_handoff() -> None:
            try:
                demo_flow.open_authenticated_demo_ui(
                    target.url,
                    api_key=accepted_key,
                    open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                    timeout_s=5.0,
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                handoff_errors.append(exc)

        handoff_thread = Thread(target=run_handoff, daemon=True)
        handoff_thread.start()
        bootstrap_url = bootstrap_urls.get(timeout=5.0)

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                if (window.location.hash.startsWith('#tacit-demo-auth-v3=')) {
                  window.__capturedDemoHandoffHash = window.location.hash;
                }
                window.setTimeout(() => {
                  window.dispatchEvent(new MessageEvent('message', {
                    origin: 'http://127.0.0.1:6553',
                    source: null,
                    data: {
                      type: 'tacit-demo-auth-deliver-v3', nonce: 'wrong-nonce',
                      apiKey: 'malformed\\nkey', expiresAtMs: Number.MAX_SAFE_INTEGER,
                    },
                  }));
                }, 0);
                """)
            page = context.new_page()
            page.add_init_script("window.open = () => null")
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=accepted_key,
            )
            captured_handoff = page.evaluate("window.__capturedDemoHandoffHash")
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            assert isinstance(captured_handoff, str) and accepted_key not in captured_handoff
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == accepted_key
            assert page.locator("#api-key").input_value() == accepted_key

            page.evaluate("handoff => { window.location.hash = handoff; }", captured_handoff)
            page.reload()
            page.wait_for_timeout(100)
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == accepted_key
            assert page.evaluate("window.name") == ""
            assert page.evaluate("localStorage.length") == 0
            assert accepted_key not in page.url
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_completes_in_the_same_tab_when_popups_are_blocked():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "popup-blocked-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    with _StaticTacitServer(html) as target:

        def run_handoff() -> None:
            try:
                demo_flow.open_authenticated_demo_ui(
                    target.url,
                    api_key=api_key,
                    open_browser=open_browser,
                    timeout_s=5.0,
                )
            except BaseException as exc:  # pragma: no cover - asserted in caller
                handoff_errors.append(exc)

        handoff_thread = Thread(target=run_handoff, daemon=True)
        handoff_thread.start()
        bootstrap_url = bootstrap_urls.get(timeout=5.0)

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()
            page.add_init_script("window.open = () => null")
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
            )
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            assert api_key not in page.url
            assert api_key not in page.content()
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("window.name") == ""
            assert page.evaluate("localStorage.length") == 0
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_reload_before_commit_purges_the_uncommitted_key():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "reload-before-commit-demo-key"
    previous_api_key = "existing-reload-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  if (payload?.type === 'tacit-demo-auth-activated-v3') {
                    return new Promise(() => {});
                  }
                  return nativeFetch(...args);
                };
                """)
            page = context.new_page()
            page.goto(f"{target.url}/")
            page.evaluate("key => sessionStorage.setItem('tacit.apiKey', key)", previous_api_key)

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=open_browser,
                        timeout_s=1.5,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            bootstrap_url = bootstrap_urls.get(timeout=5.0)

            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
            )
            assert page.locator("#api-key").input_value() == api_key
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == previous_api_key

            page.reload()
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoError)
            assert "Timed out" in str(handoff_errors[0])
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == previous_api_key
            assert page.evaluate(
                "expected => !Object.values(sessionStorage).some(value => value.includes(expected))",
                arg=api_key,
            )
            assert page.locator("#api-key").input_value() == previous_api_key
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == previous_api_key
            assert api_key not in page.url
            assert api_key not in page.content()
            assert page.evaluate("localStorage.length") == 0
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_waits_for_browser_promotion_after_prepare_response():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "post-promotion-ack-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = async (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  const response = await nativeFetch(...args);
                  if (payload?.type === 'tacit-demo-auth-accepted-v3') {
                    window.__demoPrepareResponseHeld = true;
                    await new Promise(resolve => { window.__releaseDemoPrepareResponse = resolve; });
                  }
                  return response;
                };
                """)
            page = context.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=4.0,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            bootstrap_url = bootstrap_urls.get(timeout=5.0)
            bootstrap_origin = bootstrap_url.rsplit("/", 1)[0]

            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => Object.values(sessionStorage).some(value => value.includes(expected))",
                arg=api_key,
            )
            handoff_frame = next(frame for frame in page.frames if frame.url.startswith(bootstrap_origin))
            handoff_frame.wait_for_function("window.__demoPrepareResponseHeld === true")

            assert handoff_thread.is_alive()
            handoff_frame.evaluate("window.__releaseDemoPrepareResponse()")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
            )
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            assert page.evaluate(
                "expected => !Object.values(sessionStorage).some("
                "value => value.includes(expected) && value !== expected)",
                arg=api_key,
            )
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_waits_for_parent_commit_after_activation_response():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "parent-commit-ack-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = async (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  const response = await nativeFetch(...args);
                  if (payload?.type === 'tacit-demo-auth-activated-v3') {
                    window.__demoActivationResponseHeld = true;
                    await new Promise(resolve => { window.__releaseDemoActivationResponse = resolve; });
                  }
                  return response;
                };
                """)
            page = context.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=4.0,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            bootstrap_url = bootstrap_urls.get(timeout=5.0)
            bootstrap_origin = bootstrap_url.rsplit("/", 1)[0]
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
            )
            handoff_frame = next(frame for frame in page.frames if frame.url.startswith(bootstrap_origin))
            handoff_frame.wait_for_function("window.__demoActivationResponseHeld === true")

            assert handoff_thread.is_alive()
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("knowledgeHeaders()") == {}

            handoff_frame.evaluate("window.__releaseDemoActivationResponse()")
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == api_key
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_reload_while_activation_response_is_held_cannot_report_success():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "activation-response-reload-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = async (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  const response = await nativeFetch(...args);
                  if (payload?.type === 'tacit-demo-auth-activated-v3') {
                    window.__demoActivationResponseHeld = true;
                    await new Promise(() => {});
                  }
                  return response;
                };
                """)
            page = context.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=1.5,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            bootstrap_url = bootstrap_urls.get(timeout=5.0)
            bootstrap_origin = bootstrap_url.rsplit("/", 1)[0]
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
            )
            handoff_frame = next(frame for frame in page.frames if frame.url.startswith(bootstrap_origin))
            handoff_frame.wait_for_function("window.__demoActivationResponseHeld === true")

            assert handoff_thread.is_alive()
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("knowledgeHeaders()") == {}

            page.reload()
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoError)
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") is None
            assert page.evaluate("knowledgeHeaders()") == {}
            assert api_key not in str(page.evaluate("Object.fromEntries(Object.entries(sessionStorage))"))
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_aborted_final_post_is_indeterminate_not_failed():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "aborted-final-post-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  if (payload?.type === 'tacit-demo-auth-finalized-v3') {
                    return Promise.reject(new DOMException('aborted finalization', 'AbortError'));
                  }
                  return nativeFetch(...args);
                };
                """)
            page = context.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=2.0,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            page.goto(bootstrap_urls.get(timeout=5.0))
            page.wait_for_url(f"{target.url}/")
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoHandoffIndeterminateError)
            assert page.evaluate("sessionStorage.getItem('tacit.demoAuth.pending.v3')") is None
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == api_key
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_reload_with_final_post_missing_is_indeterminate():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "missing-final-post-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = async (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  if (payload?.type === 'tacit-demo-auth-finalized-v3') {
                    window.__demoFinalPostHeld = true;
                    await new Promise(() => {});
                  }
                  return nativeFetch(...args);
                };
                """)
            page = context.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=1.5,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            bootstrap_url = bootstrap_urls.get(timeout=5.0)
            bootstrap_origin = bootstrap_url.rsplit("/", 1)[0]
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected && "
                "sessionStorage.getItem('tacit.demoAuth.pending.v3') === null",
                arg=api_key,
            )
            handoff_frame = next(frame for frame in page.frames if frame.url.startswith(bootstrap_origin))
            handoff_frame.wait_for_function("window.__demoFinalPostHeld === true")

            assert handoff_thread.is_alive()
            page.reload()
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoHandoffIndeterminateError)
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == api_key
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_final_response_delay_cannot_undo_committed_authority():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "delayed-final-response-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = async (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  const response = await nativeFetch(...args);
                  if (payload?.type === 'tacit-demo-auth-finalized-v3') {
                    window.__demoFinalResponseHeld = true;
                    await new Promise(() => {});
                  }
                  return response;
                };
                """)
            page = context.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=2.0,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            bootstrap_url = bootstrap_urls.get(timeout=5.0)
            bootstrap_origin = bootstrap_url.rsplit("/", 1)[0]
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected && "
                "sessionStorage.getItem('tacit.demoAuth.pending.v3') === null",
                arg=api_key,
            )
            handoff_frame = next(frame for frame in page.frames if frame.url.startswith(bootstrap_origin))
            handoff_frame.wait_for_function("window.__demoFinalResponseHeld === true")
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            page.reload()
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == api_key
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_never_promotes_a_prepare_response_processed_after_deadline():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "expired-prepare-response-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script(f"""
                const nativeFetch = window.fetch.bind(window);
                const nativeSetTimeout = window.setTimeout.bind(window);
                window.fetch = async (...args) => {{
                  let payload = null;
                  try {{
                    payload = JSON.parse(args[1]?.body || 'null');
                  }} catch (_error) {{}}
                  const response = await nativeFetch(...args);
                  if (payload?.type === 'tacit-demo-auth-accepted-v3') {{
                    await new Promise(resolve => nativeSetTimeout(resolve, 1200));
                  }}
                  return response;
                }};
                if (window.location.origin === {json.dumps(target.url)}) {{
                  window.setTimeout = (callback, delay, ...args) => (
                    nativeSetTimeout(callback, delay + 2000, ...args)
                  );
                }}
                """)
            page = context.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=1.0,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            page.goto(bootstrap_urls.get(timeout=5.0))
            page.wait_for_url(f"{target.url}/")
            handoff_thread.join(timeout=5.0)
            page.wait_for_timeout(1400)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoError)
            assert api_key not in str(page.evaluate("Object.fromEntries(Object.entries(sessionStorage))"))
            assert page.locator("#api-key").input_value() == ""
            assert page.evaluate("knowledgeHeaders()") == {}
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_same_origin_navigation_purges_pending_authority():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "same-origin-navigation-demo-key"
    previous_api_key = "existing-navigation-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    with _StaticTacitServer(html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                window.fetch = (...args) => {
                  let payload = null;
                  try {
                    payload = JSON.parse(args[1]?.body || 'null');
                  } catch (_error) {}
                  if (payload?.type === 'tacit-demo-auth-activated-v3') {
                    return new Promise(() => {});
                  }
                  return nativeFetch(...args);
                };
                """)
            page = context.new_page()
            page.goto(f"{target.url}/")
            page.evaluate("key => sessionStorage.setItem('tacit.apiKey', key)", previous_api_key)

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                        timeout_s=1.5,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            page.goto(bootstrap_urls.get(timeout=5.0))
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
            )
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == previous_api_key

            page.goto(f"{target.url}/handoff-abandoned")
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoError)
            assert api_key not in str(page.evaluate("Object.fromEntries(Object.entries(sessionStorage))"))
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == previous_api_key
            assert page.evaluate("localStorage.length") == 0
            page.goto(f"{target.url}/")
            assert page.evaluate("knowledgeHeaders()['X-API-Key']") == previous_api_key
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_timeout_cannot_resume_a_redeemed_key_delivery():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "timeout-race-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    with _StaticTacitServer(html) as target:

        def run_handoff() -> None:
            try:
                demo_flow.open_authenticated_demo_ui(
                    target.url,
                    api_key=api_key,
                    open_browser=open_browser,
                    timeout_s=1.0,
                )
            except BaseException as exc:  # pragma: no cover - asserted in caller
                handoff_errors.append(exc)

        handoff_thread = Thread(target=run_handoff, daemon=True)
        handoff_thread.start()
        bootstrap_url = bootstrap_urls.get(timeout=5.0)

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeFetch = window.fetch.bind(window);
                const nativeSetTimeout = window.setTimeout.bind(window);
                let delayRedemption = true;
                window.fetch = async (...args) => {
                  const response = await nativeFetch(...args);
                  if (delayRedemption && response.headers.get('content-type') === 'application/json') {
                    delayRedemption = false;
                    await new Promise(resolve => nativeSetTimeout(resolve, 1050));
                  }
                  return response;
                };
                """)
            page = context.new_page()
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            handoff_thread.join(timeout=5.0)
            page.wait_for_timeout(1200)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoError)
            assert "demo" in str(handoff_errors[0]).lower()
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") is None
            assert api_key not in page.url
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_ready_just_before_authoritative_deadline_still_succeeds():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    delayed_ready = (
        "const remainingBeforeRedeemMs = "
        "Math.max(serverDeadlineMs - monotonicEpochMs() - 1200, 0); "
        "window.setTimeout(() => "
        "frame.contentWindow.postMessage({type: DEMO_AUTH_REDEEM_TYPE, nonce}, bootstrapOrigin), "
        "remainingBeforeRedeemMs);"
    )
    delayed_html = html.replace(
        "frame.contentWindow.postMessage({type: DEMO_AUTH_REDEEM_TYPE, nonce}, bootstrapOrigin);",
        delayed_ready,
    )
    assert delayed_html != html
    api_key = "pre-deadline-delayed-ready-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    with _StaticTacitServer(delayed_html) as target:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()

            def run_handoff() -> None:
                try:
                    demo_flow.open_authenticated_demo_ui(
                        target.url,
                        api_key=api_key,
                        open_browser=open_browser,
                        timeout_s=5.0,
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    handoff_errors.append(exc)

            handoff_thread = Thread(target=run_handoff, daemon=True)
            handoff_thread.start()
            bootstrap_url = bootstrap_urls.get(timeout=5.0)
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
                timeout=6000,
            )
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            assert api_key not in page.url
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_deadline_longer_than_fifteen_seconds_does_not_start_target_ttl():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    delayed_ready = (
        "window.setTimeout(() => "
        "frame.contentWindow.postMessage({type: DEMO_AUTH_REDEEM_TYPE, nonce}, bootstrapOrigin), 100);"
    )
    delayed_html = html.replace(
        "frame.contentWindow.postMessage({type: DEMO_AUTH_REDEEM_TYPE, nonce}, bootstrapOrigin);",
        delayed_ready,
    )
    assert delayed_html != html
    api_key = "long-authoritative-deadline-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    with _StaticTacitServer(delayed_html) as target:

        def run_handoff() -> None:
            try:
                demo_flow.open_authenticated_demo_ui(
                    target.url,
                    api_key=api_key,
                    open_browser=open_browser,
                    timeout_s=20.0,
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                handoff_errors.append(exc)

        handoff_thread = Thread(target=run_handoff, daemon=True)
        handoff_thread.start()
        bootstrap_url = bootstrap_urls.get(timeout=5.0)

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script("""
                const nativeSetTimeout = window.setTimeout.bind(window);
                window.setTimeout = (callback, delay, ...args) => {
                  if (delay === 15000) return nativeSetTimeout(callback, 40, ...args);
                  if (delay > 10000) return nativeSetTimeout(callback, 500, ...args);
                  return nativeSetTimeout(callback, delay, ...args);
                };
                """)
            page = context.new_page()
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            page.wait_for_function(
                "expected => sessionStorage.getItem('tacit.apiKey') === expected",
                arg=api_key,
                timeout=3000,
            )
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert handoff_errors == []
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") == api_key
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_target_rejects_a_delivery_processed_after_deadline():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    api_key = "delayed-target-demo-key"
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    with _StaticTacitServer(html) as target:

        def run_handoff() -> None:
            try:
                demo_flow.open_authenticated_demo_ui(
                    target.url,
                    api_key=api_key,
                    open_browser=open_browser,
                    timeout_s=1.0,
                )
            except BaseException as exc:  # pragma: no cover - asserted in caller
                handoff_errors.append(exc)

        handoff_thread = Thread(target=run_handoff, daemon=True)
        handoff_thread.start()
        bootstrap_url = bootstrap_urls.get(timeout=5.0)

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = browser.new_context()
            context.add_init_script(f"""
                  const nativeAddEventListener = window.addEventListener.bind(window);
                  window.addEventListener = (type, listener, options) => {{
                    if (type === 'message' && window.location.origin === {json.dumps(target.url)}) {{
                      nativeAddEventListener(type, event => {{
                        window.setTimeout(() => listener.call(window, event), 1200);
                      }}, options);
                      return;
                    }}
                    nativeAddEventListener(type, listener, options);
                  }};
                """)
            page = context.new_page()
            page.goto(bootstrap_url)
            page.wait_for_url(f"{target.url}/")
            handoff_thread.join(timeout=5.0)
            page.wait_for_timeout(800)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoError)
            assert page.evaluate("sessionStorage.getItem('tacit.apiKey')") is None
            assert api_key not in page.url
            browser.close()


@pytest.mark.e2e
def test_demo_handoff_loopback_redirect_cannot_read_api_key_in_real_chromium():
    api_key = "redirect-protected-demo-key"
    attacker_requests: list[tuple[str, dict[str, str]]] = []
    attacker_document = b"""
      <!doctype html><script>
        window.__received = [];
        window.__handoffState = window.location.hash;
        window.addEventListener('message', event => window.__received.push(event.data));
        try {
          const descriptor = JSON.parse(decodeURIComponent(window.location.hash.split('=', 2)[1]));
          const frame = document.createElement('iframe');
          frame.src = descriptor.frameUrl;
          document.body.appendChild(frame);
        } catch (_error) {}
      </script>
    """

    class AttackerHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            attacker_requests.append((self.path, dict(self.headers.items())))
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(attacker_document)))
            self.end_headers()
            self.wfile.write(attacker_document)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    attacker = ThreadingHTTPServer(("127.0.0.1", 0), AttackerHandler)

    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{attacker.server_port}/capture")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, _format: str, *_args: object) -> None:
            return

    redirector = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    server_threads = [
        Thread(target=attacker.serve_forever, daemon=True),
        Thread(target=redirector.serve_forever, daemon=True),
    ]
    for server_thread in server_threads:
        server_thread.start()

    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def open_browser(url: str, *, new: int) -> bool:
        assert new == 2
        bootstrap_urls.put(url)
        return True

    def run_handoff() -> None:
        try:
            demo_flow.open_authenticated_demo_ui(
                f"http://127.0.0.1:{redirector.server_port}",
                api_key=api_key,
                open_browser=open_browser,
                timeout_s=1.0,
            )
        except BaseException as exc:  # pragma: no cover - asserted in caller
            handoff_errors.append(exc)

    handoff_thread = Thread(target=run_handoff, daemon=True)
    handoff_thread.start()
    bootstrap_url = bootstrap_urls.get(timeout=5.0)

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()
            page.add_init_script("window.open = () => null")
            page.goto(bootstrap_url)
            page.wait_for_url(f"http://127.0.0.1:{attacker.server_port}/capture*")
            page.wait_for_function("Array.isArray(window.__received)")
            handoff_thread.join(timeout=5.0)

            assert not handoff_thread.is_alive()
            assert len(handoff_errors) == 1
            assert isinstance(handoff_errors[0], demo_flow.DemoError)
            assert "Timed out" in str(handoff_errors[0])
            assert page.evaluate("window.__received") == []
            assert api_key not in page.evaluate("window.__handoffState")
            assert page.evaluate("sessionStorage.length") == 0
            assert page.evaluate("localStorage.length") == 0
            assert api_key not in page.content()
            assert api_key not in page.url
            browser.close()
    finally:
        redirector.shutdown()
        attacker.shutdown()
        for server_thread in server_threads:
            server_thread.join(timeout=5.0)
        redirector.server_close()
        attacker.server_close()

    assert attacker_requests
    assert api_key not in repr(attacker_requests)


@pytest.mark.e2e
def test_history_renders_hostile_stored_values_without_executing_them():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    hostile_prompt = '" onmouseover="window.__storedXss=true'
    investigation = {
        "id": "inv-hostile-history",
        "status": "success",
        "prompt": hostile_prompt,
        "path_used": "archetype",
        "panel_count": 1,
        "total_time": 1.25,
        "archetypes": [],
    }
    detail = {
        **investigation,
        "dashboard_url": "javascript:window.__urlXss = true",
        "intent_signals": [],
        "metrics_selected": [],
        "datasource_types": [],
        "generated_queries": [],
        "timings": {},
        "validation_warnings": [],
    }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/investigations":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"investigations": [investigation]}),
            )
        elif parsed.path == "/api/v1/investigations/inv-hostile-history":
            route.fulfill(status=200, content_type="application/json", body=json.dumps(detail))
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")
        page.evaluate("window.__storedXss = false; window.__urlXss = false")
        page.evaluate("switchTab('history')")

        prompt_cell = page.locator(".prompt-cell")
        prompt_cell.wait_for()
        assert prompt_cell.get_attribute("title") == hostile_prompt
        assert prompt_cell.get_attribute("onmouseover") is None
        assert prompt_cell.text_content() == hostile_prompt
        prompt_cell.hover()
        assert page.evaluate("window.__storedXss") is False
        assert page.locator("[onmouseover]").count() == 0

        page.locator(".investigation-detail-btn").click()
        page.locator(".hist-detail").wait_for()
        assert page.locator(".hist-detail a").count() == 0
        assert page.evaluate("window.__urlXss") is False
        assert page.evaluate("safeExternalUrl('javascript:alert(1)')") == ""
        assert page.evaluate("safeExternalUrl('https://grafana.example/d/checkout')").startswith(
            "https://grafana.example/"
        )
        browser.close()


@pytest.mark.e2e
def test_insights_render_hostile_stored_values_as_text():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    hostile_recommendation = '<img id="recommendation-xss" src=x onerror="window.__insightsXss=true">: review'
    hostile_archetype = '<img id="archetype-xss" src=x onerror="window.__insightsXss=true">'

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/feedback/stats":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "total_feedback": 1,
                        "total_dashboards": 1,
                        "useful_rate": 1,
                        "avg_noise_level": 4,
                    }
                ),
            )
        elif parsed.path == "/api/v1/feedback/analysis":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "recommendations": [hostile_recommendation],
                        "per_archetype_quality": [
                            {
                                "archetype": hostile_archetype,
                                "useful_rate": 1,
                                "avg_noise": 4,
                                "avg_symptom": 5,
                                "count": 1,
                            }
                        ],
                    }
                ),
            )
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")
        page.evaluate("window.__insightsXss = false")
        page.evaluate("switchTab('insights')")

        page.locator(".rec-item").first.wait_for()
        assert page.locator("#recommendation-xss, #archetype-xss").count() == 0
        assert page.locator("#stats-content img").count() == 0
        assert page.evaluate("window.__insightsXss") is False
        assert hostile_archetype in page.locator(".rec-item").nth(1).text_content()
        browser.close()


@pytest.mark.e2e
def test_signal_detail_loads_additional_mapping_pages_without_replacing_prior_rows():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()

    def mapping(pattern: str, mapping_id: int) -> dict[str, object]:
        return {
            "id": mapping_id,
            "metric_pattern": pattern,
            "confidence": 0.9,
            "source_type": "teach",
        }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/signals":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "signal_types": [
                            {
                                "signal_type": "request_latency",
                                "description": "Request latency",
                                "category": "latency",
                                "unit": "seconds",
                                "mapping_count": 2,
                            }
                        ]
                    }
                ),
            )
        elif parsed.path == "/api/v1/signals/stats":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"signal_types": 1, "metric_mappings": 2}),
            )
        elif parsed.path == "/api/v1/signals/request_latency":
            is_continuation = "cursor=" in parsed.query
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "signal_type": "request_latency",
                        "description": "Request latency",
                        "category": "latency",
                        "unit": "seconds",
                        "mapping_count": 2,
                        "mappings": [
                            (
                                mapping("second_metric_seconds", 2)
                                if is_continuation
                                else mapping("first_metric_seconds", 1)
                            )
                        ],
                        "has_more": not is_continuation,
                        "next_cursor": None if is_continuation else "next-page",
                    }
                ),
            )
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")
        page.evaluate("switchTab('signals')")

        page.locator(".signal-detail-btn").click()
        page.locator(".signal-mappings-more").wait_for()
        assert page.locator(".signal-pattern").all_text_contents() == ["first_metric_seconds"]

        page.locator(".signal-mappings-more").click()
        page.locator(".signal-pattern").nth(1).wait_for()
        assert page.locator(".signal-pattern").all_text_contents() == [
            "first_metric_seconds",
            "second_metric_seconds",
        ]
        assert page.locator(".signal-mappings-more").count() == 0
        browser.close()


@pytest.mark.e2e
def test_signal_taxonomy_loads_later_pages_into_display_and_teaching_selector():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()

    def signal(name: str, category: str) -> dict[str, object]:
        return {
            "signal_type": name,
            "description": f"{name} description",
            "category": category,
            "unit": "count",
            "mapping_count": 1,
        }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/signals":
            continuation = "cursor=" in parsed.query
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "signal_types": [
                            (
                                signal("second_page_signal", "saturation")
                                if continuation
                                else signal("first_page_signal", "latency")
                            )
                        ],
                        "has_more": not continuation,
                        "next_cursor": None if continuation else "taxonomy-page-2",
                    }
                ),
            )
        elif parsed.path == "/api/v1/signals/stats":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"signal_types": 2, "metric_mappings": 2}),
            )
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")
        page.evaluate("switchTab('signals')")

        page.locator(".signal-taxonomy-more").wait_for()
        assert page.locator(".signal-card h3").all_text_contents() == ["first_page_signal"]
        assert page.locator("#teach-signal option").all_text_contents() == ["first_page_signal (latency)"]

        page.locator(".signal-taxonomy-more").click()
        page.get_by_text("second_page_signal", exact=True).wait_for()

        assert page.locator(".signal-card h3").all_text_contents() == [
            "first_page_signal",
            "second_page_signal",
        ]
        assert page.locator("#teach-signal option").all_text_contents() == [
            "first_page_signal (latency)",
            "second_page_signal (saturation)",
        ]
        assert page.locator(".signal-taxonomy-more").count() == 0
        browser.close()


@pytest.mark.e2e
def test_long_lived_browser_views_load_later_tenant_pinned_pages():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()

    def investigation(identifier: str, prompt: str) -> dict[str, object]:
        return {
            "id": identifier,
            "status": "success",
            "prompt": prompt,
            "path_used": "archetype",
            "panel_count": 1,
            "total_time": 1,
            "archetypes": [],
        }

    def dashboard(row_id: int, uid: str) -> dict[str, object]:
        return {
            "id": row_id,
            "dashboard_uid": uid,
            "dashboard_title": uid,
            "backend_name": "grafana",
            "status": "pending",
            "panel_count": 1,
            "metrics_found": [],
            "signals_inferred": [],
        }

    def candidate(identifier: str) -> dict[str, object]:
        return {
            "id": identifier,
            "kind": "dependency",
            "proposition": {"subject_ref": "checkout", "predicate": "depends_on", "object_ref": identifier},
            "state": {"eligibility": "candidate"},
            "policy": {},
            "scope": {},
            "corroboration": {},
        }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/investigations":
            continuation = "before_started_at=" in parsed.query
            payload = {
                "investigations": [
                    (
                        investigation("inv-second", "second history page")
                        if continuation
                        else investigation("inv-first", "first history page")
                    )
                ],
                "next_cursor": None if continuation else {"before_started_at": 2.0, "before_id": "inv-first"},
            }
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))
        elif parsed.path == "/api/v1/learn/dashboards":
            continuation = "before_created_at=" in parsed.query
            payload = {
                "dashboards": [dashboard(2, "dashboard-second") if continuation else dashboard(1, "dashboard-first")],
                "next_cursor": None if continuation else {"before_created_at": 2.0, "before_id": 1},
            }
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))
        elif parsed.path == "/api/v1/knowledge/review-queue":
            continuation = "candidate_cursor=" in parsed.query
            payload = {
                "candidates": [candidate("candidate-second") if continuation else candidate("candidate-first")],
                "candidate_has_more": not continuation,
                "candidate_next_cursor": None if continuation else "candidate-page-2",
                "unresolved_conflicts": [],
                "conflict_has_more": False,
                "conflict_next_cursor": None,
                "attention_items": [],
                "attention_has_more": False,
                "attention_next_cursor": None,
            }
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))
        elif parsed.path == "/api/v1/knowledge/status":
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"tenant_id": ""}))
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")

        page.evaluate("switchTab('history')")
        page.get_by_text("first history page", exact=True).wait_for()
        page.locator(".history-more").click()
        page.get_by_text("second history page", exact=True).wait_for()
        assert page.locator(".prompt-cell").all_text_contents() == ["first history page", "second history page"]

        page.evaluate("switchTab('learning')")
        page.locator(".ingest-title").first.wait_for()
        page.locator(".dashboard-learning-more").click()
        page.locator(".ingest-title").nth(1).wait_for()
        assert page.locator(".ingest-title").all_text_contents() == ["dashboard-first", "dashboard-second"]

        page.evaluate("switchTab('knowledge')")
        page.locator('[data-candidate-id="candidate-first"]').first.wait_for()
        page.locator(".knowledge-queue-more").click()
        page.locator('[data-candidate-id="candidate-second"]').first.wait_for()
        assert page.locator(".knowledge-review-btn[data-decision='approve']").count() == 2
        assert page.locator(".knowledge-queue-more").count() == 0
        browser.close()
