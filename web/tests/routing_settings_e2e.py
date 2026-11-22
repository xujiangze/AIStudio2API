#!/usr/bin/env python3
"""Offline headless Chromium E2E test for routing_strategy=least-recent settings."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_server(base_url: str, proc: subprocess.Popen, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=5)
            raise RuntimeError(
                f"offline GUI fixture exited early ({proc.returncode}): {out}\n{err}"
            )
        try:
            with urllib.request.urlopen(f"{base_url}/api/config", timeout=1.5) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"timed out waiting for offline GUI fixture: {last_error}")


def fetch_config(base_url: str) -> dict:
    with urllib.request.urlopen(f"{base_url}/api/config", timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def build_cgo_env(repo_root: Path, addr: str) -> dict[str, str]:
    env = os.environ.copy()
    env["AISTUDIO_RACING_GUI_FIXTURE"] = addr
    env["CGO_ENABLED"] = "1"
    sm_candidates = [
        Path("/tmp/aistudio2api-deps/spidermonkey-v158.0.1-aistudio2api.2-darwin-arm64"),
        repo_root / "spidermonkey",
    ]
    sm_dir = next((p for p in sm_candidates if (p / "include").is_dir()), None)
    webp_dir = repo_root / "libwebp"
    if sm_dir is not None and (webp_dir / "include").is_dir():
        env.setdefault("CGO_CPPFLAGS", f"-I{sm_dir / 'include'} -I{webp_dir / 'include'}")
        env.setdefault("CGO_LDFLAGS", f"-L{sm_dir / 'lib'} -L{webp_dir / 'lib'}")
    return env


def select_option(page, trigger_selector: str, option_text: str) -> None:
    page.locator(trigger_selector).click()
    page.get_by_role("option", name=option_text, exact=True).click()


def save_settings(page) -> dict:
    with page.expect_response(
        lambda resp: resp.url.endswith("/api/config") and resp.request.method == "PUT"
    ) as resp_info:
        page.locator('form button[type="submit"]').click()
    resp = resp_info.value
    assert resp.status == 200, f"PUT /api/config status={resp.status} body={resp.text()}"
    return resp.json()


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    port = find_free_port()
    addr = f"127.0.0.1:{port}"
    base_url = f"http://{addr}"

    env = build_cgo_env(repo_root, addr)
    proc = subprocess.Popen(
        [
            "go",
            "test",
            "-count=1",
            "-run",
            "^TestRacingGUIFixture$",
            "./internal/app",
        ],
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )

    passed = 0
    try:
        wait_for_server(base_url, proc)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context(locale="zh-CN", viewport={"width": 1280, "height": 900})
                page = context.new_page()
                page.goto(f"{base_url}/?tab=settings&section=capacity", wait_until="networkidle")

                routing = page.locator("#setting-routing")
                window_input = page.locator("#setting-routing-recency-window")

                # 1. 默认策略为轮询，时间窗控件隐藏
                expect(routing).to_be_visible(timeout=10000)
                expect(routing).to_contain_text("轮询")
                expect(window_input).to_have_count(0)
                passed += 1

                # 2. 切到近期均衡，展示默认 300 秒时间窗，改为 120 并保存，刷新后保持且无需重启
                select_option(page, "#setting-routing", "近期均衡")
                expect(window_input).to_be_visible()
                expect(window_input).to_have_value("300")
                window_input.fill("120")
                saved = save_settings(page)
                assert saved["routing_strategy"] == "least-recent", saved
                assert saved["routing_recency_window_seconds"] == 120, saved
                assert saved["service_restart_required"] is False, saved
                assert saved["management_restart_required"] is False, saved

                page.reload(wait_until="networkidle")
                expect(routing).to_contain_text("近期均衡", timeout=10000)
                expect(window_input).to_be_visible()
                expect(window_input).to_have_value("120")
                passed += 1

                # 3. 设为 0 秒并保存，刷新后保留 0 且后端一致
                window_input.fill("0")
                saved_zero = save_settings(page)
                assert saved_zero["routing_recency_window_seconds"] == 0, saved_zero
                page.reload(wait_until="networkidle")
                expect(window_input).to_have_value("0", timeout=10000)
                api_cfg = fetch_config(base_url)
                assert api_cfg["routing_strategy"] == "least-recent", api_cfg
                assert api_cfg["routing_recency_window_seconds"] == 0, api_cfg
                passed += 1

                # 4. 切到账户粘性优先隐藏时间窗并展示账户顺序；切回轮询后两者均隐藏并保存成功
                select_option(page, "#setting-routing", "账户粘性优先")
                expect(window_input).to_have_count(0)
                expect(page.get_by_text("账户顺序", exact=True)).to_be_visible()
                select_option(page, "#setting-routing", "轮询")
                expect(window_input).to_have_count(0)
                expect(page.get_by_text("账户顺序", exact=True)).to_have_count(0)
                saved_rr = save_settings(page)
                assert saved_rr["routing_strategy"] == "round-robin", saved_rr
                passed += 1
            finally:
                browser.close()
    finally:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait(timeout=10)
            except Exception:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    pass

    evidence_path = os.environ.get("DEV_FLOW_BROWSER_EVIDENCE")
    if evidence_path:
        Path(evidence_path).write_text(
            json.dumps({"browser": "chromium", "passed": passed, "failed": 0, "skipped": 0}),
            encoding="utf-8",
        )
    print(f"Chromium E2E passed: {passed}/4 checks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
