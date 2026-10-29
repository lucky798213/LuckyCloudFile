#!/usr/bin/env python3
"""Run browser acceptance against real Go/MySQL, using temporary storage and test-only accounts."""

import contextlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


FRONTEND = Path(__file__).resolve().parents[1]
PROJECT = FRONTEND.parent


def main():
    if not os.environ.get("CLOUDREVE_DB_PASSWORD"):
        raise SystemExit("Set CLOUDREVE_DB_PASSWORD for the isolated test MySQL database first.")
    database = os.environ.get("CLOUDREVE_E2E_DB_NAME", "luckycloudfile_test")
    if not database.endswith("_test"):
        raise SystemExit("Browser acceptance requires a disposable database ending in _test.")
    environment = {
        **os.environ,
        "GOCACHE": str(PROJECT / ".gocache"),
        "GOTOOLCHAIN": "local",
        "pnpm_config_verify_deps_before_run": "false",
    }
    children = []
    with tempfile.TemporaryDirectory(prefix="luckycloudfile-web-") as directory:
        temporary = Path(directory)
        configuration = temporary / "conf.ini"
        configuration.write_text(
            "[HTTP]\nListen = 127.0.0.1:0\nShutdownTimeoutSeconds = 5\n"
            f"[Storage]\nLocalRoot = {temporary / 'data'}\n"
            f"[Task]\nWorkDir = {temporary / 'tasks'}\n"
            f"[Database]\nName = {database}\n"
            f"Host = {os.environ.get('CLOUDREVE_DB_HOST', '127.0.0.1')}\n"
            f"Port = {os.environ.get('CLOUDREVE_DB_PORT', '3306')}\n"
            f"User = {os.environ.get('CLOUDREVE_DB_USER', 'lucky')}\n",
            encoding="utf-8",
        )
        configuration.chmod(0o600)
        for command in ("server", "createuser"):
            subprocess.run(
                ["go", "build", "-o", str(temporary / command), f"./cmd/{command}"],
                cwd=PROJECT, env=environment, check=True,
            )
        suffix = secrets.token_hex(5)
        for key, quota in (("E2E", None), ("E2E_EMPTY", "0")):
            name = f"web-{suffix}-{key.lower()}"
            password = secrets.token_urlsafe(24)
            arguments = [str(temporary / "createuser"), "--conf", str(configuration), "--username", name]
            if quota is not None:
                arguments.extend(["--quota-bytes", quota])
            subprocess.run(arguments, cwd=PROJECT, env={**environment, "CLOUDREVE_USER_PASSWORD": password}, check=True)
            environment[f"{key}_USERNAME"] = name
            environment[f"{key}_PASSWORD"] = password

        with (temporary / "server.log").open("w+") as server_log, (temporary / "vite.log").open("w+") as vite_log:
            try:
                server = subprocess.Popen(
                    [str(temporary / "server"), "--conf", str(configuration)],
                    cwd=PROJECT, env=environment, stdout=server_log, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                children.append(server)
                deadline = time.monotonic() + 20
                address = None
                while time.monotonic() < deadline and not address:
                    server_log.seek(0)
                    for line in server_log:
                        with contextlib.suppress(json.JSONDecodeError):
                            event = json.loads(line)
                            if event.get("msg") == "http listening":
                                address = event["address"]
                    if server.poll() is not None:
                        raise RuntimeError("Go server exited before readiness")
                    time.sleep(0.1)
                if not address:
                    raise RuntimeError("Go server did not become ready")
                with socket.socket() as probe:
                    probe.bind(("127.0.0.1", 0))
                    port = probe.getsockname()[1]
                environment["API_PROXY_TARGET"] = f"http://{address}"
                environment["E2E_BASE_URL"] = f"http://127.0.0.1:{port}"
                frontend = subprocess.Popen(
                    ["pnpm", "dev", "--port", str(port), "--strictPort"],
                    cwd=FRONTEND, env=environment, stdout=vite_log, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                children.append(frontend)
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    try:
                        with opener.open(environment["E2E_BASE_URL"], timeout=1) as response:
                            if response.status == 200:
                                break
                    except (urllib.error.URLError, TimeoutError):
                        time.sleep(0.2)
                else:
                    raise RuntimeError("Vite did not become ready")
                print(f"Real Go/MySQL + browser acceptance: {environment['E2E_BASE_URL']}", flush=True)
                subprocess.run(["pnpm", "test:e2e"], cwd=FRONTEND, env=environment, check=True)
            except Exception:
                for label, log in (("Go", server_log), ("Vite", vite_log)):
                    log.seek(0)
                    print(f"--- {label} log tail ---\n{log.read()[-4000:]}")
                raise
            finally:
                for child in reversed(children):
                    if child.poll() is None:
                        os.killpg(child.pid, signal.SIGTERM)
                        try:
                            child.wait(timeout=8)
                        except subprocess.TimeoutExpired:
                            os.killpg(child.pid, signal.SIGKILL)
                            child.wait()
    print("Temporary services/storage removed. Test-only accounts remain in the disposable test database.")


if __name__ == "__main__":
    main()
