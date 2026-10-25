#!/usr/bin/env python3
"""构建真实二进制，用本机临时端口验证 P0；不依赖 MySQL 或第三方 Python 包。"""

import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]


def main():
    processes = []
    with tempfile.TemporaryDirectory(prefix="cloudreve-p0-") as temporary:
        temp = Path(temporary)
        binary = temp / "server"
        subprocess.run(
            ["go", "build", "-o", str(binary), "./cmd/server"],
            cwd=ROOT, check=True, env={**os.environ, "GOTOOLCHAIN": "local"},
        )

        def configuration(name, address, timeout=2):
            path = temp / name
            path.write_text(
                f"[HTTP]\nListen = {address}\nShutdownTimeoutSeconds = {timeout}\n"
                f"[Storage]\nLocalRoot = {temp / 'data'}\n",
                encoding="utf-8",
            )
            return path

        def start(path):
            process = subprocess.Popen(
                [str(binary), "--conf", str(path)], cwd=ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            processes.append(process)
            deadline = time.monotonic() + 5
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while time.monotonic() < deadline:
                    if not selector.select(timeout=max(0, deadline - time.monotonic())):
                        break
                    line = process.stdout.readline()
                    if not line:
                        raise AssertionError("server exited before readiness")
                    event = json.loads(line)
                    if event.get("msg") == "http listening":
                        assert event["database_connected"] is False
                        return process, event["address"]
            raise AssertionError("server did not become ready")

        def request(address, path, status, key, value):
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            try:
                response = opener.open(f"http://{address}{path}", timeout=3)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                assert response.status == status, response.status
                assert json.load(response)[key] == value

        def stop(process, received_signal):
            process.send_signal(received_signal)
            output, _ = process.communicate(timeout=5)
            assert process.returncode == 0, output
            assert '"msg":"http stopped"' in output, output

        try:
            conf = configuration("conf.ini", "127.0.0.1:0")
            first, address = start(conf)
            request(address, "/healthz", 200, "status", "ok")
            request(address, "/missing", 404, "code", "not_found")
            print("PASS: real-process health and JSON errors; no database required")

            fixed = configuration("fixed.ini", address)
            collision = subprocess.run(
                [str(binary), "--conf", str(fixed)], capture_output=True, text=True, timeout=5,
            )
            assert collision.returncode == 1 and "listen HTTP" in collision.stdout
            print("PASS: occupied port fails clearly")

            stop(first, signal.SIGINT)
            second, same_address = start(fixed)
            assert same_address == address
            request(same_address, "/healthz", 200, "status", "ok")
            stop(second, signal.SIGTERM)
            print("PASS: SIGINT, same-port restart, SIGTERM")

            invalid = configuration("invalid.ini", "127.0.0.1:0", timeout=0)
            result = subprocess.run(
                [str(binary), "--conf", str(invalid)], capture_output=True, text=True, timeout=5,
            )
            assert result.returncode == 1 and "ShutdownTimeoutSeconds" in result.stderr
            print("PASS: invalid configuration fails before startup")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)


if __name__ == "__main__":
    main()
