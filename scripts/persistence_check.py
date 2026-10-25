#!/usr/bin/env python3
"""P2 验收：上传 → 重启服务 → 按原 ID 下载，内容 SHA-256 不变。

前提：MySQL 已启动（docker compose up -d），且已建好 luckycloudfile 库。
"""

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def main():
    db_host = os.environ.get("CLOUDREVE_DB_HOST", "127.0.0.1")
    db_port = os.environ.get("CLOUDREVE_DB_PORT", "3306")
    db_user = os.environ.get("CLOUDREVE_DB_USER", "lucky")
    db_name = os.environ.get("CLOUDREVE_DB_NAME", "luckycloudfile")
    os.environ.setdefault("CLOUDREVE_DB_PASSWORD", "lucky")

    # 不走系统代理，避免 http_proxy 干扰本地回环请求。
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    with tempfile.TemporaryDirectory(prefix="luckycloudfile-persist-") as temporary:
        temp = Path(temporary)
        binary = temp / "server"
        subprocess.run(
            ["go", "build", "-o", str(binary), "./cmd/server"],
            cwd=ROOT, check=True, env={**os.environ, "GOTOOLCHAIN": "local"},
        )

        config = temp / "conf.ini"
        config.write_text(
            f"[HTTP]\nListen = 127.0.0.1:0\nShutdownTimeoutSeconds = 5\n"
            f"[Storage]\nLocalRoot = {temp / 'data'}\n"
            f"[Database]\nHost = {db_host}\nPort = {db_port}\n"
            f"User = {db_user}\nName = {db_name}\n",
            encoding="utf-8",
        )

        processes = []

        def start():
            process = subprocess.Popen(
                [str(binary), "--conf", str(config)], cwd=ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            processes.append(process)
            for line in process.stdout:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("msg") == "http listening":
                    assert event.get("database_connected") is True, event
                    return process, event["address"]
            raise AssertionError("server exited before becoming ready")

        def stop(process):
            process.send_signal(signal.SIGINT)
            output, _ = process.communicate(timeout=15)
            assert process.returncode == 0, output
            assert '"msg":"http stopped"' in output, output

        def upload(address, content):
            boundary = "----luckycloudfile-p2"
            body = (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="file"; filename="payload.bin"\r\n'
                "Content-Type: application/octet-stream\r\n\r\n"
            ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
            request = urllib.request.Request(
                f"http://{address}/api/files",
                data=body,
                method="POST",
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            )
            with opener.open(request, timeout=30) as response:
                assert response.status == 201, response.status
                return json.load(response)

        def download(address, file_id):
            with opener.open(f"http://{address}/api/files/{file_id}", timeout=30) as response:
                assert response.status == 200, response.status
                return response.read()

        def delete(address, file_id):
            request = urllib.request.Request(
                f"http://{address}/api/files/{file_id}", method="DELETE"
            )
            with opener.open(request, timeout=30) as response:
                assert response.status == 204, response.status

        try:
            content = os.urandom(100_000)
            before = hashlib.sha256(content).hexdigest()

            first, address = start()
            record = upload(address, content)
            file_id = record["id"]
            assert record["size"] == len(content), record
            print(f"PASS: upload -> id={file_id}, size={record['size']}")
            stop(first)

            # 关键一步：同一个二进制、同一个库、同一个存储目录，重新启动。
            second, address = start()
            downloaded = download(address, file_id)
            after = hashlib.sha256(downloaded).hexdigest()
            assert after == before, f"SHA-256 changed: {before} != {after}"
            print("PASS: restart -> download by original ID, SHA-256 unchanged")

            delete(address, file_id)
            print("PASS: delete after restart")
            stop(second)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=10)


if __name__ == "__main__":
    main()
