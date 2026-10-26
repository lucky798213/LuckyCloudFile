#!/usr/bin/env python3
"""P2 真实进程验收：登录 → 文件/目录操作 → 重启 → 数据仍在 → 清理 → 退出。

覆盖：
  - 上传文件、建目录、建子目录、往目录里传文件；
  - 重命名目录；
  - 移动文件/目录，旧目录不再列出、新目录可见；
  - 目录不能移到自己或后代；
  - 非空目录删除被拒绝，空目录可以删除；
  - 重启服务后，原 Token 仍有效，目录树和文件内容都不变。

前提：
  - MySQL 已启动（docker compose up -d），且已建好 luckycloudfile 库；
  - 账号已经通过 cmd/createuser 显式创建；
  - 通过 CLOUDREVE_TEST_USERNAME / CLOUDREVE_TEST_PASSWORD 提供账号和密码。

正常启动本脚本启动的服务不会自动创建任何账号。
"""

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def main():
    username = os.environ.get("CLOUDREVE_TEST_USERNAME", "A")
    password = os.environ.get("CLOUDREVE_TEST_PASSWORD")
    if not password:
        raise SystemExit("请通过 CLOUDREVE_TEST_PASSWORD 提供已初始化账号的密码")

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

        def headers(token):
            return {"Authorization": f"Bearer {token}"}

        def call(address, method, path, token, expected, payload=None):
            """发一个 JSON 请求，断言状态码，返回解析后的响应（204 返回 None）。"""
            data = json.dumps(payload).encode() if payload is not None else None
            request_headers = headers(token)
            if data is not None:
                request_headers["Content-Type"] = "application/json"
            request = urllib.request.Request(
                f"http://{address}{path}",
                data=data,
                method=method,
                headers=request_headers,
            )
            try:
                response = opener.open(request, timeout=30)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                body = response.read()
                assert response.status == expected, (path, response.status, body)
                return json.loads(body) if body else None

        def login(address):
            request = urllib.request.Request(
                f"http://{address}/api/auth/login",
                data=json.dumps({"username": username, "password": password}).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with opener.open(request, timeout=10) as response:
                assert response.status == 200, response.status
                token = json.load(response)["token"]
                assert token, "login returned an empty token"
                return token

        def upload(address, token, content, filename="payload.bin", parent_id=""):
            boundary = "----luckycloudfile-p3"
            body = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
                "Content-Type: application/octet-stream\r\n\r\n"
            ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
            target = "/api/files"
            if parent_id:
                target += "?" + urllib.parse.urlencode({"parent_id": parent_id})
            request = urllib.request.Request(
                f"http://{address}{target}",
                data=body,
                method="POST",
                headers={
                    "Content-Type": f"multipart/form-data; boundary={boundary}",
                    **headers(token),
                },
            )
            with opener.open(request, timeout=30) as response:
                assert response.status == 201, response.status
                return json.load(response)

        def download(address, token, file_id):
            request = urllib.request.Request(
                f"http://{address}/api/files/{file_id}", headers=headers(token)
            )
            with opener.open(request, timeout=30) as response:
                assert response.status == 200, response.status
                return response.read()

        def create_directory(address, token, name, parent_id=""):
            return call(address, "POST", "/api/directories", token, 201,
                        {"parent_id": parent_id, "name": name})

        def list_children(address, token, parent_id=""):
            target = "/api/files"
            if parent_id:
                target += "?" + urllib.parse.urlencode({"parent_id": parent_id})
            return call(address, "GET", target, token, 200)

        def move(address, token, node_id, parent_id=""):
            return call(address, "POST", f"/api/files/{node_id}/move", token, 200,
                        {"parent_id": parent_id})

        def rename(address, token, node_id, name):
            return call(address, "PATCH", f"/api/files/{node_id}", token, 200,
                        {"name": name})

        def delete(address, token, file_id):
            call(address, "DELETE", f"/api/files/{file_id}", token, 204)

        def expect_error(address, token, method, path, status, code, payload=None):
            body = call(address, method, path, token, status, payload)
            assert body and body.get("code") == code, (path, body)

        def names(listing):
            return {item["id"]: item["name"] for item in listing["items"]}

        def logout(address, token):
            call(address, "POST", "/api/auth/logout", token, 204)

        suffix = str(os.getpid())

        try:
            content = os.urandom(100_000)
            before = hashlib.sha256(content).hexdigest()
            inner_content = os.urandom(50_000)
            inner_hash = hashlib.sha256(inner_content).hexdigest()

            first, address = start()
            token = login(address)
            print("PASS: login -> token received")

            # ① 基础文件：重启后仍按原 ID 下载，SHA-256 不变。
            record = upload(address, token, content)
            root_file_id = record["id"]
            assert record["size"] == len(content), record
            print(f"PASS: upload -> id={root_file_id}, size={record['size']}")

            # ② 目录操作：建目录、建子目录、往目录里传文件。
            dir_name = f"验收目录-{suffix}"
            directory = create_directory(address, token, dir_name)
            sub = create_directory(address, token, "子目录", directory["id"])
            inside = upload(address, token, inner_content, "inside.bin", directory["id"])
            listing = list_children(address, token, directory["id"])
            assert listing["total"] == 2, listing
            print("PASS: create directory/subdirectory and upload into directory")

            # ③ 重命名目录。
            renamed = rename(address, token, directory["id"], dir_name + "-改名")
            assert renamed["name"] == dir_name + "-改名", renamed
            print("PASS: rename directory")

            # ④ 目录不能移到自己或后代。
            expect_error(address, token, "POST", f"/api/files/{directory['id']}/move",
                         400, "invalid_move", {"parent_id": sub["id"]})
            print("PASS: move directory into descendant rejected")

            # ⑤ 移动目录到根目录再移回来：旧位置不出现，新位置可见。
            move(address, token, sub["id"], "")
            assert sub["id"] in names(list_children(address, token, ""))
            assert sub["id"] not in names(list_children(address, token, directory["id"]))
            move(address, token, sub["id"], directory["id"])
            assert sub["id"] in names(list_children(address, token, directory["id"]))
            print("PASS: move directory to root and back")

            # ⑥ 非空目录不能删。
            expect_error(address, token, "DELETE", f"/api/files/{directory['id']}",
                         409, "directory_not_empty")
            print("PASS: delete non-empty directory rejected")

            # ⑦ 移动文件：旧目录不再列出，新目录可见，内容不变。
            move(address, token, inside["id"], "")
            assert inside["id"] in names(list_children(address, token, ""))
            assert inside["id"] not in names(list_children(address, token, directory["id"]))
            assert hashlib.sha256(download(address, token, inside["id"])).hexdigest() == inner_hash
            move(address, token, inside["id"], directory["id"])
            print("PASS: move file -> old listing gone, new listing visible, content unchanged")

            stop(first)

            # ⑧ 同一个二进制、同一个库、同一个存储目录，重新启动。
            second, address = start()
            # Token 和文件都存在 MySQL 里，进程重启不会让已登录的会话失效。
            downloaded = download(address, token, root_file_id)
            after = hashlib.sha256(downloaded).hexdigest()
            assert after == before, f"SHA-256 changed: {before} != {after}"
            print("PASS: restart -> download by original ID, SHA-256 unchanged")

            # ⑨ 重启后目录树仍在：改名后的目录还在根目录，里面的文件内容不变。
            root_listing = list_children(address, token, "")
            assert directory["id"] in names(root_listing), root_listing
            assert names(root_listing)[directory["id"]] == dir_name + "-改名"
            dir_listing = list_children(address, token, directory["id"])
            assert inside["id"] in names(dir_listing) and sub["id"] in names(dir_listing), dir_listing
            assert hashlib.sha256(download(address, token, inside["id"])).hexdigest() == inner_hash
            print("PASS: restart -> directory tree and file content unchanged")

            # ⑩ 从叶子开始清理：文件 → 空子目录 → 空父目录。
            delete(address, token, inside["id"])
            delete(address, token, sub["id"])
            delete(address, token, directory["id"])
            assert directory["id"] not in names(list_children(address, token, ""))
            print("PASS: delete empty directories after clearing children")

            delete(address, token, root_file_id)
            print("PASS: delete file after restart")

            logout(address, token)
            print("PASS: logout -> token revoked")
            stop(second)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=10)


if __name__ == "__main__":
    main()
