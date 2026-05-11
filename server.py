#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
server.py - YARN Monitor HTTP 服务器

功能：
  - 静态文件服务（index.html、snapshots/）
  - GET  /api/snapshots  → 返回快照列表 JSON
  - POST /api/scan       → 触发 yarn_scan.py 生成新快照

用法：
    python3 server.py          # 默认 9903 端口
    python3 server.py 8080     # 指定端口
    python3 server.py --help
"""
import json
import os
import subprocess
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
SNAPSHOTS_DIR = os.path.join(ROOT, "snapshots")
SNAPSHOTS_JSON = os.path.join(ROOT, "snapshots.json")
YARN_SCAN = os.path.join(ROOT, "yarn_scan.py")

# 全局状态：防止并发触发多次采集
_scan_lock = threading.Lock()
_scanning = False
_last_scan = None


def get_snapshots():
    if os.path.exists(SNAPSHOTS_JSON):
        with open(SNAPSHOTS_JSON) as f:
            return json.load(f)
    # 降级：直接扫目录
    if os.path.isdir(SNAPSHOTS_DIR):
        return sorted([f for f in os.listdir(SNAPSHOTS_DIR) if f.endswith(".html")], reverse=True)
    return []


def trigger_scan():
    global _scanning, _last_scan
    with _scan_lock:
        if _scanning:
            return {"status": "running", "message": "采集正在进行中，请稍候"}
        _scanning = True

    try:
        print("[server] 触发采集: python3 yarn_scan.py")
        result = subprocess.run(
            [sys.executable, YARN_SCAN],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        _last_scan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if result.returncode == 0:
            print("[server] 采集完成")
            return {"status": "ok", "message": "采集完成", "at": _last_scan}
        else:
            err = (result.stderr or result.stdout)[:500]
            print(f"[server] 采集失败: {err}")
            return {"status": "error", "message": err, "at": _last_scan}
    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "采集超时（>120s）", "at": _last_scan}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        with _scan_lock:
            _scanning = False


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {self.address_string()} {fmt % args}")

    def send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def serve_file(self, path):
        if not os.path.exists(path) or not os.path.isfile(path):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return
        ext = os.path.splitext(path)[1].lower()
        content_types = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript",
            ".css": "text/css",
            ".json": "application/json",
            ".png": "image/png",
        }
        ct = content_types.get(ext, "application/octet-stream")
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/api/snapshots":
            snaps = get_snapshots()
            self.send_json(200, {
                "snapshots": snaps,
                "count": len(snaps),
                "scanning": _scanning,
                "last_scan": _last_scan,
            })
        elif path == "/api/status":
            self.send_json(200, {"scanning": _scanning, "last_scan": _last_scan})
        elif path == "/" or path == "/index.html":
            self.serve_file(os.path.join(ROOT, "index.html"))
        elif path.startswith("/snapshots/"):
            fname = path[len("/snapshots/"):]
            self.serve_file(os.path.join(SNAPSHOTS_DIR, fname))
        elif path == "/snapshots.json":
            self.serve_file(SNAPSHOTS_JSON)
        else:
            # 尝试从 ROOT 提供静态文件
            local = os.path.join(ROOT, path.lstrip("/"))
            if os.path.isfile(local):
                self.serve_file(local)
            else:
                self.send_response(404)
                self.end_headers()

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/api/scan":
            # 在后台线程执行采集，立即返回 accepted
            t = threading.Thread(target=trigger_scan, daemon=True)
            t.start()
            self.send_json(202, {"status": "accepted", "message": "采集已触发，请稍候刷新列表"})
        else:
            self.send_response(404)
            self.end_headers()


def main():
    port = 9903
    if len(sys.argv) > 1:
        if sys.argv[1] in ("--help", "-h"):
            print(__doc__)
            sys.exit(0)
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    os.makedirs(SNAPSHOTS_DIR, exist_ok=True)
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"YARN Monitor 服务已启动: http://0.0.0.0:{port}")
    print(f"  快照目录: {SNAPSHOTS_DIR}")
    print(f"  Ctrl+C 停止")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止")


if __name__ == "__main__":
    main()
