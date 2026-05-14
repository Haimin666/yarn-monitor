#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
server.py - YARN + DolphinScheduler 一站式监控服务器

功能：
  - 静态文件服务（index.html、snapshots/）
  - GET  /api/snapshots      → 返回快照列表 JSON
  - POST /api/scan           → 触发 yarn_scan.py 生成新快照
  - GET  /api/dashboard      → 返回聚合监控数据（YARN资源+任务+海豚实例）
  - GET  /app?id=xxx         → 返回 app_detail.html（任务详情页）
  - GET  /api/app?id=xxx     → 返回任务详情 JSON
  - GET  /api/app_logs?id=xxx → 代理 YARN AM 日志
  - GET  /proxy?url=xxx      → 反向代理集群内网请求

用法：
    python3 server.py          # 默认 9903 端口
    python3 server.py 8080     # 指定端口
"""
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError

ROOT = os.path.dirname(os.path.abspath(__file__))
SNAPSHOTS_DIR = os.path.join(ROOT, "snapshots")
SNAPSHOTS_JSON = os.path.join(ROOT, "snapshots.json")
YARN_SCAN = os.path.join(ROOT, "yarn_scan.py")

# ── 读取 .env 配置文件 ────────────────────────────────────────
def _load_env(path):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip()
                if key and key not in os.environ:
                    os.environ[key] = val

_load_env(os.path.join(ROOT, ".env"))

RM_HOST = os.environ.get("YARN_RM_HOST", "hadoop-nn-1.bigdata.shiqiao.com")
RM_PORT = os.environ.get("YARN_RM_PORT", "8088")
RM_BASE = f"http://{RM_HOST}:{RM_PORT}"

DS_BASE = os.environ.get("DS_BASE_URL", "http://olds.bigdata.shiqiao.com")
DS_TOKEN = os.environ.get("DS_TOKEN", "2a323940d94d9a0c47f343d1c91304e2")
DS_PROJECTS = [p.strip() for p in os.environ.get("DS_PROJECTS", "lion_dw_ods,lion_dw_dws,lion_dw_dwd").split(",") if p.strip()]

# 项目名 → ID 映射（用于生成海豚跳转链接）
def _parse_project_ids():
    raw = os.environ.get("DS_PROJECT_IDS", "")
    result = {}
    for item in raw.split(","):
        item = item.strip()
        if ":" in item:
            name, _, pid = item.rpartition(":")
            try:
                result[name.strip()] = int(pid.strip())
            except ValueError:
                pass
    return result

DS_PROJECT_IDS = _parse_project_ids()

DASHBOARD_REFRESH = int(os.environ.get("DASHBOARD_REFRESH", "30"))

# ── 全局状态 ───────────────────────────────────────────────────────────────────
_scan_lock = threading.Lock()
_scanning = False
_last_scan = None

# dashboard 缓存
_dashboard_cache = {}
_dashboard_lock = threading.Lock()
_dashboard_last_update = None


# ── YARN API ──────────────────────────────────────────────────────────────────

def fetch_yarn_metrics():
    """获取 YARN 集群资源指标"""
    url = f"{RM_BASE}/ws/v1/cluster/metrics"
    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("clusterMetrics", {})
    except Exception as e:
        return {"error": str(e)}


def fetch_yarn_apps_today():
    """获取 YARN 今日应用列表（运行中 + 已完成/失败）"""
    import calendar
    now = datetime.now()
    today_start_ms = int(calendar.timegm(now.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()) * 1000)
    # 查询今日启动的所有任务（不限状态）
    url = (f"{RM_BASE}/ws/v1/cluster/apps"
           f"?startedTimeBegin={today_start_ms}"
           f"&states=RUNNING,FINISHED,FAILED,KILLED,SUBMITTED,ACCEPTED")
    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            apps = data.get("apps", {})
            if not apps:
                return []
            raw = apps.get("app", [])
            # 精简字段，减少传输量
            result = []
            for app in raw:
                result.append({
                    "id": app.get("id", ""),
                    "name": app.get("name", ""),
                    "user": app.get("user", ""),
                    "queue": app.get("queue", ""),
                    "state": app.get("state", ""),
                    "applicationType": app.get("applicationType", ""),
                    "progress": app.get("progress", 0),
                    "startedTime": app.get("startedTime", 0),
                    "elapsedTime": app.get("elapsedTime", 0),
                    "trackingUrl": app.get("trackingUrl", ""),
                    "allocatedMB": app.get("allocatedMB", 0),
                    "allocatedVCores": app.get("allocatedVCores", 0),
                    "runningContainers": app.get("runningContainers", 0),
                })
            # 按启动时间降序
            result.sort(key=lambda x: x["startedTime"], reverse=True)
            return result
    except Exception as e:
        return []


def fetch_app_detail(app_id):
    """从 YARN RM 获取单个 App 的详细信息"""
    url = f"{RM_BASE}/ws/v1/cluster/apps/{app_id}"
    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except URLError as e:
        return {"error": str(e)}


def fetch_app_logs_url(app_id):
    """获取 AM 日志 URL（返回 tracking URL 或 log aggregation URL）"""
    detail = fetch_app_detail(app_id)
    if "error" in detail:
        return None, detail["error"]
    app = detail.get("app", {})
    log_url = app.get("amContainerLogs") or app.get("trackingUrl") or ""
    return log_url, None


# ── DolphinScheduler API ──────────────────────────────────────────────────────

def _ds_headers():
    return {
        "token": DS_TOKEN,
        "Accept": "application/json",
        "language": "zh_CN",
    }


def fetch_ds_instances(project_name, page_size=50, state_type=""):
    """获取海豚调度今日工作流实例列表"""
    # 今日时间范围（服务器本地时间）
    today = datetime.now().strftime("%Y-%m-%d")
    start_date = today + " 00:00:00"
    end_date = today + " 23:59:59"
    url = (f"{DS_BASE}/dolphinscheduler/projects/{project_name}/instance/list-paging"
           f"?pageSize={page_size}&pageNo=1&searchVal=&stateType={state_type}"
           f"&startDate={start_date}&endDate={end_date}&executorName=")
    try:
        req = Request(url, headers=_ds_headers())
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if data.get("code") == 0:
                items = data.get("data", {}).get("totalList", [])
                # 精简字段
                result = []
                for item in items:
                    result.append({
                        "id": item.get("id"),
                        "name": item.get("name", ""),
                        "state": item.get("state", ""),
                        "startTime": item.get("startTime", ""),
                        "endTime": item.get("endTime", ""),
                        "duration": item.get("duration", 0),
                        "executorName": item.get("executorName", ""),
                        "commandType": item.get("commandType", ""),
                        "processDefinitionId": item.get("processDefinitionId"),
                        "project": project_name,
                    })
                return result
            return []
    except Exception:
        return []


# ── 后台自动刷新 ───────────────────────────────────────────────────────────────

def _refresh_dashboard():
    """后台线程：定期刷新 dashboard 数据"""
    while True:
        try:
            metrics = fetch_yarn_metrics()
            apps = fetch_yarn_apps_today()

            ds_instances = []
            for proj in DS_PROJECTS:
                proj = proj.strip()
                if proj:
                    items = fetch_ds_instances(proj, page_size=20)
                    ds_instances.extend(items)
            # 按开始时间降序
            ds_instances.sort(key=lambda x: x.get("startTime") or "", reverse=True)

            with _dashboard_lock:
                global _dashboard_last_update
                _dashboard_cache["metrics"] = metrics
                _dashboard_cache["apps"] = apps
                _dashboard_cache["ds_instances"] = ds_instances
                _dashboard_last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            print(f"[dashboard] 刷新失败: {e}")

        time.sleep(DASHBOARD_REFRESH)


def get_snapshots():
    if os.path.exists(SNAPSHOTS_JSON):
        with open(SNAPSHOTS_JSON) as f:
            return json.load(f)
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


# ── HTTP 处理器 ────────────────────────────────────────────────────────────────

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
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

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

        elif path == "/api/dashboard":
            with _dashboard_lock:
                data = dict(_dashboard_cache)
                data["last_update"] = _dashboard_last_update
                data["refresh_interval"] = DASHBOARD_REFRESH
                data["ds_projects"] = DS_PROJECTS
                data["ds_project_ids"] = DS_PROJECT_IDS
                data["ds_base"] = DS_BASE
            self.send_json(200, data)

        elif path == "/api/app":
            app_id = qs.get("id", [None])[0]
            if not app_id:
                self.send_json(400, {"error": "缺少 id 参数"})
                return
            data = fetch_app_detail(app_id)
            self.send_json(200, data)

        elif path == "/api/app_logs":
            app_id = qs.get("id", [None])[0]
            if not app_id:
                self.send_json(400, {"error": "缺少 id 参数"})
                return
            log_url, err = fetch_app_logs_url(app_id)
            if err:
                self.send_json(500, {"error": err})
                return
            self.send_json(200, {"app_id": app_id, "log_url": log_url, "rm_base": RM_BASE})

        elif path == "/proxy":
            target = qs.get("url", [None])[0]
            if not target:
                self.send_json(400, {"error": "缺少 url 参数"})
                return
            from urllib.parse import urlparse as _up
            _parsed = _up(target)
            _host = _parsed.hostname or ""
            if not (_host == RM_HOST or _host.startswith("hadoop-")):
                self.send_json(403, {"error": f"不允许代理到 {_host}"})
                return
            try:
                req = Request(target, headers={"Accept": "text/html,*/*", "User-Agent": "YARNProxy/1.0"})
                with urlopen(req, timeout=20) as resp:
                    ct = resp.headers.get("Content-Type", "text/html; charset=utf-8")
                    body = resp.read()
                self.send_response(200)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", len(body))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                self.send_json(502, {"error": f"代理请求失败: {e}"})

        elif path == "/app":
            self.serve_file(os.path.join(ROOT, "app_detail.html"))

        elif path == "/" or path == "/index.html":
            self.serve_file(os.path.join(ROOT, "index.html"))

        elif path.startswith("/snapshots/"):
            fname = path[len("/snapshots/"):]
            self.serve_file(os.path.join(SNAPSHOTS_DIR, fname))

        elif path == "/snapshots.json":
            self.serve_file(SNAPSHOTS_JSON)

        else:
            local = os.path.join(ROOT, path.lstrip("/"))
            if os.path.isfile(local):
                self.serve_file(local)
            else:
                self.send_response(404)
                self.end_headers()

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/api/scan":
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

    # 启动后台 dashboard 刷新线程
    t = threading.Thread(target=_refresh_dashboard, daemon=True)
    t.start()
    print(f"[server] Dashboard 自动刷新已启动（间隔 {DASHBOARD_REFRESH}s）")

    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"YARN Monitor 服务已启动: http://0.0.0.0:{port}")
    print(f"  快照目录: {SNAPSHOTS_DIR}")
    print(f"  海豚项目: {DS_PROJECTS}")
    print(f"  Ctrl+C 停止")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止")


if __name__ == "__main__":
    main()
