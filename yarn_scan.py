#!/usr/bin/env python3
"""
YARN 集群资源监控 - 生成 HTML 快照
每次运行生成一个带时间戳的快照文件到 snapshots/ 目录
"""
import json
import os
import sys
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

# ====== 配置 ======
RM_HOST = os.environ.get("YARN_RM_HOST", "hadoop-nn-1.bigdata.shiqiao.com")
RM_PORT = os.environ.get("YARN_RM_PORT", "8088")
RM_BASE = f"http://{RM_HOST}:{RM_PORT}"
SNAPSHOTS_DIR = os.path.join(os.path.dirname(__file__), "snapshots")
SLOW_THRESHOLD_HOURS = 2  # 超过这个小时数认为是慢任务


def fetch_json(path, timeout=15):
    url = f"{RM_BASE}/ws/v1/{path}"
    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except URLError as e:
        print(f"ERROR: 无法连接 {url}: {e}", file=sys.stderr)
        return None


def parse_queues_fair(child_queues):
    """解析 Fair Scheduler 队列（递归）"""
    result = []
    for q in (child_queues.get('queue') or []):
        name = q.get('queueName', '')
        used_mb = q.get('usedResources', {}).get('memory', 0)
        max_mb = q.get('maxResources', {}).get('memory', 0)
        used_vc = q.get('usedResources', {}).get('vCores', 0)
        max_vc = q.get('maxResources', {}).get('vCores', 0)
        num_active = q.get('numActiveApps', q.get('numActiveApplications', 0))
        num_pending = q.get('numPendingApps', q.get('numPendingApplications', 0))
        used_pct = round(used_mb / max_mb * 100, 1) if max_mb else 0
        child = q.get('childQueues', {})
        if child:
            result.extend(parse_queues_fair(child))
        else:
            result.append({
                'name': name, 'used_mb': used_mb, 'max_mb': max_mb,
                'used_vc': used_vc, 'max_vc': max_vc,
                'num_active': num_active, 'num_pending': num_pending,
                'used_pct': used_pct,
            })
    return result


def format_duration(ms):
    if ms < 0:
        return "N/A"
    s = ms // 1000
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    return f"{m}m {sec:02d}s"


def format_mem(mb):
    if mb >= 1024:
        return f"{mb/1024:.1f} GB"
    return f"{mb} MB"


def generate_html(metrics, apps, scheduler, ts):
    now_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    # 集群概览数据
    cm = metrics.get("clusterMetrics", {}) if metrics else {}
    total_mem = cm.get("totalMB", 0)
    used_mem = cm.get("allocatedMB", 0)
    avail_mem = cm.get("availableMB", 0)
    total_vcores = cm.get("totalVirtualCores", 0)
    used_vcores = cm.get("allocatedVirtualCores", 0)
    apps_running = cm.get("appsRunning", 0)
    apps_pending = cm.get("appsPending", 0)
    mem_pct = round(used_mem / total_mem * 100, 1) if total_mem else 0
    vcores_pct = round(used_vcores / total_vcores * 100, 1) if total_vcores else 0

    # 处理 application 列表
    app_list = []
    if apps and "apps" in apps and apps["apps"] and "app" in apps["apps"]:
        app_list = apps["apps"]["app"]

    slow_apps = [a for a in app_list if a.get("elapsedTime", 0) > SLOW_THRESHOLD_HOURS * 3600 * 1000]
    slow_apps.sort(key=lambda x: -x.get("elapsedTime", 0))

    all_running = [a for a in app_list if a.get("state") == "RUNNING"]
    all_running.sort(key=lambda x: -x.get("elapsedTime", 0))

    # 解析队列信息
    queues = []
    if scheduler:
        try:
            root_queue = scheduler.get("scheduler", {}).get("schedulerInfo", {}).get("rootQueue", {})
            queues = parse_queues_fair(root_queue.get("childQueues", {}))
            queues.sort(key=lambda q: -q["used_pct"])
        except Exception:
            pass

    def mem_bar(pct, color):
        return f"""<div style="background:#eee;border-radius:4px;height:12px;width:200px;display:inline-block;vertical-align:middle">
  <div style="background:{color};width:{min(pct,100)}%;height:100%;border-radius:4px"></div></div>
  <span style="margin-left:8px;font-weight:bold">{pct}%</span>"""

    def queue_bar(pct):
        w = min(pct, 100)
        color = "#e74c3c" if pct > 80 else "#3498db"
        return f'<div style="background:#eee;border-radius:3px;height:10px;width:120px;display:inline-block;vertical-align:middle"><div style="background:{color};width:{w}%;height:100%;border-radius:3px"></div></div>'

    # 队列 HTML
    if queues:
        queue_rows = ""
        for q in queues:
            pending_style = "color:red;font-weight:bold" if q["num_pending"] > 0 else ""
            queue_rows += f"""<tr>
  <td style="font-family:monospace">{q['name']}</td>
  <td>{queue_bar(q['used_pct'])} <span style="font-size:12px">{q['used_pct']}%</span></td>
  <td>{q['used_mb']//1024}G / {q['max_mb']//1024}G</td>
  <td>{q['used_vc']} / {q['max_vc']}</td>
  <td>{q['num_active']}</td>
  <td style="{pending_style}">{q['num_pending']}{' ⚠️' if q['num_pending'] > 0 else ''}</td>
</tr>"""
        queue_section = f"""<section>
  <h2>📋 队列资源分配（Fair Scheduler）</h2>
  <table>
    <thead><tr><th>队列名</th><th>内存占用率</th><th>内存(已用/上限)</th><th>vCores</th><th>运行</th><th>Pending</th></tr></thead>
    <tbody>{queue_rows}</tbody>
  </table>
</section>"""
    else:
        queue_section = ""

    def app_row(a, highlight=False):
        elapsed = a.get("elapsedTime", 0)
        name = a.get("name", "")[:60]
        app_id = a.get("id", "")
        state = a.get("state", "")
        queue = a.get("queue", "")
        user = a.get("user", "")
        amem = a.get("allocatedMB", 0)
        avc = a.get("allocatedVCores", 0)
        progress = a.get("progress", 0)
        row_style = 'background:#fff3cd' if highlight else ''
        return f"""<tr style="{row_style}">
  <td><a href="{RM_BASE}/proxy/{app_id}/" target="_blank" style="color:#0066cc;font-family:monospace;font-size:12px">{app_id}</a></td>
  <td title="{a.get('name','')}">{name}</td>
  <td>{user}</td>
  <td>{queue}</td>
  <td>{state}</td>
  <td style="{'color:red;font-weight:bold' if elapsed > SLOW_THRESHOLD_HOURS*3600*1000 else ''}">{format_duration(elapsed)}</td>
  <td>{format_mem(amem)}</td>
  <td>{avc}</td>
  <td>{progress:.0f}%</td>
</tr>"""

    slow_rows = "\n".join(app_row(a, highlight=True) for a in slow_apps) if slow_apps else \
        '<tr><td colspan="9" style="text-align:center;color:#888">暂无慢任务（运行 >{SLOW_THRESHOLD_HOURS}h）</td></tr>'

    all_rows = "\n".join(app_row(a) for a in all_running) if all_running else \
        '<tr><td colspan="9" style="text-align:center;color:#888">当前无运行中的任务</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>YARN 快照 {now_str}</title>
<style>
  body {{ font-family: -apple-system, sans-serif; margin: 0; padding: 16px; background: #f5f5f5; color: #333; font-size: 14px; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }}
  .ts {{ color: #888; font-size: 12px; margin-bottom: 20px; }}
  .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }}
  .card {{ background: white; border-radius: 8px; padding: 16px 24px; box-shadow: 0 1px 4px rgba(0,0,0,.1); min-width: 200px; }}
  .card-val {{ font-size: 28px; font-weight: bold; }}
  .card-label {{ color: #888; font-size: 12px; margin-top: 4px; }}
  section {{ background: white; border-radius: 8px; padding: 16px; box-shadow: 0 1px 4px rgba(0,0,0,.1); margin-bottom: 20px; }}
  h2 {{ font-size: 16px; margin: 0 0 12px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #f0f0f0; text-align: left; padding: 8px; border-bottom: 2px solid #ddd; }}
  td {{ padding: 7px 8px; border-bottom: 1px solid #eee; word-break: break-all; }}
  tr:hover {{ background: #f9f9f9; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; }}
  .badge-warn {{ background: #fff3cd; color: #856404; }}
</style>
</head>
<body>
<h1>YARN 集群资源快照</h1>
<div class="ts">采集时间：{now_str} &nbsp;|&nbsp; RM: {RM_BASE}</div>

<div class="cards">
  <div class="card">
    <div class="card-val">{apps_running}</div>
    <div class="card-label">运行中任务</div>
  </div>
  <div class="card">
    <div class="card-val" style="color:{'#e74c3c' if apps_pending > 0 else '#27ae60'}">{apps_pending}</div>
    <div class="card-label">等待队列</div>
  </div>
  <div class="card">
    <div class="card-val" style="color:{'#e74c3c' if mem_pct > 80 else '#333'}">{mem_pct}%</div>
    <div class="card-label">内存占用 {format_mem(used_mem)} / {format_mem(total_mem)}</div>
    <div style="margin-top:8px">{mem_bar(mem_pct, '#e74c3c' if mem_pct > 80 else '#3498db')}</div>
  </div>
  <div class="card">
    <div class="card-val" style="color:{'#e74c3c' if vcores_pct > 80 else '#333'}">{vcores_pct}%</div>
    <div class="card-label">vCores 占用 {used_vcores} / {total_vcores}</div>
    <div style="margin-top:8px">{mem_bar(vcores_pct, '#e74c3c' if vcores_pct > 80 else '#2ecc71')}</div>
  </div>
  <div class="card">
    <div class="card-val" style="color:{'#e74c3c' if len(slow_apps) > 0 else '#27ae60'}">{len(slow_apps)}</div>
    <div class="card-label">慢任务（>{SLOW_THRESHOLD_HOURS}h）</div>
  </div>
</div>

<section>
  <h2>⚠️ 慢任务列表（运行超过 {SLOW_THRESHOLD_HOURS} 小时）</h2>
  <table>
    <thead><tr>
      <th>Application ID</th><th>名称</th><th>用户</th><th>队列</th>
      <th>状态</th><th>运行时长</th><th>内存</th><th>vCores</th><th>进度</th>
    </tr></thead>
    <tbody>{slow_rows}</tbody>
  </table>
</section>

{queue_section}

<section>
  <h2>所有运行中任务（共 {len(all_running)} 个，按运行时长降序）</h2>
  <table>
    <thead><tr>
      <th>Application ID</th><th>名称</th><th>用户</th><th>队列</th>
      <th>状态</th><th>运行时长</th><th>内存</th><th>vCores</th><th>进度</th>
    </tr></thead>
    <tbody>{all_rows}</tbody>
  </table>
</section>

</body>
</html>"""


def main():
    os.makedirs(SNAPSHOTS_DIR, exist_ok=True)
    ts = datetime.now()
    ts_str = ts.strftime("%Y-%m-%d_%H-%M")

    print(f"[{ts}] 开始采集 YARN 数据...")
    metrics = fetch_json("cluster/metrics")
    apps = fetch_json("cluster/apps?state=RUNNING")
    scheduler = fetch_json("cluster/scheduler")

    if metrics is None and apps is None:
        print("ERROR: 无法连接到 YARN RM，请检查网络", file=sys.stderr)
        sys.exit(1)

    html = generate_html(metrics, apps, scheduler, ts)
    out_path = os.path.join(SNAPSHOTS_DIR, f"{ts_str}.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"快照已生成: {out_path}")

    # 更新 index.json（供 index.html 读取快照列表）
    snapshots = sorted(
        [f for f in os.listdir(SNAPSHOTS_DIR) if f.endswith(".html")],
        reverse=True
    )
    index_json = os.path.join(os.path.dirname(__file__), "snapshots.json")
    with open(index_json, "w", encoding="utf-8") as f:
        json.dump(snapshots, f, ensure_ascii=False, indent=2)

    print(f"快照列表已更新: {index_json}（共 {len(snapshots)} 个快照）")


if __name__ == "__main__":
    main()
