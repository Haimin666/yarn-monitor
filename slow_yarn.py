#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
slow_yarn.py - 分析 YARN Spark/Hive 任务情况

用法:
    python3 slow_yarn.py                        # 查运行中任务
    python3 slow_yarn.py --finished             # 查今天已完成的任务
    python3 slow_yarn.py --finished --limit 500 # 查更多历史
    python3 slow_yarn.py --rm hadoop-nn-1.xxx   # 指定 RM 地址

环境变量:
    YARN_RM_HOST   ResourceManager 地址（默认 hadoop-nn-1.bigdata.shiqiao.com）
    YARN_RM_PORT   端口（默认 8088）
"""
import json
import os
import sys
from datetime import datetime, date
from urllib.request import urlopen, Request
from urllib.error import URLError

# ====== 配置 ======
DEFAULT_RM_HOST = os.environ.get('YARN_RM_HOST', 'hadoop-nn-1.bigdata.shiqiao.com')
DEFAULT_RM_PORT = os.environ.get('YARN_RM_PORT', '8088')
SLOW_HOURS = 2
FILTER_TYPES = ('SPARK', 'HIVE')


def parse_args():
    args = sys.argv[1:]
    mode = 'running'
    limit = 200
    rm_host = DEFAULT_RM_HOST
    rm_port = DEFAULT_RM_PORT

    i = 0
    while i < len(args):
        a = args[i]
        if a == '--finished':
            mode = 'finished'
        elif a == '--limit' and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 1
        elif a == '--rm' and i + 1 < len(args):
            rm_host = args[i + 1]
            i += 1
        i += 1
    return mode, limit, rm_host, rm_port


def fetch(url, timeout=15):
    try:
        req = Request(url, headers={'Accept': 'application/json'})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except URLError as e:
        print('ERROR: 无法连接 %s: %s' % (url, e), file=sys.stderr)
        sys.exit(1)


def fmt_time(ms):
    if not ms or ms <= 0:
        return '-'
    return datetime.fromtimestamp(ms / 1000).strftime('%m-%d %H:%M')


def show_running(apps):
    result = sorted(apps, key=lambda a: -a['elapsedTime'])

    print('%-35s  %-7s  %-7s  %-5s  %-5s  %-6s  %-15s  %s' % (
        'ApplicationId', '时长', '内存', 'Cores', '类型', '用户', '开始时间', '任务名'))
    print('-' * 130)

    for a in result:
        h = a['elapsedTime'] / 3600000
        flag = '[慢]' if h > SLOW_HOURS else '    '
        mem = a['allocatedMB'] / 1024
        start = fmt_time(a.get('startedTime'))
        app_type = a.get('applicationType', '')[:5]
        user = a.get('user', '')[:6]
        print('%s %s  %.0fG     %3d  %-5s  %-6s  %-15s  %s' % (
            a['id'], flag, mem, a['allocatedVCores'],
            app_type, user, start, a['name'][:45]))

    if result:
        total_m = sum(a['allocatedMB'] for a in result)
        total_c = sum(a['allocatedVCores'] for a in result)
        slow = sum(1 for a in result if a['elapsedTime'] > SLOW_HOURS * 3600 * 1000)
        print()
        print('合计: %.1fGB / %d cores | %d 个任务 | 慢任务(>%dh): %d' % (
            total_m / 1024, total_c, len(result), SLOW_HOURS, slow))
    else:
        print('当前无运行中的 Spark/Hive 任务')


def show_finished(apps):
    today = date.today()
    apps = [a for a in apps
            if datetime.fromtimestamp(a.get('startedTime', 0) / 1000).date() == today]
    result = sorted(apps, key=lambda a: -a.get('elapsedTime', 0))

    print('今日已完成任务（共 %d 个）' % len(result))
    print()
    print('%-35s  %-7s  %-9s  %-5s  %-15s  %-15s  %-8s  %s' % (
        'ApplicationId', '时长', '状态', '类型', '开始时间', '结束时间', '用户', '任务名'))
    print('-' * 140)

    for a in result:
        h = a.get('elapsedTime', 0) / 3600000
        flag = '[慢]' if h > SLOW_HOURS else '    '
        dur = '%.1fh' % h if h >= 1 else '%dm' % int(h * 60)
        status = a.get('finalStatus', '')[:9]
        start = fmt_time(a.get('startedTime'))
        end = fmt_time(a.get('finishedTime'))
        app_type = a.get('applicationType', '')[:5]
        user = a.get('user', '')[:8]
        print('%s %s  %-7s  %-9s  %-5s  %-15s  %-15s  %-8s  %s' % (
            a['id'], flag, dur, status, app_type, start, end, user, a['name'][:40]))

    if not result:
        print('今天暂无已完成的 Spark/Hive 任务')


def parse_queues_fair(child_queues):
    """
    解析 Fair Scheduler 队列结构。
    child_queues 格式: {"queue": [...]}
    """
    result = []
    for q in (child_queues.get('queue') or []):
        name = q.get('queueName', '')
        used_mb = q.get('usedResources', {}).get('memory', 0)
        max_mb = q.get('maxResources', {}).get('memory', 0)
        min_mb = q.get('minResources', {}).get('memory', 0)
        used_vc = q.get('usedResources', {}).get('vCores', 0)
        max_vc = q.get('maxResources', {}).get('vCores', 0)
        num_active = q.get('numActiveApps', q.get('numActiveApplications', 0))
        num_pending = q.get('numPendingApps', q.get('numPendingApplications', 0))
        used_pct = used_mb / max_mb * 100 if max_mb else 0

        # 递归处理子队列
        child = q.get('childQueues', {})
        if child:
            result.extend(parse_queues_fair(child))
        else:
            result.append({
                'name': name,
                'used_mb': used_mb,
                'max_mb': max_mb,
                'min_mb': min_mb,
                'used_vc': used_vc,
                'max_vc': max_vc,
                'used_pct': used_pct,
                'num_active': num_active,
                'num_pending': num_pending,
            })
    return result


def show_cluster_metrics(base):
    """展示集群总体资源概览 + 各队列情况"""
    try:
        data = fetch('%s/ws/v1/cluster/metrics' % base)
        m = data.get('clusterMetrics', {})
        total_mem = m.get('totalMB', 0) / 1024
        used_mem = m.get('allocatedMB', 0) / 1024
        mem_pct = used_mem / total_mem * 100 if total_mem else 0
        total_vc = m.get('totalVirtualCores', 0)
        used_vc = m.get('allocatedVirtualCores', 0)
        vc_pct = used_vc / total_vc * 100 if total_vc else 0
        apps_running = m.get('appsRunning', 0)
        apps_pending = m.get('appsPending', 0)

        mem_bar = int(mem_pct / 5)
        vc_bar = int(vc_pct / 5)

        print('┌─ 集群资源概览 ' + '─' * 65)
        print('│ 内存: [%s%s] %.1f%% (%.0f/%.0f GB)' % (
            '█' * mem_bar, '░' * (20 - mem_bar), mem_pct, used_mem, total_mem))
        print('│ Cores: [%s%s] %.1f%% (%d/%d vCores)' % (
            '█' * vc_bar, '░' * (20 - vc_bar), vc_pct, used_vc, total_vc))
        print('│ 全集群: 运行中 %d 个 | 等待 %d 个' % (apps_running, apps_pending))
        if mem_pct > 85:
            print('│ *** 内存使用率超过 85%%，集群面临压力！***')
        print('│')
    except Exception as e:
        print('（集群概览获取失败: %s）' % e)

    # 队列详情
    try:
        sched_data = fetch('%s/ws/v1/cluster/scheduler' % base)
        sched = sched_data.get('scheduler', {}).get('schedulerInfo', {})
        root_queue = sched.get('rootQueue', {})
        child_queues = root_queue.get('childQueues', {})

        queues = parse_queues_fair(child_queues)

        if queues:
            queues_sorted = sorted(queues, key=lambda q: -q['used_pct'])
            print('│ %-22s  %-22s  %-5s  %-5s  %-7s  %s' % (
                '队列', '内存(已用/上限)', 'Cores', '运行', 'Pending', '占用%'))
            print('│ ' + '-' * 75)
            for q in queues_sorted:
                bar_len = min(int(q['used_pct'] / 10), 10)
                bar = '█' * bar_len + '░' * (10 - bar_len)
                pending_flag = ' !' if q['num_pending'] > 0 else '  '
                mem_str = '%.0fG/%.0fG' % (q['used_mb']/1024, q['max_mb']/1024)
                print('│ %-22s  [%s] %-12s  %-5d  %-4d  %-4d%s  %.0f%%' % (
                    q['name'][:22], bar, mem_str,
                    q['used_vc'], q['num_active'], q['num_pending'], pending_flag,
                    q['used_pct']))
    except Exception as e:
        print('│ （队列信息获取失败: %s）' % e)
            print('│ ' + '-' * 72)
            for q in sorted(queues, key=lambda x: -x['used_cap']):
                bar_len = int(q['used_cap'] / 5) if q['used_cap'] else 0
                bar = '█' * bar_len + '░' * (10 - bar_len)
                pending_flag = ' !' if q['num_pending'] > 0 else '  '
    except Exception as e:
        print('│ （队列信息获取失败: %s）' % e)

    print('└' + '─' * 78)
    print()


def main():
    mode, limit, rm_host, rm_port = parse_args()
    base = 'http://%s:%s' % (rm_host, rm_port)

    print('=== YARN %s任务分析 ===' % ('历史' if mode == 'finished' else '运行中'))
    print('时间: %s | RM: %s | 类型: %s' % (
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), base, ', '.join(FILTER_TYPES)))
    print()

    show_cluster_metrics(base)

    if mode == 'finished':
        url = '%s/ws/v1/cluster/apps?state=FINISHED&limit=%d' % (base, limit)
    else:
        url = '%s/ws/v1/cluster/apps?state=RUNNING' % base

    data = fetch(url)
    apps = (data.get('apps') or {}).get('app') or []
    apps = [a for a in apps if a.get('applicationType', '').upper() in FILTER_TYPES]

    if mode == 'finished':
        show_finished(apps)
    else:
        show_running(apps)


if __name__ == '__main__':
    main()
