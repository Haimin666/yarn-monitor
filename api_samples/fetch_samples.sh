#!/bin/bash
# 采集 API 样例数据，在生产机器执行后 git push
# 使用方法: bash api_samples/fetch_samples.sh

TOKEN="2a323940d94d9a0c47f343d1c91304e2"
DS_BASE="http://olds.bigdata.shiqiao.com"
YARN_RM="http://hadoop-nn-1.bigdata.shiqiao.com:8088"
OUT="api_samples"

echo "=== Step 1: YARN 集群资源 ==="
curl -s "$YARN_RM/ws/v1/cluster/metrics" > $OUT/yarn_metrics.json
echo "Done: yarn_metrics.json"

echo "=== Step 2: YARN 运行中任务（全量，含 Spark/Flink） ==="
curl -s "$YARN_RM/ws/v1/cluster/apps?states=RUNNING" > $OUT/yarn_apps_running.json
echo "Done: yarn_apps_running.json ($(wc -c < $OUT/yarn_apps_running.json) bytes)"

echo "=== Step 3: YARN 队列资源使用 ==="
curl -s "$YARN_RM/ws/v1/cluster/scheduler" > $OUT/yarn_scheduler.json
echo "Done: yarn_scheduler.json"

echo "=== Step 4: 海豚 - lion_dw_dws 工作流实例（最近10条） ==="
curl -s "$DS_BASE/dolphinscheduler/projects/lion_dw_dws/instance/list-paging?pageSize=10&pageNo=1&searchVal=&stateType=&startDate=&endDate=" \
  -H "token: $TOKEN" > $OUT/ds_instances_dws.json
echo "Done: ds_instances_dws.json"

echo "=== Step 5: 海豚 - lion_dw_dws 任务实例（最近10条） ==="
curl -s "$DS_BASE/dolphinscheduler/projects/lion_dw_dws/task-instance/list-paging?pageSize=10&pageNo=1&searchVal=&stateType=" \
  -H "token: $TOKEN" > $OUT/ds_tasks_dws.json
echo "Done: ds_tasks_dws.json"

echo "=== Step 6: 海豚 - lion_dw_dwd 工作流实例 ==="
curl -s "$DS_BASE/dolphinscheduler/projects/lion_dw_dwd/instance/list-paging?pageSize=10&pageNo=1&searchVal=&stateType=&startDate=&endDate=" \
  -H "token: $TOKEN" > $OUT/ds_instances_dwd.json
echo "Done: ds_instances_dwd.json"

echo "=== Step 7: 海豚 - lion_dw_dim 工作流实例 ==="
curl -s "$DS_BASE/dolphinscheduler/projects/lion_dw_dim/instance/list-paging?pageSize=10&pageNo=1&searchVal=&stateType=&startDate=&endDate=" \
  -H "token: $TOKEN" > $OUT/ds_instances_dim.json
echo "Done: ds_instances_dim.json"

echo ""
echo "=== 全部完成，请 git add api_samples/ && git commit -m 'add api samples' && git push ==="
