# yarn-monitor 项目规则

## 代码修改规则（CRITICAL）

在修改任何现有代码之前，必须先说明思路并等待用户确认。确认后才执行编辑操作。

## 自动 Git 推送规则

**每次完成代码改动后，必须自动执行 git add + commit + push。**

- 不需要用户单独说"推送 git"
- commit message 用中文或英文均可，格式：`fix/feat: <简要描述>`
- 推送到 origin main（或当前分支）

## 项目背景

- **用途**：YARN 集群监控 Web 工具
- **主要文件**：
  - `yarn_scan.py`：采集 YARN 集群信息，生成 HTML 快照
  - `server.py`：HTTP 服务，提供 REST API 和静态文件服务，内置 `/proxy` 反向代理
  - `index.html`：主界面，快照列表 + iframe 展示
  - `app_detail.html`：任务详情页，日志/Stage/详情 tab
  - `slow_yarn.py`：命令行版 YARN 任务分析工具

## 关键配置

- 默认端口：9903
- RM 地址：`YARN_RM_HOST` 环境变量（默认 hadoop-nn-1.bigdata.shiqiao.com:8088）
- `/proxy` 路由：转发集群内网请求，解决浏览器无法直连 DN 节点的问题
