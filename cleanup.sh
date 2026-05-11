#!/bin/bash
# cleanup.sh - 清理超过 30 天的快照文件
# 建议 crontab: 0 2 * * * /path/to/yarn-monitor/cleanup.sh >> /path/to/yarn-monitor/cleanup.log 2>&1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SNAPSHOTS_DIR="$SCRIPT_DIR/snapshots"
KEEP_DAYS=30

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始清理 $KEEP_DAYS 天前的快照..."

if [ ! -d "$SNAPSHOTS_DIR" ]; then
  echo "快照目录不存在: $SNAPSHOTS_DIR"
  exit 0
fi

# 找出并删除超过 KEEP_DAYS 天的文件
deleted=0
while IFS= read -r -d '' file; do
  rm "$file"
  echo "  已删除: $(basename "$file")"
  ((deleted++))
done < <(find "$SNAPSHOTS_DIR" -name "*.html" -mtime +$KEEP_DAYS -print0)

echo "共删除 $deleted 个旧快照"

# 重新生成 snapshots.json
python3 - <<'EOF'
import json, os
snap_dir = os.path.join(os.path.dirname(os.path.abspath("$SCRIPT_DIR")), "snapshots")
# 直接用相对路径
import sys
script_dir = sys.argv[1] if len(sys.argv) > 1 else "."
snap_dir = os.path.join(script_dir, "snapshots")
snaps = sorted([f for f in os.listdir(snap_dir) if f.endswith(".html")], reverse=True) if os.path.exists(snap_dir) else []
out = os.path.join(script_dir, "snapshots.json")
with open(out, "w") as f:
    json.dump(snaps, f, indent=2)
print(f"snapshots.json 已更新，剩余 {len(snaps)} 个快照")
EOF

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 清理完成"
