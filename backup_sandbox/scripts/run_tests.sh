#!/usr/bin/env bash
# 一键测试：25 项 unittest（服务规则 + 重启恢复 + HTTP 端到端）
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 -m unittest discover -s backup_sandbox/tests -p 'test_*.py' -v
