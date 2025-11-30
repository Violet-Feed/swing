#!/usr/bin/env bash
set -euo pipefail

# HiveServer2 启动脚本
# 安装 curl 并下载依赖 jar

# 安装 curl（如果不存在）
if ! command -v curl &>/dev/null; then
  echo "Installing curl..."
  apt-get update -qq && apt-get install -y -qq curl
fi

# 下载依赖
/opt/hive/download-hive-libs.sh /opt/hive/lib

mkdir -p /tmp/hive-local
chmod -R 777 /tmp/hive-local

# 预创建 JuiceFS 目录，失败不终止
hadoop fs -mkdir -p jfs://feedjfs/tmp/hive 2>/dev/null || true
hadoop fs -chmod -R 777 jfs://feedjfs/tmp 2>/dev/null || true

exec /entrypoint.sh
