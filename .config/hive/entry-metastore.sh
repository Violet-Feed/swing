#!/usr/bin/env bash
set -euo pipefail

# Hive Metastore 启动脚本
# 自动判断是否需要初始化 schema

# 安装 curl（如果不存在）
if ! command -v curl &>/dev/null; then
  echo "Installing curl..."
  apt-get update -qq && apt-get install -y -qq curl
fi

# 下载依赖
/opt/hive/download-hive-libs.sh /opt/hive/lib

# 检查 metastore schema 是否已初始化（通过查询 VERSION 表）
check_schema_initialized() {
  # 需要 PostgreSQL 客户端，但容器里没有，用 Java 检查
  # 通过 schematool -info 来判断
  /opt/hive/bin/schematool -dbType postgres -info &>/dev/null
  return $?
}

# 自动判断是否跳过 schema 初始化
if check_schema_initialized; then
  echo "Schema already initialized, skipping..."
  export IS_RESUME="true"
else
  echo "Schema not initialized, will initialize..."
  export IS_RESUME="false"
fi

# 交给官方入口启动
exec /entrypoint.sh
