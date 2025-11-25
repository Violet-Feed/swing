#!/usr/bin/env bash
set -euo pipefail

# 下载 Hive 运行所需的外部依赖到指定目录（默认 ./.config/hive/lib）
# 使用方法:
#   ./download-hive-libs.sh                # 下载到默认目录
#   ./download-hive-libs.sh /your/target   # 下载到自定义目录

DEST_DIR="${1:-./.config/hive/lib}"

PG_JAR_VERSION="42.7.3"
PG_JAR_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JAR_VERSION}/postgresql-${PG_JAR_VERSION}.jar"

JFS_JAR_VERSION="1.2.4"
JFS_JAR_URL="https://repo1.maven.org/maven2/io/juicefs/juicefs-hadoop/${JFS_JAR_VERSION}/juicefs-hadoop-${JFS_JAR_VERSION}.jar"

mkdir -p "$DEST_DIR"

echo "Downloading PostgreSQL JDBC ${PG_JAR_VERSION} ..."
curl -fL "$PG_JAR_URL" -o "${DEST_DIR}/postgresql-${PG_JAR_VERSION}.jar"

echo "Downloading JuiceFS Hadoop ${JFS_JAR_VERSION} ..."
curl -fL "$JFS_JAR_URL" -o "${DEST_DIR}/juicefs-hadoop-${JFS_JAR_VERSION}.jar"

echo "Done. Files saved to: $DEST_DIR"
