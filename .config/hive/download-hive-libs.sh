#!/usr/bin/env bash
set -euo pipefail

# 下载 Hive 运行所需的外部依赖到指定目录（默认 ./.config/hive/lib）
# PostgreSQL JDBC 从 Maven Central 下载，JuiceFS Hadoop 也从 Maven Central 下载。
#
# 用法:
#   ./download-hive-libs.sh                # 下载到默认目录
#   ./download-hive-libs.sh /your/target   # 下载到自定义目录

DEST_DIR="${1:-./.config/hive/lib}"

PG_JAR_VERSION="42.7.3"
PG_JAR_NAME="postgresql-${PG_JAR_VERSION}.jar"

# 默认 1.2.1，可通过环境变量 JFS_JAR_VERSION 覆盖
JFS_JAR_VERSION="${JFS_JAR_VERSION:-1.2.1}"
JFS_JAR_NAME="juicefs-hadoop-${JFS_JAR_VERSION}.jar"

MAVEN_BASE="https://repo.maven.apache.org/maven2"

mkdir -p "$DEST_DIR"

echo "Downloading PostgreSQL JDBC ${PG_JAR_VERSION} from Maven Central ..."
curl -fL --retry 3 --retry-delay 2 \
  "${MAVEN_BASE}/org/postgresql/postgresql/${PG_JAR_VERSION}/${PG_JAR_NAME}" \
  -o "${DEST_DIR}/${PG_JAR_NAME}"

echo "Downloading JuiceFS Hadoop ${JFS_JAR_VERSION} from Maven Central ..."
curl -fL --retry 3 --retry-delay 2 \
  "${MAVEN_BASE}/io/juicefs/juicefs-hadoop/${JFS_JAR_VERSION}/${JFS_JAR_NAME}" \
  -o "${DEST_DIR}/${JFS_JAR_NAME}"

echo "Done. Files saved to: $DEST_DIR"
