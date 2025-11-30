#!/usr/bin/env bash
set -euo pipefail

# 下载 Spark 运行所需的外部依赖到指定目录（默认 ./.config/spark/lib）
# 仅下载 JuiceFS Hadoop 依赖，固定从 Maven Central。
#
# 用法:
#   ./download-spark-libs.sh                # 下载到默认目录
#   ./download-spark-libs.sh /your/target   # 下载到自定义目录

DEST_DIR="${1:-./.config/spark/lib}"

# 默认 1.2.1，可通过环境变量 JFS_JAR_VERSION 覆盖
JFS_JAR_VERSION="${JFS_JAR_VERSION:-1.2.1}"
JFS_JAR_NAME="juicefs-hadoop-${JFS_JAR_VERSION}.jar"
MAVEN_BASE="https://repo.maven.apache.org/maven2"

mkdir -p "$DEST_DIR"

echo "Downloading JuiceFS Hadoop ${JFS_JAR_VERSION} from Maven Central ..."
curl -fL --retry 3 --retry-delay 2 \
  "${MAVEN_BASE}/io/juicefs/juicefs-hadoop/${JFS_JAR_VERSION}/${JFS_JAR_NAME}" \
  -o "${DEST_DIR}/${JFS_JAR_NAME}"

echo "Done. Files saved to: $DEST_DIR"
