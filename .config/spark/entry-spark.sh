#!/usr/bin/env bash
set -euo pipefail

# 下载 Spark 依赖并启动 master/worker/history（脚本已在宿主机 chmod +x 后只读挂载）
/opt/spark/download-spark-libs.sh /opt/spark/jars

mkdir -p /tmp/spark-events
mkdir -p /home/spark
mkdir -p /home/spark/.ivy2/cache
chown -R spark:spark /home/spark /tmp/spark-events 2>/dev/null || true

/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master &
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark:7077 &
/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer &

wait
