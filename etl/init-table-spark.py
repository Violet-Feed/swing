#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark 集群版本的初始化脚本：通过 spark-submit 在集群/standalone 上创建 Hive action 表。

示例：
  docker cp etl/init-table-spark.py spark-standalone:/opt/spark-apps/
  docker exec spark-standalone /opt/spark/bin/spark-submit \
    --master spark://spark:7077 \
    --conf spark.sql.hive.metastore.version=3.1.3 \
    --conf spark.sql.hive.metastore.jars=maven \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=jfs://feedjfs/warehouse \
    --conf hive.metastore.uris=thrift://hive-metastore:9083 \
    /opt/spark-apps/init-table-spark.py \
      --db dwd \
      --table action

与本地脚本的区别：
  - 不显式设置 master，默认继承 spark-submit 的 master 参数
  - 依赖 spark-submit 传入的 Spark/Hadoop 配置，但也支持命令行参数覆盖
"""

import argparse
import sys

from pyspark.sql import SparkSession


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Spark 集群上初始化 Hive action 表")
    parser.add_argument("--metastore-uri", default="thrift://hive-metastore:9083", help="Hive Metastore URI")
    parser.add_argument("--warehouse-dir", default="jfs://feedjfs/warehouse", help="Hive 仓库目录")
    parser.add_argument("--db", default="dwd", help="数据库名")
    parser.add_argument("--table", default="action", help="表名")
    parser.add_argument("--app-name", default="init_action_table_cluster", help="Spark 应用名称")
    parser.add_argument("--master", default=None, help="可选：覆盖 spark-submit 提供的 master")
    return parser.parse_args(argv)


def build_spark(args) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(args.app_name)
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", args.metastore_uri)
        .config("spark.sql.warehouse.dir", args.warehouse_dir)
        .config("spark.sql.hive.metastore.version", "3.1.3")
        .config("spark.sql.hive.metastore.jars", "maven")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("hive.metastore.schema.verification", "false")
    )
    if args.master:
        builder = builder.master(args.master)
    return builder.enableHiveSupport().getOrCreate()


def create_action_table(spark: SparkSession, db: str, table: str):
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db}.{table} (
            user_id BIGINT COMMENT '用户 ID，主键',
            action_ts TIMESTAMP COMMENT '操作时间',
            action_type INT COMMENT '操作类型',
            creation_id BIGINT COMMENT '创作 ID'
        )
        COMMENT '用户操作行为表'
        PARTITIONED BY (p_date STRING COMMENT '导入日期分区 yyyyMMdd')
        STORED AS PARQUET
        TBLPROPERTIES ('parquet.compression'='SNAPPY', 'parquet.block.size'='268435456')
        """
    )


def main(argv=None):
    args = parse_args(argv)
    spark = build_spark(args)
    create_action_table(spark, args.db, args.table)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
