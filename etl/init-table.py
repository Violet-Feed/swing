#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地运行的轻量脚本：直接连 Hive Metastore 创建行为表 action。
无需 docker exec/spark-submit，使用本机 pyspark（master=local[*]）。

示例：
  python etl/init-table.py \
    --metastore-uri thrift://localhost:9083 \
    --warehouse-dir jfs://feedjfs/warehouse \
    --db dwd \
    --table action
"""

import argparse
import sys

from pyspark.sql import SparkSession


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="本地 Spark 连接 Hive Metastore 创建 action 表")
    p.add_argument("--metastore-uri", default="thrift://localhost:9083", help="Hive Metastore URI")
    p.add_argument("--warehouse-dir", default="jfs://feedjfs/warehouse", help="Hive 仓库目录")
    p.add_argument("--db", default="dwd", help="数据库名")
    p.add_argument("--table", default="action", help="表名")
    return p.parse_args(argv)


def build_spark(metastore_uri: str, warehouse_dir: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("init_action_table_local")
        .master("local[*]")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", metastore_uri)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.hive.metastore.version", "4.0.0")
        .config("spark.sql.hive.metastore.jars", "maven")  # 在线拉取 Hive 4 依赖
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("hive.metastore.schema.verification", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    return spark


def create_action_table(spark: SparkSession, db: str, table: str):
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
    spark = build_spark(args.metastore_uri, args.warehouse_dir)
    create_action_table(spark, args.db, args.table)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
