#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hive 7天点击行为聚合 ETL

从 action 表中提取最近7天的点击行为（action_type=2），按 (user_id, creation_id)
去重并保留最新的交互时间戳，生成用户-物品交互表，用于推荐算法。

功能:
- 读取 action 表最近7天的分区数据（p_date）
- 筛选 action_type = 2 的点击行为
- 按 (user_id, creation_id) 去重，保留最新 action_ts
- 写入 click7aggre 表（包含 action_ts 列）

部署:
  docker cp etl/aggre7.py spark-standalone:/opt/spark-apps/

运行:
  docker exec spark-standalone /opt/spark/bin/spark-submit \
    --master spark://spark:7077 \
    --conf spark.sql.hive.metastore.version=4.0.0 \
    --conf spark.sql.hive.metastore.jars=maven \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=jfs://feedjfs/warehouse \
    --conf hive.metastore.uris=thrift://hive-metastore:9083 \
    /opt/spark-apps/aggre7.py \
      --src-db dwd \
      --src-table action \
      --dst-db dwd \
      --dst-table click7aggre1 \
      --end-date 20251109 \
      --days 7

参数说明:
  --end-date: 结束日期（yyyyMMdd 格式），默认为当前日期
  --days: 回溯天数，默认为 7

注意:
- 目标表会被覆盖写入（overwrite），每次运行生成最新的7天聚合数据
- 源表必须存在且包含对应日期的分区数据
- 若源表有 action_ts 列则使用，否则用 p_date 转换
"""

import argparse
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def main(argv=None):
    a = argparse.ArgumentParser(description="Hive 7天点击聚合（统一列名 action_ts）")
    a.add_argument("--src-db", default="dwd")
    a.add_argument("--src-table", default="action")
    a.add_argument("--dst-db", default="dwd")
    a.add_argument("--dst-table", default="click7aggre")
    a.add_argument("--end-date", default=None, help="yyyyMMdd（默认: 今天）")
    a.add_argument("--days", type=int, default=7)
    a.add_argument("--metastore-uri", default="thrift://hive-metastore:9083")
    a.add_argument("--warehouse-dir", default="jfs://feedjfs/warehouse")
    opt = a.parse_args(argv)

    end = opt.end_date or datetime.now().strftime("%Y%m%d")
    try:
        end_dt = datetime.strptime(end, "%Y%m%d")
    except ValueError:
        raise SystemExit(f"无效日期格式 '{end}'，期望 yyyyMMdd")

    dates = [(end_dt - timedelta(days=i)).strftime("%Y%m%d") for i in range(opt.days - 1, -1, -1)]
    print(f"聚合日期范围: {dates[0]} - {dates[-1]} (共 {opt.days} 天)")

    spark = (
        SparkSession.builder
        .appName("hive_click7_aggr_timestamp")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", opt.metastore_uri)
        .config("spark.sql.warehouse.dir", opt.warehouse_dir)
        .config("spark.sql.hive.metastore.version", "4.0.0")
        .config("spark.sql.hive.metastore.jars", "maven")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("hive.metastore.schema.verification", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Set dynamic partition mode
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {opt.dst_db}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {opt.dst_db}.{opt.dst_table} (
            user_id BIGINT COMMENT '用户ID',
            creation_id BIGINT COMMENT '物品ID',
            action_ts TIMESTAMP COMMENT '最近一次交互时间'
        )
        STORED AS PARQUET
        TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """)

    src = (spark.table(f"{opt.src_db}.{opt.src_table}")
              .where(F.col("p_date").isin(dates))
              .where(F.col("action_type") == 2)
              .where(F.col("user_id").isNotNull() & F.col("creation_id").isNotNull()))

    # 使用 action_ts 列，若不存在则用 p_date 代替
    src = src.withColumn("action_ts", 
        F.when(F.col("action_ts").isNotNull(), F.col("action_ts"))
         .otherwise(F.to_timestamp(F.col("p_date"), "yyyyMMdd"))
         .cast("timestamp"))

    # 对 (user_id, creation_id) 只保留最新 action_ts
    w = Window.partitionBy("user_id", "creation_id").orderBy(F.col("action_ts").desc())
    df = (src.select("user_id", "creation_id", "action_ts")
             .withColumn("rn", F.row_number().over(w))
             .where(F.col("rn") == 1)
             .drop("rn"))

    print(f"去重后交互记录数: {df.count()}")

    df.write.mode("overwrite").format("parquet").saveAsTable(f"{opt.dst_db}.{opt.dst_table}")
    print(f"成功写入 {opt.dst_db}.{opt.dst_table}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
