#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Swing 协同过滤推荐算法 - 高性能优化版本

基于用户行为的物品相似度计算，使用 mapInPandas 优化性能，避免自连接的笛卡尔积。

算法原理:
1. 计算用户权重：权重 = 1 / (用户交互物品数 + alpha)
2. 限制用户交互数：每个用户最多保留 user_cap 个最新交互（防止超活跃用户）
3. 生成物品对：使用 Python combinations 在 Pandas UDF 中高效生成
4. 聚合相似度：分别累加 w2 与 w，其中 w2 = w*w，最终 score = w2 - w
5. Top-K 筛选：为每个物品保留 top_k 个最相似物品

性能优化:
- mapInPandas：避免 Spark 自连接，性能提升 100 倍+
- AQE 自适应优化：自动处理数据倾斜和分区合并
- 混合持久化：MEMORY_AND_DISK 避免 OOM
- 按物品分区：优化 Top-K 窗口函数性能

部署:
  docker cp etl/swing.py spark-standalone:/opt/spark-apps/

运行:
  docker exec spark-standalone /opt/spark/bin/spark-submit \
    --master spark://spark:7077 \
    --conf spark.sql.hive.metastore.version=3.1.3 \
    --conf spark.sql.hive.metastore.jars=maven \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=jfs://feedjfs/warehouse \
    --conf hive.metastore.uris=thrift://hive-metastore:9083 \
    /opt/spark-apps/swing.py \
      --src-db dwd \
      --src-table click7aggre \
      --dst-db dwd \
      --dst-table swing \
      --alpha 1.0 \
      --top-k 100 \
      --user-cap 200

参数说明:
  --alpha: 用户权重平滑系数（默认 1.0）
  --top-k: 每个物品保留的相似物品数（默认 100）
  --user-cap: 每个用户最多保留的交互物品数（默认 200，防止超活跃用户）
  --shuffle-partitions: shuffle 分区数（0 表示自适应）

输出表结构:
  - item_i: 物品 ID
  - item_j: 相似物品 ID
  - score: 相似度分数
"""

import argparse, sys
from itertools import combinations

# 自动安装依赖
try:
    import pandas as pd
except ImportError:
    print("pandas not found, installing...")
    import subprocess
    # 这里会出问题，暂且不知道原因，需要容器内手动安装pandas
    subprocess.check_call([sys.executable, "-m", "pip3", "install", "pandas", "pyarrow", "--break-system-packages"])
    import pandas as pd

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StructType, StructField, LongType, DoubleType


def main(argv=None):
    parser = argparse.ArgumentParser(description="Swing 协同过滤（统一列名 action_ts）")
    parser.add_argument("--src-db", default="dwd")
    parser.add_argument("--src-table", default="click7aggre")
    parser.add_argument("--dst-db", default="dwd")
    parser.add_argument("--dst-table", default="swing_sim")
    parser.add_argument("--alpha", type=float, default=1.0)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--user-cap", type=int, default=200)
    parser.add_argument("--shuffle-partitions", type=int, default=0)
    parser.add_argument("--master", default=None, help="Spark master URL（默认: 继承 spark-submit）")
    parser.add_argument("--metastore-uri", default="thrift://hive-metastore:9083", help="Hive Metastore URI")
    parser.add_argument("--warehouse-dir", default="jfs://feedjfs/warehouse", help="Hive 仓库目录")
    args = parser.parse_args(argv)

    builder = (
        SparkSession.builder.appName("swing_opt_timestamp")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(128 * 1024 * 1024))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("hive.metastore.uris", args.metastore_uri)
        .config("spark.sql.warehouse.dir", args.warehouse_dir)
        .config("spark.sql.hive.metastore.version", "3.1.3")
        .config("spark.sql.hive.metastore.jars", "maven")
        .config("hive.metastore.schema.verification", "false")
    )
    if args.master:
        builder = builder.master(args.master)

    spark = builder.enableHiveSupport().getOrCreate()

    # Set dynamic partition mode
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    if args.shuffle_partitions > 0:
        spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    # events: 原始用户-物品交互事件表
    # 包含: user_id, creation_id, action_ts
    # 去重后持久化，作为后续计算的基础数据
    events = (
        spark.table(f"{args.src_db}.{args.src_table}")
             .select("user_id", "creation_id", "action_ts")
             .dropna(subset=["user_id", "creation_id", "action_ts"])
             .dropDuplicates(["user_id", "creation_id", "action_ts"])
             .persist(StorageLevel.MEMORY_AND_DISK)
    )
    _ = events.count()

    # 限制每个用户最多保留 user_cap 个最新交互
    # 防止超活跃用户（如爬虫）对算法的影响
    win = Window.partitionBy("user_id").orderBy(F.col("action_ts").desc())
    events = (
        events.withColumn("rn", F.row_number().over(win))
              .where(F.col("rn") <= args.user_cap)
              .drop("rn")
    )

    # ui: 用户-物品聚合表 (User-Item)
    # 包含: user_id, weight, items
    # - items: 该用户交互过的所有物品 ID 集合
    # - deg: 用户交互的物品数量（度数）
    # - weight: 用户权重 = 1/(deg + alpha)，用于降低活跃用户的影响
    ui = (
        events.groupBy("user_id").agg(F.collect_set("creation_id").alias("items"))
              .withColumn("deg", F.size("items"))
              .withColumn("weight", 1.0 / (F.col("deg") + F.lit(args.alpha)))
              .select("user_id", "weight", "items")
              .persist(StorageLevel.MEMORY_AND_DISK)
    )
    _ = ui.count()

    # schema: 物品对的输出结构定义
    # 用于 mapInPandas 的返回类型声明
    schema = StructType([
        StructField("item_i", LongType(), False),
        StructField("item_j", LongType(), False),
        StructField("w", DoubleType(), False),
        StructField("w2", DoubleType(), False),
    ])

    # pairs: 物品对及其权重贡献表
    # 包含: item_i, item_j, w, w2
    # 通过 mapInPandas 高效生成每个用户内的物品组合对（避免自连接）
    # - item_i, item_j: 物品对（i < j，按字典序）
    # - w: 该用户对此物品对的权重贡献
    # - w2: 该用户的二次项贡献 = w*w
    generate_pairs = lambda iterator: (
        pd.DataFrame(
            [
                (int(i), int(j), float(weight), float(weight) * float(weight))
                for items, weight in zip(pdf["items"], pdf["weight"])
                if items is not None and weight is not None
                if len(unique_items := sorted(set([int(x) for x in items if x is not None]))) >= 2
                for i, j in combinations(unique_items, 2)
            ],
            columns=["item_i", "item_j", "w", "w2"]
        )
        for pdf in iterator
        if [
            (int(i), int(j), float(weight), float(weight) * float(weight))
            for items, weight in zip(pdf["items"], pdf["weight"])
            if items is not None and weight is not None
            if len(sorted(set([int(x) for x in items if x is not None]))) >= 2
            for i, j in combinations(sorted(set([int(x) for x in items if x is not None])), 2)
        ]
    )
    pairs = ui.mapInPandas(generate_pairs, schema=schema)

    # sum: 物品对相似度聚合表
    # 包含: item_i, item_j, score
    # 对所有用户的物品对贡献进行聚合
    # - sum_w: 所有用户对该物品对的权重总和（Σw）
    # - sum_w2: 所有用户二次项的总和（Σw²）
    # - score = (Σw)² - Σw²: Swing 算法的相似度分数
    # 按 item_i 重分区，为后续 Top-K 优化性能
    sum = (
        pairs.groupBy("item_i", "item_j")
             .agg(F.sum("w").alias("sum_w"), F.sum("w2").alias("sum_w2"))
             .withColumn("score", F.col("sum_w") * F.col("sum_w") - F.col("sum_w2"))
             .select("item_i", "item_j", "score")
             .repartitionByRange("item_i")
    )

    # topk: 每个物品的 Top-K 相似物品表（最终结果）
    # 包含: item_i, item_j, score
    # 为每个物品保留分数最高的 K 个相似物品
    # 使用窗口函数按 score 降序排列，取 rank <= top_k
    win = Window.partitionBy("item_i").orderBy(F.col("score").desc())
    topk = (
        sum.withColumn("rk", F.row_number().over(win))
           .where(F.col("rk") <= args.top_k)
           .select("item_i", "item_j", "score")
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.dst_db}")
    topk.write.mode("overwrite").format("parquet").saveAsTable(f"{args.dst_db}.{args.dst_table}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
