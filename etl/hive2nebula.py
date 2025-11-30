#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hive -> NebulaGraph 数据同步

Spark 批处理任务: 从 Hive 读取 Swing 相似度数据，同步到 NebulaGraph 图数据库。

- 读取 Hive 中的 Swing 物品相似度表（item_i, item_j, score）
- 写入点数据：创建 creation Tag（物品节点）
- 写入边数据：创建 sim Edge（相似度边）

NebulaGraph Schema (参考 swing.ngql):
  - Space: swing0
  - Tag: creation (creation_id: int64)
  - Edge: sim (simscore: double)

部署:
  docker cp etl/hive2nebula.py spark-standalone:/opt/spark-apps/

运行:
  docker exec spark-standalone /opt/spark/bin/spark-submit \
    --master spark://spark:7077 \
    --packages com.vesoft:nebula-spark-connector_3.0:3.6.0,org.scala-lang.modules:scala-collection-compat_2.12:2.11.0 \
    --conf spark.sql.hive.metastore.version=3.1.3 \
    --conf spark.sql.hive.metastore.jars=maven \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=jfs://feedjfs/warehouse \
    --conf hive.metastore.uris=thrift://hive-metastore:9083 \
    /opt/spark-apps/hive2nebula.py \
      --src-db dwd \
      --src-table swing \
      --nebula-space swing0 \
      --nebula-meta-address metad0:9559 \
      --nebula-graph-address graphd:9669

参数说明:
  --src-db: Hive 源数据库名（默认: dwd）
  --src-table: Hive 源表名（默认: swing_sim）
  --nebula-space: NebulaGraph Space 名称（默认: swing0）
  --nebula-meta-address: NebulaGraph Meta 服务地址
  --nebula-graph-address: NebulaGraph Graph 服务地址
  --nebula-user: NebulaGraph 用户名（默认: root）
  --nebula-passwd: NebulaGraph 密码（默认: nebula）
  --batch: 批量写入大小（默认: 1000）
  --write-vertex: 是否写入点数据（默认: True）
  --write-edge: 是否写入边数据（默认: True）

注意:
- 需要 Nebula Spark Connector 依赖包
- NebulaGraph 的 Space 和 Schema 需要提前创建（参考 swing.ngql）
- 推荐使用 nebula-spark-connector_3.0 版本，兼容 Spark 3.x
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Nebula Spark Connector 的 Maven 坐标
# nebula-spark-connector_3.0:3.6.0 兼容 Spark 3.x (Scala 2.12) 和 Nebula 3.x
# 注意: 3.8.0 版本可能存在 Scala 兼容性问题，使用 3.6.0 更稳定
DEFAULT_NEBULA_PACKAGES = "com.vesoft:nebula-spark-connector_3.0:3.6.0,org.scala-lang.modules:scala-collection-compat_2.12:2.11.0"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="从 Hive 同步 Swing 数据到 NebulaGraph")
    # Hive 源配置
    parser.add_argument("--src-db", default="dwd", help="Hive 源数据库名（默认: dwd）")
    parser.add_argument("--src-table", default="swing_sim", help="Hive 源表名（默认: swing_sim）")

    # NebulaGraph 配置
    parser.add_argument("--nebula-space", default="swing0", help="NebulaGraph Space 名称（默认: swing0）")
    parser.add_argument("--nebula-meta-address", default="metad0:9559", help="NebulaGraph Meta 服务地址")
    parser.add_argument("--nebula-graph-address", default="graphd:9669", help="NebulaGraph Graph 服务地址")
    parser.add_argument("--nebula-user", default="root", help="NebulaGraph 用户名（默认: root）")
    parser.add_argument("--nebula-passwd", default="nebula", help="NebulaGraph 密码（默认: nebula）")

    # 写入配置
    parser.add_argument("--batch", type=int, default=1000, help="批量写入大小（默认: 1000）")
    parser.add_argument("--write-vertex", type=lambda x: x.lower() == 'true', default=True, help="是否写入点数据（默认: True）")
    parser.add_argument("--write-edge", type=lambda x: x.lower() == 'true', default=True, help="是否写入边数据（默认: True）")

    # Spark 配置
    parser.add_argument("--master", default=None, help="Spark master URL（默认: 继承 spark-submit）")
    parser.add_argument("--metastore-uri", default="thrift://hive-metastore:9083", help="Hive Metastore URI")
    parser.add_argument("--warehouse-dir", default="jfs://feedjfs/warehouse", help="Hive 仓库目录")
    parser.add_argument(
        "--packages",
        default=DEFAULT_NEBULA_PACKAGES,
        help=f"Spark --packages 依赖列表（默认: {DEFAULT_NEBULA_PACKAGES}）"
    )

    return parser.parse_args(argv)


def build_spark(app_name: str = "hive2nebula_swing",
                master: str = None,
                warehouse_dir: str = None,
                metastore_uri: str = None,
                packages: str = None) -> SparkSession:
    """构建 Spark 会话"""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Hive 3.1.3 Metastore 配置
        .config("spark.sql.hive.metastore.version", "3.1.3")
        .config("spark.sql.hive.metastore.jars", "maven")
        .config("hive.metastore.schema.verification", "false")
    )

    if master:
        builder = builder.master(master)
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    if metastore_uri:
        builder = builder.config("hive.metastore.uris", metastore_uri)
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    spark = builder.enableHiveSupport().getOrCreate()
    return spark


def write_vertices(df, nebula_meta_address, nebula_graph_address,
                   nebula_space, nebula_user, nebula_passwd, batch):
    """
    写入点数据到 NebulaGraph

    从 Swing 表中提取所有物品 ID，写入为 creation Tag 的点。
    根据 swing.ngql 定义: Tag creation (creation_id: int64)
    """
    # 提取所有唯一的物品 ID（item_i 和 item_j 的并集）
    items_i = df.select(F.col("item_i").alias("creation_id"))
    items_j = df.select(F.col("item_j").alias("creation_id"))
    vertices = items_i.union(items_j).distinct()

    # 写入点数据到 NebulaGraph
    # vertexField 指定 VID 字段，Nebula 会使用这个字段作为点的唯一标识
    print(f"开始写入点数据到 NebulaGraph, Space: {nebula_space}, Tag: creation")
    print(f"点数据数量: {vertices.count()}")

    (vertices.write
        .format("com.vesoft.nebula.connector.NebulaDataSource")
        .mode("overwrite")
        .option("type", "vertex")
        .option("operateType", "write")  # 指定操作类型为写入
        .option("spaceName", nebula_space)
        .option("label", "creation")
        .option("vertexField", "creation_id")
        .option("vidPolicy", "")  # 空表示使用原始值作为 VID
        .option("vidAsProp", "true")  # 将 VID 字段同时作为属性写入
        .option("batch", batch)
        .option("metaAddress", nebula_meta_address)
        .option("graphAddress", nebula_graph_address)
        .option("user", nebula_user)
        .option("passwd", nebula_passwd)
        .option("writeMode", "insert")
        .save())

    print("点数据写入完成")


def write_edges(df, nebula_meta_address, nebula_graph_address,
                nebula_space, nebula_user, nebula_passwd, batch):
    """
    写入边数据到 NebulaGraph

    将 Swing 相似度数据写入为 sim Edge。
    根据 swing.ngql 定义: Edge sim (simscore: double)
    """
    # 准备边数据
    # srcVertexField: 源点 VID
    # dstVertexField: 目标点 VID
    # 边属性: simscore
    edges = df.select(
        F.col("item_i").alias("srcid"),
        F.col("item_j").alias("dstid"),
        F.col("score").alias("simscore")
    )

    print(f"开始写入边数据到 NebulaGraph, Space: {nebula_space}, Edge: sim")
    print(f"边数据数量: {edges.count()}")

    (edges.write
        .format("com.vesoft.nebula.connector.NebulaDataSource")
        .mode("overwrite")
        .option("type", "edge")
        .option("operateType", "write")  # 指定操作类型为写入
        .option("spaceName", nebula_space)
        .option("label", "sim")
        .option("srcVertexField", "srcid")
        .option("dstVertexField", "dstid")
        .option("srcPolicy", "")  # 空表示使用原始值作为源 VID
        .option("dstPolicy", "")  # 空表示使用原始值作为目标 VID
        .option("rankField", "")  # 无 rank 字段
        .option("batch", batch)
        .option("metaAddress", nebula_meta_address)
        .option("graphAddress", nebula_graph_address)
        .option("user", nebula_user)
        .option("passwd", nebula_passwd)
        .option("writeMode", "insert")
        .save())

    print("边数据写入完成")


def main(argv=None):
    args = parse_args(argv)

    packages = (args.packages or "").strip()
    if packages:
        print(f"通过 spark.jars.packages 自动加载依赖: {packages}")
    else:
        print("未配置额外 --packages，假定 Nebula Spark Connector 依赖已在 Spark 集群 classpath 中。")

    spark = build_spark(
        master=args.master,
        warehouse_dir=args.warehouse_dir,
        metastore_uri=args.metastore_uri,
        packages=packages or None,
    )

    # 读取 Hive 源表
    source_table = f"{args.src_db}.{args.src_table}"
    print(f"读取 Hive 表: {source_table}")

    df = spark.table(source_table)
    print(f"源表 Schema:")
    df.printSchema()
    print(f"源表数据量: {df.count()}")

    # 缓存数据以提高性能（需要同时写入点和边时复用）
    if args.write_vertex and args.write_edge:
        df.cache()

    # 写入点数据
    if args.write_vertex:
        write_vertices(
            df,
            args.nebula_meta_address,
            args.nebula_graph_address,
            args.nebula_space,
            args.nebula_user,
            args.nebula_passwd,
            args.batch
        )

    # 写入边数据
    if args.write_edge:
        write_edges(
            df,
            args.nebula_meta_address,
            args.nebula_graph_address,
            args.nebula_space,
            args.nebula_user,
            args.nebula_passwd,
            args.batch
        )

    print("Hive -> NebulaGraph 同步完成!")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
