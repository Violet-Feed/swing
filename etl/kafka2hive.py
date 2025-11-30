#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka -> Hive 批处理 ETL

Spark 批处理任务: 从 Kafka 摄取行为事件数据到 Hive 分区表 (p_date)。

- 读取指定日期的 Kafka 记录（可配置偏移量），解析 JSON 数据。
- 针对新格式消息（userId + actionTypeList/creationIdList/timestampList），拆分逗号串并按索引对齐展开。
- 将 action_ts 字段标准化为 Spark TimestampType（支持 ISO 字符串或 epoch 秒/毫秒）。
- 写入 Hive 表 `action`（数据库可配置），按 p_date（字符串 yyyyMMdd）分区。

部署:
  docker cp etl/kafka2hive.py spark-standalone:/opt/spark-apps/

运行:
  docker exec spark-standalone /opt/spark/bin/spark-submit \
    --master spark://spark:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.kafka:kafka-clients:3.8.1,org.apache.commons:commons-pool2:2.12.0 \
    --conf spark.sql.hive.metastore.version=3.1.3 \
    --conf spark.sql.hive.metastore.jars=maven \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=jfs://feedjfs/warehouse \
    --conf hive.metastore.uris=thrift://hive-metastore:9083 \
    /opt/spark-apps/kafka2hive.py \
      --bootstrap-servers kafka:9093 \
      --topic action \
      --p-date 20251130 \
      --database dwd \
      --table action \
      --starting-offsets earliest \
      --ending-offsets latest

注意:
- 此任务旨在每天运行一次，需要 --p-date 参数（yyyyMMdd 格式）。
- p_date 仅用于分区标识，不会按事件时间戳过滤数据。
- 如果 Hive 表/数据库不存在，将自动创建。
- 需要 Kafka 连接包：spark-sql-kafka-0-10_2.13:4.0.0
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DEFAULT_KAFKA_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
    "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.0",
    "org.apache.kafka:kafka-clients:3.8.1",
    "org.apache.commons:commons-pool2:2.12.0",
])


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="从 Kafka 导入行为数据到 Hive（按日分区）")
    parser.add_argument("--bootstrap-servers", default="kafka:9093", help="Kafka 服务器地址（默认: kafka:9093）")
    parser.add_argument("--topic", required=True, help="Kafka 主题名称")
    parser.add_argument("--p-date", required=True, help="分区日期，格式 yyyyMMdd")
    parser.add_argument("--database", default="dwd", help="Hive 数据库名（默认: dwd）")
    parser.add_argument("--table", default="action", help="Hive 表名（默认: action）")
    parser.add_argument("--starting-offsets", default="earliest", help="Kafka 起始偏移量（默认: earliest）")
    parser.add_argument("--ending-offsets", default="latest", help="Kafka 结束偏移量（默认: latest）")
    parser.add_argument("--fail-on-data-loss", default="false", choices=["true", "false"], help="Kafka 数据丢失时是否失败（默认: false）")
    parser.add_argument("--bad-records-path", default=None, help="如果设置，将格式错误的 JSON 行写入此路径（JuiceFS/HDFS）")
    parser.add_argument("--master", default=None, help="Spark master URL（默认: 继承 spark-submit）")
    parser.add_argument("--metastore-uri", default="thrift://hive-metastore:9083", help="Hive Metastore URI")
    parser.add_argument("--warehouse-dir", default="jfs://feedjfs/warehouse", help="Hive 仓库目录（Spark SQL warehouse dir）")
    parser.add_argument(
        "--packages",
        default=DEFAULT_KAFKA_PACKAGES,
        help=(
            "Spark --packages 依赖列表（默认包含 Kafka 源、Token Provider、kafka-clients 3.8.1 以及 commons-pool2 2.12.0）。"
            "为兼容 Spark 4.0 KafkaSource 必须保证 commons-pool2>=2.12.0；如集群已内置，可传空字符串跳过下载。"
        ),
    )
    return parser.parse_args(argv)


def validate_date(p_date: str) -> None:
    """验证分区日期格式"""
    try:
        datetime.strptime(p_date, "%Y%m%d")
    except ValueError:
        raise SystemExit(f"无效的 --p-date '{p_date}'。期望格式 yyyyMMdd，如: 20251105")


def build_spark(app_name: str = "kafka2hive_action",
                master: str = None,
                warehouse_dir: str = None,
                metastore_uri: str = None,
                packages: str = None) -> SparkSession:
    """构建 Spark 会话"""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Hive 3.1.3 Metastore 配置
        .config("spark.sql.hive.metastore.version", "3.1.3")
        .config("spark.sql.hive.metastore.jars", "maven")
    )

    # 如果未提供配置，使用默认值
    if master:
        builder = builder.master(master)
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    if metastore_uri:
        builder = builder.config("hive.metastore.uris", metastore_uri)
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    spark = builder.enableHiveSupport().getOrCreate()
    # Hive 动态分区配置
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    return spark


def init_table(spark: SparkSession, db: str, table: str):
    """初始化 Hive 数据库和表"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    # 如果表不存在则创建（与 dataTable/action.md 保持一致）
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
        TBLPROPERTIES (
            'parquet.compression'='SNAPPY',
            'parquet.block.size'='268435456'
        )
        """
    )


def parse_json(df_json):
    """解析新格式 Kafka JSON 数据并标准化字段。

    新格式示例:
    {"actionTypeList":"1,1","creationIdList":"111,222","timestampList":"111,222","userId":1981463119351775232}

    返回列: user_id (long), action_ts (timestamp), action_type (int), creation_id (string)。
    """
    def parse_ts(col):
        """支持 epoch 秒/毫秒 或 ISO 字符串 -> timestamp"""
        ts_num = col.cast("bigint")
        ts_epoch = F.when(ts_num.isNotNull(),
                          F.when(ts_num > F.lit(9999999999), (ts_num / F.lit(1000)).cast("double"))
                          .otherwise(ts_num.cast("double")))
        ts_epoch = F.when(ts_num.isNotNull(), F.to_timestamp(F.from_unixtime(ts_epoch)))
        ts_str = F.to_timestamp(col)
        return F.coalesce(ts_epoch, ts_str)

    def split_and_clean(colname: str):
        """按逗号拆分字符串，去掉空白与空元素"""
        return F.filter(
            F.transform(
                F.split(F.coalesce(F.col(colname), F.lit("")), "\\s*,\\s*"),
                lambda x: F.when(F.length(F.trim(x)) == 0, None).otherwise(F.trim(x))
            ),
            lambda x: x.isNotNull()
        )

    schema = T.StructType([
        T.StructField("userId", T.LongType()),
        T.StructField("actionTypeList", T.StringType()),
        T.StructField("creationIdList", T.StringType()),
        T.StructField("timestampList", T.StringType()),
    ])

    parsed = df_json.select(F.from_json(F.col("json_str"), schema, {"mode": "PERMISSIVE"}).alias("data"), F.col("json_str"))
    good = parsed.select("data.*").where(F.col("data").isNotNull())

    base = (
        good
        .withColumn("user_id", F.col("userId").cast("long"))
        .withColumn("action_type_arr", split_and_clean("actionTypeList"))
        .withColumn("creation_id_arr", split_and_clean("creationIdList"))
        .withColumn("action_ts_arr", split_and_clean("timestampList"))
        .withColumn("zipped", F.arrays_zip("action_ts_arr", "action_type_arr", "creation_id_arr"))
    )

    exploded = base.withColumn("item", F.explode_outer("zipped"))

    normalized = (
        exploded
        .select(
            "user_id",
            F.col("item.action_ts_arr").alias("action_ts_str"),
            F.col("item.action_type_arr").alias("action_type_str"),
            F.col("item.creation_id_arr").alias("creation_id_str"),
        )
        .withColumn("action_ts", parse_ts(F.col("action_ts_str")))
        .withColumn("action_type", F.col("action_type_str").cast("int"))
        .withColumn("creation_id", F.regexp_replace(F.col("creation_id_str"), "\\s", "").cast("long"))
        .drop("action_ts_str", "action_type_str", "creation_id_str")
    )

    return normalized


def main(argv=None):
    args = parse_args(argv)
    validate_date(args.p_date)
    packages = (args.packages or "").strip()
    if packages:
        print(f"通过 spark.jars.packages 自动加载依赖: {packages}")
    else:
        print("未配置额外 --packages，假定 Kafka 相关依赖已在 Spark 集群 classpath 中。")

    # 显式配置 metastore / warehouse，确保与 HiveServer2 使用同一元数据
    spark = build_spark(
        master=args.master,
        warehouse_dir=args.warehouse_dir,
        metastore_uri=args.metastore_uri,
        packages=packages or None,
    )

    init_table(spark, args.database, args.table)

    # 从 Kafka 批量读取数据
    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("endingOffsets", args.ending_offsets)
        .option("failOnDataLoss", args.fail_on_data_loss)
        .load()
    )

    # 从 Kafka value 提取 JSON 字符串
    df_json = kafka_df.select(F.col("value").cast("string").alias("json_str"))

    # 解析并标准化
    parsed = parse_json(df_json)

    # 添加分区列
    final = (
        parsed
        .withColumn("p_date", F.lit(args.p_date))
        .select("user_id", "action_ts", "action_type", "creation_id", "p_date")
    )

    # 分离格式错误的行（如缺少关键字段）
    # 定义最小有效性: user_id 非空, action_ts 非空, action_type 非空
    good = final.where(
        F.col("user_id").isNotNull() &
        F.col("action_ts").isNotNull() &
        F.col("action_type").isNotNull()
    )
    bad = final.exceptAll(good)

    # 将好的行写入 Hive
    target = f"{args.database}.{args.table}"
    good.write.mode("append").format("hive").insertInto(target)

    # 显式添加分区到 Hive Metastore（确保分区元数据同步）
    # 这一步很重要：即使文件已写入，也需要告诉 Metastore 这个分区存在
    spark.sql(f"ALTER TABLE {target} ADD IF NOT EXISTS PARTITION (p_date='{args.p_date}')")
    print(f"已注册分区: {target} (p_date={args.p_date})")

    # 可选：写入坏记录供检查
    if args.bad_records_path:
        bad.write.mode("overwrite").parquet(args.bad_records_path.rstrip("/") + f"/p_date={args.p_date}")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
