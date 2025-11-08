# Spark 与 Hive 4 集成配置

本目录包含 Spark 连接到 Hive 4.0 Metastore 所需的配置文件。

## 核心配置说明

### Spark 支持的 Hive 版本
根据 [Spark 官方文档](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html#interacting-with-different-versions-of-hive-metastore)，Spark 支持以下 Hive Metastore 版本：
- Hive 2.0.0 - 2.3.10
- Hive 3.0.0 - 3.1.3
- **Hive 4.0.0 - 4.0.1** ✅

### 关键配置项

#### 1. spark.sql.hive.metastore.version
指定要使用的 Hive Metastore 版本，默认为 2.3.10。

**配置：**
```xml
<property>
  <name>spark.sql.hive.metastore.version</name>
  <value>4.0.0</value>
</property>
```

#### 2. spark.sql.hive.metastore.jars
控制 Hive JAR 包的来源，有 4 种选项：
- **"builtin"**（默认）- 使用 Spark 自带的 Hive 2.3.10
- **"maven"** - 从 Maven 仓库自动下载指定版本 ✅ 推荐
- **"path"** - 使用指定路径的 JAR 包
- **classpath** - 使用类路径中的 JAR

**配置：**
```xml
<property>
  <name>spark.sql.hive.metastore.jars</name>
  <value>maven</value>
</property>
```

### hive-site.xml
Spark 的 Hive 配置文件，已包含以下关键配置：

**核心配置：**
- Metastore 版本: `4.0.0`
- Metastore URI: `thrift://hive-metastore:9083`
- Warehouse 目录: `jfs://feedjfs/warehouse`
- JAR 包来源: `maven`（自动下载）
- Catalog 实现: `hive`

### 3. test_hive_connection.py
测试脚本，用于验证 Spark 与 Hive 4 的连接。

## 使用方法

### 1. 启动服务

```bash
# 启动所有服务
docker-compose up -d

# 查看 Spark 容器日志
docker-compose logs -f spark
```

### 2. 在 PySpark 代码中使用

**方法 1：使用配置文件（推荐）**

配置已经在 `hive-site.xml` 中设置好，直接使用即可：

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YourApp") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# 使用 Hive
spark.sql("SHOW DATABASES").show()
```

**方法 2：在代码中显式配置**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YourApp") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.hive.metastore.version", "4.0.0") \
    .config("spark.sql.hive.metastore.jars", "maven") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 使用 Hive
spark.sql("SHOW DATABASES").show()
spark.sql("CREATE TABLE IF NOT EXISTS test (id INT, name STRING)")
spark.sql("INSERT INTO test VALUES (1, 'hello')")
spark.sql("SELECT * FROM test").show()
```

**方法 3：使用 spark-submit 命令行参数**

```bash
spark-submit \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.hive.metastore.version=4.0.0 \
  --conf spark.sql.hive.metastore.jars=maven \
  --conf hive.metastore.uris=thrift://hive-metastore:9083 \
  your_script.py
```

## 故障排查

### 问题 1: Unsupported Hive Metastore version
**错误信息：**
```
IllegalArgumentException: '4.1.0' in spark.sql.hive.metastore.version is invalid. Unsupported Hive Metastore version
```

**原因：** Spark 4.0.0 只支持 Hive 4.0.0-4.0.1，不支持 4.1.0。

**解决方法：**
1. 将 Hive Metastore 降级到 4.0.0（推荐）：
   ```yaml
   # docker-compose.yaml
   hive-metastore:
     image: apache/hive:4.0.0
   ```

2. 配置 Spark 使用 Hive 4.0.0：
   ```xml
   <property>
     <name>spark.sql.hive.metastore.version</name>
     <value>4.0.0</value>
   </property>
   ```

### 问题 2: Maven 下载 JAR 包失败
**错误信息：**
```
Failed to download org.apache.hive:hive-metastore:4.0.0
```

**解决方法：**
1. 检查容器是否能访问 Maven 中央仓库：
   ```bash
   docker exec spark-standalone curl -I https://repo1.maven.org/maven2/
   ```

2. 检查 Ivy 缓存目录权限：
   ```bash
   docker exec spark-standalone ls -la /home/spark/.ivy2/
   ```

3. 首次运行时，Maven 下载需要时间，请耐心等待（可能需要几分钟）。

### 问题 3: Invalid method name: 'get_table'
**错误信息：**
```
HiveException: Unable to fetch table action. Invalid method name: 'get_table'
```

**原因：** Spark 使用的 Hive 客户端版本与 Hive Metastore 服务端版本不匹配。

**解决方法：**
确保配置了正确的版本：
```python
.config("spark.sql.hive.metastore.version", "4.0.0")
.config("spark.sql.hive.metastore.jars", "maven")
```

### 问题 4: Metastore 连接失败
**错误信息：**
```
MetaException: Could not connect to meta store using any of the URIs provided
```

**解决方法：**
1. 检查 Hive Metastore 是否运行：
   ```bash
   docker-compose ps hive-metastore
   docker-compose logs hive-metastore
   ```

2. 检查网络连接：
   ```bash
   docker exec spark-standalone ping -c 3 hive-metastore
   ```

## 配置调优

### 内存配置
在 docker-compose.yaml 中调整：
```yaml
environment:
  - SPARK_WORKER_MEMORY=4g  # Worker 内存
  - SPARK_EXECUTOR_MEMORY=2g  # Executor 内存
```

### 并行度配置
在 Spark 代码中配置：
```python
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()
```

## 参考资源

- [Apache Spark 官方文档](https://spark.apache.org/docs/latest/)
- [Apache Hive 4 文档](https://hive.apache.org/)
- [Spark SQL with Hive](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
