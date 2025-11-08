# Action Table Schema

## Field Definition

- user_id LONG pk
- action_ts timestamp
- action_type int
- creation_id LONG
- p_date STRING

## Hive 建表语句

```sql
CREATE TABLE IF NOT EXISTS action (
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
);
```

**说明：**
- Hive 中 LONG 对应 BIGINT，timestamp 对应 TIMESTAMP
- 使用 **Parquet 格式**存储，支持列式压缩
- **按 p_date（导入日期）分区，支持分区裁剪优化查询性能**
- 使用 SNAPPY 压缩，平衡压缩率和性能
- Parquet 格式具有更好的跨平台兼容性（Spark、ClickHouse、Python 等）
- 注：Hive 不强制主键约束，user_id 的唯一性需要在 ETL 层保证

**查询示例（分区裁剪）：**
```sql
-- 只扫描指定日期的分区
SELECT * FROM action WHERE p_date = '20251105';

-- 查询最近 7 天的用户操作
SELECT * FROM action WHERE p_date >= '20251029' AND p_date <= '20251105';

-- 按操作类型统计
SELECT action_type, COUNT(*) as cnt
FROM action
WHERE p_date = '20251105'
GROUP BY action_type;
```

---

## ClickHouse 建表语句

### MergeTree 引擎（推荐）

```sql
CREATE TABLE IF NOT EXISTS action
(
    user_id Int64 COMMENT '用户 ID，主键',
    action_ts DateTime COMMENT '操作时间',
    action_type Int32 COMMENT '操作类型',
    creation_id Int64 COMMENT '创作 ID',
    p_date String COMMENT '导入日期分区 yyyyMMdd'
)
ENGINE = MergeTree()
PARTITION BY p_date
ORDER BY (p_date, user_id, action_ts)
PRIMARY KEY (p_date, user_id, action_ts)
SETTINGS index_granularity = 8192;
```

**说明：**

### 引擎选择
- 使用 **MergeTree** 引擎，ClickHouse 最通用的引擎，支持高效的数据插入和查询
- 支持主键索引、数据分区、数据副本等特性

### 分区策略
- **按 p_date 分区**：支持快速删除过期数据，优化查询性能
- 分区粒度为天，适合按日期范围查询的场景

### 排序键设计
- **ORDER BY (p_date, user_id, action_ts)**：
  - 第一级：p_date - 配合分区键，加速时间范围查询
  - 第二级：user_id - 支持按用户查询操作历史的场景
  - 第三级：action_ts - 保证时间序列排序，支持时间范围查询

### 主键设计
- **PRIMARY KEY (p_date, user_id, action_ts)**：
  - 与 ORDER BY 一致，确保查询效率
  - user_id 虽然是业务主键，但 ClickHouse 不强制唯一性约束
  - 唯一性需要在 ETL 层保证（或使用 ReplacingMergeTree 去重）

### 性能优化
- **index_granularity = 8192**：索引粒度，平衡索引大小和查询性能
- 建议后续根据实际查询模式添加跳数索引（Skip Index）

### 查询示例

**分区裁剪查询：**
```sql
-- 查询指定日期的数据
SELECT * FROM action WHERE p_date = '20251105';

-- 查询最近 7 天的用户操作
SELECT * FROM action
WHERE p_date >= '20251029' AND p_date <= '20251105';
```

**用户行为分析：**
```sql
-- 查询某用户的最近操作历史
SELECT * FROM action
WHERE p_date >= '20251029'
  AND user_id = 1001
ORDER BY action_ts DESC
LIMIT 20;

-- 查询某用户对特定创作的操作记录
SELECT * FROM action
WHERE p_date >= '20251029'
  AND user_id = 1001
  AND creation_id = 5001
ORDER BY action_ts DESC;
```

**操作类型统计：**
```sql
-- 按操作类型统计
SELECT action_type, count() as cnt
FROM action
WHERE p_date = '20251105'
GROUP BY action_type
ORDER BY cnt DESC;

-- 按小时统计操作量（热力图分析）
SELECT
    toHour(action_ts) as hour,
    action_type,
    count() as cnt
FROM action
WHERE p_date = '20251105'
GROUP BY hour, action_type
ORDER BY hour, cnt DESC;
```

**用户活跃度分析：**
```sql
-- 统计每个用户的操作次数
SELECT
    user_id,
    count() as action_count,
    countDistinct(creation_id) as creation_count
FROM action
WHERE p_date >= '20251029' AND p_date <= '20251105'
GROUP BY user_id
ORDER BY action_count DESC
LIMIT 100;
```

### ReplacingMergeTree 替代方案（可选）

如果需要 ClickHouse 自动去重（基于相同 ORDER BY key 的数据），可以使用 ReplacingMergeTree：

```sql
CREATE TABLE IF NOT EXISTS action
(
    user_id Int64 COMMENT '用户 ID，主键',
    action_ts DateTime COMMENT '操作时间',
    action_type Int32 COMMENT '操作类型',
    creation_id Int64 COMMENT '创作 ID',
    p_date String COMMENT '导入日期分区 yyyyMMdd'
)
ENGINE = ReplacingMergeTree(action_ts)
PARTITION BY p_date
ORDER BY (p_date, user_id, action_ts)
PRIMARY KEY (p_date, user_id, action_ts)
SETTINGS index_granularity = 8192;
```

**注意**：
- ReplacingMergeTree 在后台合并时会保留 action_ts 最新的记录
- 去重不是实时的，查询时可能仍看到重复数据
- 如需保证查询结果去重，需使用 `FINAL` 关键字（影响性能）

---

## 数据类型对照

| Hive | ClickHouse | 说明 |
|------|------------|------|
| BIGINT | Int64 | 64位整数 |
| INT | Int32 | 32位整数 |
| STRING | String | 变长字符串 |
| TIMESTAMP | DateTime | 时间戳，精度到秒 |