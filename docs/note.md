## MQTT 数据接入

存储位置 /data/iedb/iedb.db （mqtt_subscriptions）

### 1.通过API创建订阅
```bash
curl -X POST http://localhost:8080/api/v1/mqtt/subscriptions \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $TOKEN" \
-d '{
"name": "test-sensors",
"broker": "tcp://localhost:1883",
"client_id": "iedb-test",
"topics": ["sensors/#"],
"database": "mqtt_test"
}
```

### 2.开始订阅
```bash
curl -X POST http://localhost:8080/api/v1/mqtt/subscriptions/{id}/start \
-H "Authorization: Bearer $TOKEN"
```
### 3.支持格式
#### (1) 标准格式
```bash
{
    "measurement": "environment",
    "time": 1706248500000,
    "tags": {
        "location": "office",
        "sensor_id": "S001"
    },
    "fields": {
        "temperature": 25.5,
        "humidity": 60.2,
        "status": "active"
    }
}
```
* measurement或简写为 "m"。不填默认为 "mqtt"
* time或 "t", "timestamp"。支持秒、毫
* tags 索引字段 (String)
* fields 数据字段 (Int, Float, String, Bool)

#### (2) 扁平/IoT 格式

* 简化格式，适合直接从设备转发的数据。系统会自动将顶层字段识别为 Fields，将 dn 识别为 Tags。
* dn (Device Name)：会自动被转换为 Tag。
* 其他字段：自动转换为 Fields。
* 嵌套对象：例如 properties 下的字段会被自动展平提取到根 Fields 中。

* (1) 示例 A：直接平铺: 此时 Measurement 默认为 "mqtt"，时间默认为当前接收时间
```bash
{
    "dn": "device_1024",
    "temperature": 23.5,
    "voltage": 3.6
}
```


* (2) 示例 B：包含属性对象 某些 IoT 平台（如阿里云/腾讯云）常见格式，会自动展平 properties 等嵌套对象。
```bash
{
  "sys_id": "123",
  "properties": {
  "current": 1.2,
  "power": 220
  }
}
```

* (3) 罗米云默认使用数组对象批量上传格式
```bash
  [{
  "desc": "",
  "dn": "Device1",
  "properties": {
  "E": 3660.25,
  "EQ": 0,
  "Er": 0
  },
  "time": 1769413400
  },
  {
  "desc": "",
  "dn": "Device2",
  "properties": {
  "Ua": 260,
  "Ub": 260,
  "Uc": 260
  },
  "time": 1769413400
  }]
```



## 分区压缩

* 存储位置 /data/iedb/{database}/mqtt/YYYY/MM/DD/HH
* 存储格式 parquet
* 压缩算法 Snappy, ZSTD
* 参数修改 iedb.toml [compaction]

### 1.触发层(Trigger)
#### 自动压缩：
系统默认会每小时把刚写的小文件合并一次；每天凌晨3点会把前一天的数据再合并成一个大文件
#### 手动触发压缩: 
66文件-234KB -> 1文件-9KB 数据经过压缩后体积减少了近 96%
#### 执行参数:
```bash
curl -X POST http://localhost:8000/api/compaction/trigger \
-H "Authorization: Bearer YOUR_TOKEN"
```
### 2.管理层(Manager)
#### Manager 负责协调工作
* 检查状态: 确认当前是否已有压缩循环 (Cycle) 正在运行。
* 是: 跳过本次触发，防止冲突。
* 否: 生成新的 CycleID，开始执行。
* 按层级遍历: 依次执行已启用的层级策略：
* 先执行 Hourly (每小时)
* 后执行 Daily (每天)

### 3. 发现层(Discovery)
#### 寻找压缩分区 (FindCandidates)：

* 列出数据库: 扫描数据目录，找出所有数据库 (listDatabases)。
* 列出测量表: 找出数据库下的所有 Measurement。
* 扫描分区: 针对每个层级（如 Hourly），扫描对应的分区目录。
* 筛选候选者:
* 检查1 (数量): 文件数是否超过 MinFiles (默认5个)？
* 检查2 (时间): 数据是否旧于 CutoffTime？
* Hourly: MinAgeHours=0 (立即压缩)
* Daily: MinAgeHours=24 (只压缩昨天的数据)
* 结果: 满足条件的分区被加入“候选列表”。
### 4. 执行层 (Execution)
#### 候选分区执行压缩：

* 并发控制: 获取信号量，限制同时运行的任务数。
* 加锁: 锁定该分区，防止其他任务同时操作。
* 启动子进程 (SubProcess):
* 为了内存隔离，启动一个独立的 OS 进程来执行压缩。
* DuckDB 操作:
* 读取该分区下所有旧的 Parquet 文件。
* 在内存/磁盘中进行排序 (Sort) 和去重 (Deduplicate)。
* 将结果写入一个新的 _compacted.parquet 文件。
* 验证: 检查新文件是否完整。
* 原子替换:
* 成功: 删除旧的零散文件，保留新文件。
* 失败: 清理生成的临时文件，原有数据不变。
### 5. 结束 (Finish)
* 释放分区锁。
* 更新统计数据（压缩了多少文件、节省了多少空间）。
* 继续处理下一个 Measurement 或结束循环。

### 数据库
#### 1.结构设计
* Client → IEDB API → Buffer (DuckDB Query Engine) → Parquet → Storage (S3/MinIO/Azure/Local)

#### 2.数据生命周期

#### 1) 连续查询
##### 实现功能
* 数据降采样：将高频数据聚合为低频摘要
* 减少存储空间：存储聚合数据而非原始指标。
* 保留历史记录：保留长期趋势，但无需完全细粒度数据
* 提升查询性能：查询预聚合数据以加快结果获取速度
* 创建物化视图：自动维护聚合数据集
##### 工作原理
* 连续查询使用 DuckDB SQL 将源测量数据聚合到目标测量数据中
* 设置计划：配置分组的时间间隔（例如，每小时、每天）支持手动执行
* 存储结果：将汇总数据写入新的measurement
* 应用保留策略：可选择为聚合数据、源数据设置自定义保留策略
* 参数修改 iedb.toml [continuous_query]
##### 执行参数: 
```bash
#创建 CQ（使用占位符，is_active激活 则默认首次执行处理 1 小时数据）
`curl -X POST http://192.168.0.87:8000/api/v1/continuous_queries \
  -H "Content-Type: application/json" \
  -d '{
  "name": "mqtt_hourly_avg",
  "database": "default",
  "source_measurement": "mqtt",
  "destination_measurement": "mqtt_hourly",
  "query": "SELECT time_bucket('\''1h'\'', time) AS time, dn, avg(E) AS avg_e FROM default.mqtt WHERE time BETWEEN '\''{start_time}'\'' AND '\''{end_time}'\'' GROUP BY time, dn",
  "interval": "1h",
  "retention_days": 90,
  "is_active": true
}'`
#手动执行可以指定时间范围
`curl -X POST http://192.168.0.87:8000/api/v1/continuous_queries/:id/execute \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2026-03-09T00:00:00Z",
    "end_time": "2026-03-11T00:00:00Z"
  }'`
#激活 CQ，让调度器每小时自动执行
`curl -X PUT http://192.168.0.87:8000/api/v1/continuous_queries/1 \
  -H "Content-Type: application/json" \
  -d '{"is_active": true}'`
```
#### 2) 对象存储
##### 实现功能
* 数据持久化：将 Parquet 文件存储到远程对象存储（S3）
* 无限扩展：利用对象存储的弹性容量，突破本地磁盘限制
* 成本优化：冷数据归档到低成本存储层（如 S3 Glacier）
* 分层存储：热数据本地 + 冷数据远程的混合架构
* 数据安全：多副本存储，避免单点故障

##### 工作原理
* 存储后端抽象：统一的 Backend 接口，支持 S3、Azure Blob、本地存储
* 自动上传：数据写入时同步或异步上传到对象存储
* 路径映射：将database/measurement/时间分区映射为对象键路径
* 大文件优化：超过 100MB 自动启用多段上传（Multipart Upload）
* 流式处理：使用 UploadStream 避免一次性加载到内存
* 参数修改 iedb.toml [storage] 或 [tiered_storage]

##### 配置示例
** 分层存储（热数据本地 + 冷数据远程）**
```toml
# 本地热存储
[storage.local]
enabled = true
data_dir = "/mnt/nvme/iedb-hot"

# 远程冷存储
[tiered_storage]
enabled = true
default_hot_max_age_days = 30       # 30 天以上的数据迁移到冷存储
migration_schedule = "0 2 * * *"    # 每天凌晨 2 点执行

[tiered_storage.cold]
enabled = true
backend = "s3"                      # 或 "azure"
s3_bucket = "my-iedb-archive"
s3_region = "us-east-1"
s3_storage_class = "GLACIER"        # S3 归档层
```

##### 存储路径结构
```
# S3 路径
s3://bucket/{database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/data.parquet
例如：
s3://my-iedb-data/default/mqtt/2026/03/10/09/data.parquet

# 本地路径
/data/iedb/{database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/data.parquet
例如：
/data/iedb/default/mqtt/2026/03/10/09/data.parquet
```

##### 认证方式
**S3 认证：**
* 静态密钥：配置文件的 `access_key` / `secret_key`
* 环境变量：`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
* 支持 S3 自建

##### 执行参数
```bash
# 查看存储状态
curl http://localhost:8000/api/v1/storage/status

# 手动触发数据迁移（热 → 冷）
curl -X POST http://localhost:8000/api/v1/tiering/migrate \
  -H "Content-Type: application/json" \
  -d '{
    "database": "default",
    "measurement": "mqtt",
    "before_time": "2026-02-10T00:00:00Z"
  }'

# 查询数据（自动透明访问冷热存储）
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM default.mqtt WHERE time >= '\''2026-03-01'\''"
  }'
```