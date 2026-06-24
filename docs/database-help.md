# IEDB MCP Server 帮助

本文件为 IEDB MCP Server 的数据库操作提示词，面向 LLM 作为"工具使用指南"。内容简洁、结构化，便于快速定位正确工具和注意事项。

---

## 可用工具一览

| 工具 | 用途 |
|------|------|
| `list_databases` | 列出所有数据库及其 measurement 数量 |
| `list_measurements` | 列出指定数据库中的所有 measurement |
| `describe_measurement` | 查看 measurement 的 schema（tags、fields、列类型） |
| `get_sample_data` | 获取 measurement 的最近样本数据（按时间倒序） |
| `query` | 执行只读 SQL 查询（DuckDB 方言） |
| `write_line_protocol` | 用 InfluxDB Line Protocol 写入数据 |
| `load_database_context` | 加载数据库上下文文档（业务背景、数据说明） |
| `get_help` | 获取本帮助文档 |

---

## 快速开始

1. **列出数据库**：调用 `list_databases` 了解数据范围。
2. **查看结构**：调用 `list_measurements` → `describe_measurement` 获取表和字段信息。
3. **浏览样本**：调用 `get_sample_data` 查看最近数据，了解数据形态。
4. **再做查询/写入**：查询时加时间范围；写入前校验行协议。

---

## 线协议写入（Line Protocol）

工具：`write_line_protocol`

格式：

```
measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
```

示例：

```
temperature,location=office value=23.5 1640995200000000000
sensor_data,device=s1,room=lab temperature=22.5,humidity=45.2,status="ok"
```

参数：
- `database`：目标数据库名（必填）
- `data`：Line Protocol 数据（必填）
- `precision`：时间戳精度，可选 `ns`、`u`、`ms`、`s`、`m`、`h`（默认 `ns`）

规则：
- **至少 1 个 field**
- **tag 是索引元数据**，值只能是字符串
- **field 可以是数值/字符串/布尔**
- **timestamp 可选**（纳秒）
- **tag 中空格/逗号/等号需转义**

常见错误：
- 缺少 field
- 时间戳格式错误
- tag 未正确转义

---

## 查询（DuckDB SQL）

工具：`query`

iedb 使用 **DuckDB SQL 方言**，支持标准分析 SQL 包括聚合、JOIN、CTE、窗口函数，以及 `time_bucket()`、`date_trunc()` 等时间函数。

参数：
- `database`：目标数据库名（必填）
- `sql`：只读 SQL 查询（必填，写操作会被拦截）

示例：

```
SELECT field1, field2 FROM measurement
WHERE time >= '2024-01-01T00:00:00Z' AND tag1 = 'value'
```

时间过滤：
- `time >= '2024-01-01T00:00:00Z'`
- `time >= now() - interval '1 hour'`
- `time BETWEEN '2024-01-01' AND '2024-01-02'`

常用查询：
- 列出 measurement：使用 `list_measurements` 工具
- 查看 schema：使用 `describe_measurement` 工具
- 最近数据：使用 `get_sample_data` 工具，或 `SELECT * FROM measurement ORDER BY time DESC LIMIT 10`
- 聚合：`SELECT AVG(field1), MAX(field2) FROM measurement WHERE time >= now() - interval '1 day'`
- 时间分桶：`SELECT time_bucket(INTERVAL '1 hour', time) AS bucket, AVG(value) FROM measurement GROUP BY bucket`

性能建议：
- **始终加时间范围**
- **避免 `SELECT *`**
- **优先用 tag 过滤**
- **大数据先 COUNT 再复杂查询**

安全限制：
- 只允许 SELECT 查询，INSERT/UPDATE/DELETE/DROP/ALTER/CREATE 等写操作会被拦截
- 不允许多语句查询（分号分隔）
- 查询结果行数受上限约束（默认 500 行）

---

## 标识符命名规则

数据库和 measurement 名称需满足：
- 只允许**字母（含 Unicode，如中文）、数字、下划线**
- 以字母或下划线开头
- 最长 128 字符

---

## 故障排查

查询错误：
- SQL 语法错误 — 确认使用 DuckDB 方言
- measurement/field 名称不正确 — 先用 `describe_measurement` 确认 schema
- 未加时间范围导致性能差
- 写操作被拦截 — `query` 工具只支持只读查询

写入错误：
- Line Protocol 格式错误
- 缺少 field
- tag 未转义
- 数据库名称无效 — 检查命名规则

认证错误：
- 检查 API Token 是否正确配置
- 确认 Token 是否有对应数据库的访问权限

---

## 提示词使用说明

当用户提出需求时：
1. 先用 `list_databases` 和 `list_measurements` 了解数据范围
2. 用 `describe_measurement` 确认 schema 后再写查询
3. 给出最小可执行步骤
4. 出错时优先用 `describe_measurement` 校验字段名

保持简洁、可执行、避免过度推断。