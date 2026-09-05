# Connector Adaptation Guide

Yak Link Up 优先复用已有执行模型，再增加数据库差异层。新增关系型数据库时，不要复制一套 Source/Sink。

## JDBC 数据库适配

以 MySQL、PostgreSQL、Oracle、SQL Server、OceanBase、DB2 为参考，一个新的 JDBC 数据库通常只需要补齐：

1. **Driver**
   - 在 `link-up-connector-jdbc` 引入 JDBC Driver。
   - 明确默认 Driver 类名。

2. **Dialect**
   - 在 `DatabaseIdentifier` 增加唯一标识。
   - 实现 `JdbcDialectFactory`，通过 SPI 注册。
   - 实现 `JdbcDialect`：标识符引用、表路径规则、UPSERT、读取 PreparedStatement、Hash Split 等数据库差异。

3. **Type Mapper / Row Converter**
   - JDBC 元数据转换为 Flux 类型。
   - Flux 类型转换为目标数据库类型。
   - 数据库特殊类型优先映射到已有 Flux 基础类型，并保留 `sourceType`；无法安全写入时明确失败，不做隐式错误转换。

4. **Catalog / DDL**
   - 实现数据库、Schema、Table、Column、Primary Key 的发现。
   - Sink 需要自动建表时，实现对应 `CREATE TABLE` Builder，并接入 `JdbcCreateTableSqlResolver`。
   - DDL 只处理任务启动前的离线准备，不把运行时 Schema Event 混入 Catalog。

5. **Tests**
   - URL / SPI 自动识别。
   - 表路径与 Identifier quoting。
   - 常用类型映射。
   - INSERT / UPSERT。
   - CREATE TABLE。
   - 确认已有数据库方言没有回归。

## 数据库差异不要强行抹平

适配时保留真正影响正确性的差异。例如 PostgreSQL cursor fetch 需要事务；Oracle `DATE` 包含时分秒、UPSERT 使用 `MERGE`；SQL Server 使用 `database.schema.table`，`timestamp` 实际是 `rowversion`，`tinyint` 是 0~255，`datetimeoffset` 需要保留时区偏移。

OceanBase 同时提供 MySQL 与 Oracle 兼容模式。Yak Link Up 通过 `compatible_mode=mysql|oracle` 显式选择语义，不根据字段或 SQL 自动猜测。MySQL 模式复用 MySQL 类型、UPSERT 与 DDL 规则；Oracle 模式复用 Oracle 类型、`MERGE`、Schema 与 DDL 规则。兼容模式分流属于 Dialect/Catalog 差异，不进入公共 Source/Sink。

DB2 LUW 的 JDBC URL 已绑定 database，SQL 对象定位使用 `schema.table`；默认 Schema 按显式 `schema`、JCC `currentSchema`、用户名依次解析。UPSERT 使用 `MERGE ... USING (VALUES ...)`。字符串分片通过 `HASH8` 生成稳定桶，避免依赖数据库排序规则做字符串 RANGE。

DB2 类型适配以数据正确性优先：`DECIMAL` 最大 precision 为 31，超过上限直接在建表阶段失败，不静默降低 scale；`TIMESTAMP` 小数秒精度最高 12；`DECFLOAT(16|34)` 不能安全降为 IEEE `DOUBLE`，Source 以精确文本承载并保留 `sourceType`；CLOB/BLOB/GRAPHIC/DBCLOB/XML 复用 Flux 基础类型。DB2 LUW 不原生支持 `TIMESTAMP WITH TIME ZONE`，目标字段遇到该类型时明确失败。

## 能力边界

JDBC Offline Connector 默认只负责全量读取、分片读取和批量写入。

CDC、Binlog/WAL、Oracle LogMiner/SCN、SQL Server CDC/Change Tracking、OceanBase Binlog/LogProxy/CLog、DB2 CDC/LSN、Replication Slot、流式 Checkpoint、XA / Exactly Once 等能力应作为独立 Stage 设计，不直接塞进离线 JDBC 方言。这样新增达梦等数据库时，只需要实现数据库差异，而不需要重复执行框架。
