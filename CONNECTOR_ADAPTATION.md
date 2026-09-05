# Connector Adaptation Guide

Yak Link Up 优先复用已有执行模型，再增加数据库差异层。新增关系型数据库时，不要复制一套 Source/Sink。

## JDBC 数据库适配

以 MySQL、PostgreSQL、Oracle 为参考，一个新的 JDBC 数据库通常只需要补齐：

1. **Driver**
   - 在 `link-up-connector-jdbc` 引入 JDBC Driver。
   - 明确默认 Driver 类名。

2. **Dialect**
   - 在 `DatabaseIdentifier` 增加唯一标识。
   - 实现 `JdbcDialectFactory`，通过 SPI 注册。
   - 实现 `JdbcDialect`：标识符引用、`schema.table` 规则、UPSERT、读取 PreparedStatement、Hash Split 等数据库差异。

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

适配时保留真正影响正确性的差异。例如 PostgreSQL cursor fetch 需要事务，Oracle `DATE` 包含时分秒、UPSERT 使用 `MERGE`，Oracle SQL 对象定位是 `schema.table`。这些差异应放在 Dialect/TypeMapper/Catalog 中，而不是塞进公共 Source/Sink。

## 能力边界

JDBC Offline Connector 默认只负责全量读取、分片读取和批量写入。

CDC、Binlog/WAL、Oracle LogMiner/SCN、Replication Slot、流式 Checkpoint、XA / Exactly Once 等能力应作为独立 Stage 设计，不直接塞进离线 JDBC 方言。这样新增 SQL Server、DB2 等数据库时，只需要实现数据库差异，而不需要重复执行框架。
