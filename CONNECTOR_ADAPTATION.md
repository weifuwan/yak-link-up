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

## Native OLAP Connector

当数据库提供的原生批量协议或物理分片模型明显不同于 JDBC 执行模型，并且这些能力是 Connector 正确性或性能语义的一部分时，可以使用独立 Connector，而不是把数据库特有规划逻辑塞进 `link-up-connector-jdbc`。

StarRocks 使用独立的 `link-up-connector-starrocks`：

- Native Source 通过 FE `/_query_plan` 获取 opaque query plan 和 Tablet 路由。
- Source Split 绑定 BE 与 Tablet 集合，Reader 通过 StarRocks Thrift Scanner 直接读取 BE。
- BE 返回 Apache Arrow，Connector 在边界内转换为 `FluxRow`。
- StarRocks Source 不依赖 JDBC / MySQL Driver，也不复用 JDBC Split Planner。
- Stage 1 只处理 bounded full read；Schema 由任务显式声明，复杂类型在没有安全映射前明确失败。
- Stage 2 Sink 通过 Stream Load 写入：按行数/字节数形成批次，网络重试复用 label；`Label Already Exists` 必须先确认 label 最终状态，只有明确 `ABORTED` 才允许换新 label。
- Stream Load 成功 flush 已经是远端独立提交；没有真正 2PC 时，不把 Link-Up 的 `commit/abort` 生命周期包装成 Job-level exactly once。
- Stage 2 不使用 JDBC 自动建表，也不声明 CDC / DELETE / runtime schema evolution 能力。

Doris 的 bounded Native Source 同样属于独立 OLAP 数据面：

- Catalog 元数据继续使用 Doris 的 MySQL-compatible protocol，避免重复实现稳定的 schema discovery。
- 数据读取使用 FE `/_query_plan` + BE `TDorisExternalService` Scanner，BE 返回 Arrow IPC 后转换为 `FluxRow`。
- Tablet/BE 路由由 `SourceSplitEnumerator` 转成确定性的 bounded splits。
- 该 Source 不把 Doris Binlog/CDC 或 Arrow Flight SQL 混入当前离线读取阶段。

ClickHouse 使用独立的 `link-up-connector-clickhouse`，但不重复实现网络协议：

- 元数据与数据传输复用 ClickHouse 官方 Java/JDBC HTTP client。
- MergeTree-family table mode 从 `system.parts` 读取 `active = 1` 的当前 parts，并按 `split.size` 形成 `_part` bounded splits。
- `partition_list` 只约束 part discovery；`filter_query` 继续由 ClickHouse 服务端过滤。
- local table 配置多个 host 时，每个 host 被视为一个 shard 节点；不要把同一 shard 的多个 replica 当成独立 host，以免重复读取相同 parts。
- `Distributed` table 在第一阶段由一个 bounded query 交给 ClickHouse 自己做集群分发，不枚举每个 replica。
- 自定义 SQL 第一阶段是单 bounded split，不自动改写 JOIN/GROUP BY/subquery 到多个 shard；复杂 SQL 并行重写留给独立阶段。
- 高位无符号/128-bit/256-bit 数值不做有损缩窄；复杂 ARRAY/MAP/TUPLE/NESTED/AggregateFunction 在没有明确 Flux 语义前 fail-fast。
- Stage 1 不包含 CDC、连续轮询、mutation/Keeper change capture、runtime schema evolution 或 Sink。

这种例外是协议/物理分片边界，不是为数据库复制通用执行框架。Source 仍复用 Link-Up 的 `SourceSplitEnumerator`、动态 Split 分配、`SourceReader`、Channel、Metrics 和 Job 生命周期；Sink 仍复用 `SinkPreparer`、`SinkWriter`、Task commit evidence 和统一错误传播。

## 数据库差异不要强行抹平

适配时保留真正影响正确性的差异。例如 PostgreSQL cursor fetch 需要事务；Oracle `DATE` 包含时分秒、UPSERT 使用 `MERGE`；SQL Server 使用 `database.schema.table`，`timestamp` 实际是 `rowversion`，`tinyint` 是 0~255，`datetimeoffset` 需要保留时区偏移。

OceanBase 同时提供 MySQL 与 Oracle 兼容模式。Yak Link Up 通过 `compatible_mode=mysql|oracle` 显式选择语义，不根据字段或 SQL 自动猜测。MySQL 模式复用 MySQL 类型、UPSERT 与 DDL 规则；Oracle 模式复用 Oracle 类型、`MERGE`、Schema 与 DDL 规则。兼容模式分流属于 Dialect/Catalog 差异，不进入公共 Source/Sink。

DB2 LUW 的 JDBC URL 已绑定 database，SQL 对象定位使用 `schema.table`；默认 Schema 按显式 `schema`、JCC `currentSchema`、用户名依次解析。UPSERT 使用 `MERGE ... USING (VALUES ...)`。字符串分片通过 `HASH8` 生成稳定桶，避免依赖数据库排序规则做字符串 RANGE。

DB2 类型适配以数据正确性优先：`DECIMAL` 最大 precision 为 31，超过上限直接在建表阶段失败，不静默降低 scale；`TIMESTAMP` 小数秒精度最高 12；`DECFLOAT(16|34)` 不能安全降为 IEEE `DOUBLE`，Source 以精确文本承载并保留 `sourceType`；CLOB/BLOB/GRAPHIC/DBCLOB/XML 复用 Flux 基础类型。DB2 LUW 不原生支持 `TIMESTAMP WITH TIME ZONE`，目标字段遇到该类型时明确失败。

## 能力边界

JDBC Offline Connector 默认只负责全量读取、分片读取和批量写入。

CDC、Binlog/WAL、Oracle LogMiner/SCN、SQL Server CDC/Change Tracking、OceanBase Binlog/LogProxy/CLog、DB2 CDC/LSN、Replication Slot、流式 Checkpoint、XA / Exactly Once 等能力应作为独立 Stage 设计，不直接塞进离线 JDBC 方言。这样新增达梦等数据库时，只需要实现数据库差异，而不需要重复执行框架。

## 测试用 Connector:DataGen Source 与 Print Sink

`link-up-connector-datagen`（identifier `datagen`）与 `link-up-connector-print`（identifier `print`）是一对零外部依赖的测试 Connector，构成 source → sink 的完整参考链路，设计规范见 `DATA_GEN_SOURCE_DESIGN.md` 与 `PRINT_SINK_DESIGN.md`。

DataGen Source：

- 按 schema 在内存中生成有界数据，生成为纯函数 `f(config, 全局行号) -> FluxRow`；同一 `seed` 完全复现，`rows` 显式预置数据用于逐字段断言。
- 生成规则内嵌列定义（min/max、length、values、sequence_start 四类提示，冲突即报错），`row_count` 是全局总行数，由 `split_count` 均分为行区间 split。
- 不声明任何 Capability：schema 是显式契约而非发现，split 是合成行区间而非数据分区；`discoverTableSchemas` 零 IO，validate/explain 语义下均可用。

Print Sink：

- 每行数据以固定格式写入 INFO 日志（任务日志文件可查），schema 行每数据集只打一次；`PrintRowFormatter` 是纯函数，可直接对输出字符串做单元断言。
- Preparer 显式留空（无目标端 DDL、无连接校验），commit/abort 为默认 no-op，`CommitScope.TASK_LOCAL`；它是无事务 Sink 的参考实现。
- 行号按 Writer 从 1 计数，跨 Writer 不构成全序，断言用行集合比较。

## File Source

文件族连接器（`link-up-connector-file-base` 引擎 + `localfile`/`s3file`/`sftpfile` 三个叶 identifier）从本地文件系统、S3 与 SFTP 读取有界文本数据，设计规范见 `FILE_SOURCE_DESIGN.md`。

- 存储接入收敛在 base 的 `FileStorage` 接口（list / openRange / exists / wholeFileOnly），叶模块经 `FileStorageFactory` 注入各自的实现（`LocalFileStorage`、AWS SDK v2 的 `S3FileStorage`、JSch 的 `SftpFileStorage`），厂商 SDK 只存在于对应叶模块。格式解析、行对齐拆分、表头发现在 base 中一次实现，三个存储全部复用。
- 格式 Stage 1 支持 csv/tsv/text/jsonl；csv 用 commons-csv 严格解析（引号、转义、内嵌换行），jsonl 需显式 schema，text 输出单列。
- 拆分按字节范围 + 行对齐回扫：cut 处引号深度取窗口内引号总数奇偶，候选换行符与 cut 之间引号数保持该深度才是真实行边界；窗口超限 fail-fast。gz 文件按文件判定并整文件单 split。
- Schema 优先级：显式声明 > csv/tsv 表头发现（全 STRING）> text 单列；jsonl 推断留待后续 Stage。
- Capability 声明 `TABLE_SCHEMA_DISCOVERY` 与 `PARTITION_SPLIT`；S3 表头发现需要网络访问，只在 explain 路径生效，由引擎的 validate/explain 契约保证。
