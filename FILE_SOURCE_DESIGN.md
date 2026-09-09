# File Source Design

为 Link-Up 增加 `file` Source Connector:从**本地文件系统**与 **S3(含 S3 兼容对象存储)** 读取有界批量数据。本文档是设计规范,后续实现、Review 与测试验收都以本文为准。

参考实现:`link-up-connector-mongodb`(最新的 TableSourceFactory 全链路样板)、`link-up-connector-clickhouse`(bounded split 规划样板)。

## 1. 背景与目标

Link-Up 是离线批量数据同步引擎。文件 Source 是"数据落地文件 → 目标端表"这类离线迁移场景的入口:

- 读取本地目录/文件,读取 S3 bucket 前缀下的对象。
- 支持 CSV/TSV、JSON Lines、纯文本三类常见格式。
- 文件按字节范围拆成 bounded splits,由多个 Reader 并行读取。
- 输出统一为 `FluxRow`,复用现有 Split 分配、Channel、Metrics、Job 生命周期。

一条判断标准:只解决**离线全量读取文件**现在的问题,不因为"以后可能实时"增加任何机制。

## 2. 范围与非目标

### Stage 1(本文范围)

```text
storage:  local / s3 / sftp(同一 FileStorage 接口,同一连接器)
format:   csv / tsv(分隔文本)、jsonl、text(单列)
schema:   显式声明 或 CSV 表头发现
split:    字节范围 + 行对齐;gzip 文件整文件单 split
dataset:  单数据集(一个 path 模式对应一个逻辑表)
```

### 非目标(明确不做)

```text
Parquet / ORC / Avro 等二进制格式      -> 独立 Stage
Excel / XML / Markdown / binary 透传  -> 独立 Stage
FTP / HDFS 存储后端                   -> FileStorage 第四实现,后续 Stage
SSE-KMS 加密桶、跨账户角色assume      -> 有真实需求再加
写入端(File Sink)                    -> 独立设计文档
增量读取 / 文件变更捕获                -> 违反 bounded 边界
Split/offset 断点续读                 -> 引擎明确非目标
多数据集(目录模式→多表)              -> Stage 2,见 §13
压缩归档:ZIP / TAR 整包解包          -> Stage 2;Stage 1 仅 gzip 流压缩
```

不做 Split offset resume:Reader 失败后由新 Attempt 从 split 头部重读,与引擎"不从 Split/offset 自动恢复"的语义一致。

## 3. 模块与包结构

新增 Maven 模块 `link-up-connectors/link-up-connector-file`,包结构遵循角色分包:

```text
com.link.up.connector.file/
├── config/     FileSourceOptions、FileSourceConfig、FileFormat
├── source/     FileSourceFactory、FileSource、FileSourceSplit、
│               FileSourceSplitEnumerator、FileSourceReader
├── converter/  DelimitedRowConverter、JsonLineRowConverter、TextRowConverter
├── schema/     FileSchemaResolver(声明 schema / CSV 表头 → TableSchema)
└── internal/   FileStorage(接口)、LocalFileStorage、S3FileStorage、
                FileRowSplitter(行对齐扫描)
```

不新增 `common`、`utils` 包。`internal` 包是 Connector 私有实现区,`S3FileStorage` 与 AWS SDK 类型只能出现在这里,不得泄漏到 `source`/`config` 的公共签名。

### 依赖(遵循 DEPENDENCIES.md)

```text
link-up-api                     (compile)
auto-service                    (provided, @AutoService 注册)
slf4j-api
jackson-core/databind           (经由 link-up-api 传递,JSONL 解析)
aws sdk v2: s3 + auth/regions   (版本由根 POM 属性 <aws-sdk.version> pin)
commons-csv                     (单一 jar、零传递依赖,见下)
junit                           (test)
```

S3 SDK 选型 AWS SDK for Java v2(Java 8 兼容,仅引 s3 模块)。不引入 hadoop-aws / spark 类重依赖——那是通用计算平台的依赖面,与本项目定位冲突。commons-csv 是对"能用 JDK 解决的简单问题,不额外引库"的显式豁免:引号转义、内嵌换行、BOM 等 CSV 正确性问题手写 parser 是典型 bug 源;若依赖评审不通过,退路是 internal 包内实现严格 RFC4180 parser 并配套边界测试,不阻塞整体设计。

## 4. 角色链(遵循 CODE_STYLE.md §10)

```text
FileSourceFactory  (SPI 入口:optionRule / capabilities / discoverTableSchemas / createSource)
  -> FileSource     (创建 Enumerator 与 Reader,不持有 IO 资源)
    -> FileSourceSplitEnumerator  (列文件、规划 bounded splits,完成后关闭)
      -> FileSourceReader          (逐 split 逐 batch 读取,输出 FluxRow)
```

- `SourceSplitEnumerator` 是有界枚举器:一次 `enumerateSplits()` 返回全部 split,框架用后即关。它不得持有 Reader 或任务运行时状态。
- `FileSource` 覆写 `createEnumerator(tables, context)`,不使用 legacy `createSplits`。
- `validateParallelism` 保持默认正数校验;文件 Source 对并行度没有额外限制。
- Reader 生命周期与 MongoSourceReader 同构:`open(splits)` → 逐 split `readBatch()` → `closeSplit()` → `close()`;同一时刻只允许一个活动 split。

## 5. 配置与 Option 规则

Option 命名 snake_case,统一带 `withSemanticType` 与 `withScope`,与 MongoSourceOptions 同构。命名原则:**与既有 connector 的词汇保持一致**——列投影沿用 Mongo 的 `fields`,split 语义沿用 ClickHouse 的 `split_size`;其余取语义自明的最短表达,不引入新的命名体系。

### 5.1 Options

| Option | 类型 | 默认 | Scope | 说明 |
| --- | --- | --- | --- | --- |
| `path` | string | 必填 | DATASOURCE | 本地目录/文件路径,或 `s3://bucket/prefix/` |
| `storage_type` | string(local/s3) | auto 推断 | DATASOURCE | 缺省时按 `path` scheme 推断;显式值与之冲突时报错 |
| `format` | string(csv/tsv/text/jsonl) | auto 按扩展名 | TASK | 扩展名无法识别时必填 |
| `schema` | list<object> | 无 | TASK | `{name, type}` 列表;type 见 §8 |
| `fields` | list<string> | 全部 | TASK | 列投影,语义与 Mongo Source `fields` 一致 |
| `delimiter` | string | csv `,` / text `\001` | TASK | csv 恒为 RFC4180 逗号;tsv 自动 `\t`;text 采用 Hive 惯例 `\001`,避开正文常见字符 |
| `quote_char` | string | `"` | TASK | 仅 csv |
| `escape_char` | string | `"` | TASK | 仅 csv |
| `skip_header_rows` | long | `0` | TASK | 跳过文件前 N 行,仅 csv/tsv/text |
| `header` | boolean | `false` | TASK | 首行为表头:列名取自首行且该行不输出;与 `schema` 互斥 |
| `null_value` | string | 无 | TASK | 视为 null 的文本(如 `\N`);不配则空值语义由转换器定义 |
| `encoding` | string | `UTF-8` | TASK | 仅允许 ASCII 兼容字符集(UTF-8/GBK/GB18030/Big5 等):字节级行对齐扫描要求 0x0A/0x22 是自包含字节,UTF-16 类构造期直接拒绝 |
| `compression` | string(none/gz/auto) | `auto` | TASK | auto 按**每个文件**的扩展名识别 gzip(目录可混合);显式 none 遇 .gz 文件在枚举期报错 |
| `table_name` | string | 由 path 推导 | TASK | dataSetId / 目标逻辑表名 |
| `split_size` | long(bytes) | `134217728`(128MB) | RUNTIME | 单 split 目标大小,下限 `1048576`(1MB);行对齐可能使实际 split 略大 |
| `recursive` | boolean | `true` | RUNTIME | 目录/前缀递归枚举 |
| `file_pattern` | string | 无 | RUNTIME | 文件名正则过滤(只匹配文件名,不含目录路径),枚举期应用;按扩展名筛选用 `\.csv$` 即可 |
| `endpoint` | string | 无 | DATASOURCE | 仅 s3;S3 兼容端点(MinIO/OSS/COS) |
| `bucket` | string | 无 | DATASOURCE | 仅 s3;`path` 为 `s3://` 时可由 path 携带 |
| `access_key` | string | 无 | DATASOURCE | 仅 s3,sensitive |
| `secret_key` | string | 无 | DATASOURCE | 仅 s3,sensitive |
| `region` | string | 无 | DATASOURCE | 仅 s3;自定义 endpoint 时可省略 |
| `path_style_access` | boolean | `false` | DATASOURCE | 仅 s3;MinIO 等需要 true |
| `host` | string | 无 | DATASOURCE | 仅 sftp;SFTP 服务器地址 |
| `port` | int | `22` | DATASOURCE | 仅 sftp |
| `user` | string | 无 | DATASOURCE | 仅 sftp;登录用户 |
| `password` | string | 无 | DATASOURCE | 仅 sftp,sensitive;与 private_key 至少其一 |
| `private_key` | string | 无 | DATASOURCE | 仅 sftp;SSH 身份文件路径 |
| `strict_host_key_checking` | boolean | `false` | DATASOURCE | 仅 sftp;是否校验 known_hosts |

刻意不提供的选项与理由:

- **行分隔符**(`row_delimiter` 类):三种格式的行边界统一按 `\n` / `\r\n` 识别,文本文件场景没有需要自定义行边界的真实输入;少一个选项少一类歧义。
- **独立扩展名过滤**:与 `file_pattern` 职责重叠,一个过滤入口足够。
- **文件修改时间过滤**:同步期间文件不可变是 bounded 语义的既有约定(§6.4),按时间过滤会诱导对"正在写入的文件"做同步。

### 5.2 OptionRule

```text
required:   path
optional:   其余全部(含 schema 与 header,二者都是可选)
requiredWhen(storage_type == 's3'): bucket  → 未从 path 解析出时必填
requiredWhen(format 明确): delimiter 仅 text 生效;quote/escape 仅 csv 生效
互斥:      schema 与 header=true 在业务层校验冲突;header=false + schema 合法,因此不能放进 OptionRule 的 exclusive(exclusive 语义是"配置了即互斥",默认值会误伤)
```

利用 `OptionRule.Builder.requiredWhen(...)` 与 `Conditions.equalTo(...)` 表达条件必填;互斥由 `FileSourceConfig` 构造期判定。`access_key`/`secret_key`/`password` 声明 `.sensitive()`,错误与日志中不得输出(遵循 CODE_STYLE.md §9)。

### 5.3 FileSourceConfig

不可变配置对象,`FileSourceConfig.of(ReadonlyConfig)` 在构造期完成全部校验(路径非空、storage 推断一致、format 合法、split_size 下限、s3 必填项、encoding 可用性、schema 与 header 互斥)。构造完成即合法,Reader/Enumerator 内不再重复校验。遵循"构造器建出合法对象"(CODE_STYLE.md §6)。

## 6. Split 设计

### 6.1 Split 模型

```java
FileSourceSplit implements SourceSplit
├── splitId      // "<table_name>#<fileKey>#<seq>" 稳定可读
├── dataSetId    // table_name
├── fileKey      // 本地绝对路径 或 S3 object key
├── startOffset  // 字节起点(含)
└── length       // 字节长度;压缩文件时 == 整文件
```

`SourceSplit` 契约(Serializable、稳定 splitId、数据集放 dataSetId 不进 FluxRow)全部满足。

### 6.2 行对齐拆分(FileRowSplitter)

纯文本类格式按字节范围切分,必须保证每个 split 从**行边界**开始:

```text
文件大小 L,目标 split_size S:
  cut = S, 2S, ... 依次试探
  从 cut 向前扫描到最近行边界(实际起点),或向后扫描到下一行边界(实际终点),
  取扫描开销更小的方向。
```

- CSV 引号内嵌换行:对齐扫描必须携带引号状态,不得把 `"a\nb"` 从中间切开。引号状态从文件头或上一个 split 边界起算开销过大,采用**从 cut 向前回扫**(内部固定窗口 8MB)定位无歧义行边界;超窗时 fail-fast 并提示减小 split_size 或改用无引号数据。窗口为内部常量、不开放配置——它是正确性护栏而不是调优参数。
- JSONL/text:按 `\n` 对齐(处理 `\r\n` 与结尾无换行)。
- 文件末尾残余不足一行的字节并入最后一个 split。
- 文件大小 ≤ S:单 split,不做对齐扫描。

行对齐使实际 split 可能略大于 `split_size`。**拆分默认开启且不提供开关**:并行读取是本引擎的核心执行模型,opt-in 开关只会制造"默认慢"的路径;单文件天然不拆(≤ S),关不掉,无需关。

### 6.3 压缩文件

`compression=auto` 时按 `.gz` 扩展名(含 `.csv.gz`)识别 gzip:解压流不可随机访问,整文件一个 split,`split_size` 对其无效。显式 `compression=none` 遇到 `.gz` 扩展名时构造期直接报错——声明确认不压缩却读出乱码行,应在最早时刻暴露。目录中混合压缩/非压缩文件是合法输入,各自按上述规则规划。ZIP/TAR 归档整包解包留 Stage 2。

### 6.4 枚举语义

`enumerateSplits()` 在规划时一次性完成:列文件 → `file_pattern` 过滤 → 按文件大小生成全部 split。过滤在枚举期应用,Reader 不再感知被过滤文件。**不承诺跨 Attempt 的文件清单稳定**:重试时重新枚举,若期间文件集变化,按新清单执行。文档向用户约定:同步期间 path 下的文件应是不可变的完整文件(一次写入完成后不再追加),这是 bounded 离线同步的既有语义,不引入清单快照机制。

## 7. Reader 设计

`FileSourceReader` 职责单一:按 split 打开输入流,格式解析,批量返回。

```text
open(splits)            -> 记录分配的 splits,创建任务级资源(与 MongoSourceReader 同构)
openSplit(split)        -> 通过 FileStorage 打开 [startOffset, startOffset+length) 字节流,
                           包一层 BufferedReader(encoding);split 起点为 offset==0 且
                           skip_header_rows > 0 时跳过前 N 行
readBatch()             -> 填满 batchSize 行或当前 split 耗尽;
                           行解析失败 fail-fast,异常带 fileKey + 行号
closeSplit()            -> 关闭当前 split 的流与解析器,保留任务级资源
close()                 -> 关闭全部剩余资源
```

主执行路径是框架批量分配(`SourceTask` 调用 `open(plan.getSplits())` 后循环 `readBatch()`);`openSplit`/`closeSplit` 保持与 Mongo 样板一致的单 split 生命周期,兼容动态分配路径,不在其中引入新机制。

正确性细节:

- **表头只跳一次**:仅 `startOffset == 0` 的 split 跳过前 N 行;续接 split 从精确行边界开始,天然不含表头。`header=true` 隐含跳过首行,同样只发生在 offset==0 的 split。
- **csv/tsv 记录边界由 CSVParser 管理**:引号内嵌换行可以让一条记录横跨多个物理行,Reader 不得按 readLine 逐行解析;表头跳过仍按物理行读取,之后再把流交给 CSVParser。
- **逐文件表头校验**:`header=true` 时每个文件的表头行都与发现的 schema 逐一比对,列名或顺序不一致立即报错,不允许静默按位置串列。
- **fields 投影**:在转换器边界应用(与 Mongo `fields` 同构),split/流层面不做列裁剪——文本格式裁列不省 IO,只增加解析分支。
- **行号语义**:异常消息报告 `fileKey + split 字节起点 + split 内行号`,可唯一定位出错位置;跨 split 的绝对行号需要额外扫描,不在 Source 侧维护。
- **空文件 / 空目录**:空文件产出 0 个 split;过滤后 path 未匹配到任何文件时在枚举期抛 `TABLE_NOT_EXISTED` 语义错误(见 §9),不静默成功。
- **压缩按文件判定**:`compression=auto` 时枚举器对每个文件按扩展名识别 gzip 并整文件规划;显式 `none` 遇 `.gz` 文件在枚举期报错。
- **SFTP 整文件单 split**:SFTP 的 InputStream 变体不支持服务端 seek,按范围读取意味着丢弃偏移字节,大文件下不可接受;因此 SFTP 与 gzip 一样整文件一个 split,顺序读满即止。
- **batchSize**:沿用框架传入值;单行超过 batch 预期大小不特殊处理,由 `RecordBatch` 语义兜底。
- 转换器(`converter` 包)把字符串/JSON 值按目标 `TableSchema` 转成 Flux 物理类型,`null_value` 命中的字段转 null;无法安全转换(如非数字文本进 BIGINT 列)时 fail-fast,不做隐式截断,与 ClickHouse/DB2 适配的既有立场一致。

## 8. Schema 设计

文件没有 Catalog。`FileSchemaResolver` 按优先级解析:

```text
1. 显式 schema 配置      -> 精确类型,逐列校验名字合法、无重复;与 header 互斥
2. CSV 表头发现          -> header=true 时列名来自首行,全部映射 STRING_TYPE;
                            数值/时间等类型交给 Sink 端或映射规则,不在 Source 侧猜测
3. jsonl 推断(Stage 2)  -> 采样推断,沿用 MongoSchemaInference 的保守策略
4. text 格式            -> 单列,列名 "content",STRING_TYPE
```

类型词表使用 Flux 基础类型名(与 `SqlType` 对齐):`string / boolean / tinyint / smallint / int / bigint / float / double / decimal(p,s) / bytes / date / time / timestamp / timestamp_tz`。声明外的类型名在建 schema 阶段直接失败。

`discoverTableSchemas()` 返回单个 `CatalogTable`:

- local:读表头不触网,`validate` 语义下也可用;
- s3:读取需要网络访问,只在 `explain` 路径生效——这与"validate 不访问外部系统,explain 可以发现 Schema"的引擎契约一致,由框架调用时机保证,Connector 自身不做双模式。

`TablePath` 取 `(storage_type, table_name)` 的规范化形式(如 `file://local/sales_csv`),保证同 jobId 内稳定。

## 9. Capability 与错误语义

```text
capabilities = { TABLE_SCHEMA_DISCOVERY, PARTITION_SPLIT }
```

不声明 `MULTI_TABLE`(单数据集)、不声明 `DIRTY_DATA_HANDLING`(fail-fast,无脏数据收集选项)。schemaVersion 从 `"1"` 开始。

错误语义:Connector 层抛普通运行时异常,结构化错误映射由框架边界完成;但信息必须足够定位——`what failed / fileKey / offset 或行号`,不携带 secret 与完整 options(遵循 CODE_STYLE.md §9)。枚举/打开阶段失败不可重试配置错误(path 不存在、format 无法识别);读取阶段 IO 失败按可重试传播,由引擎结构化错误规则判定。

## 10. 存储抽象(internal)

```text
interface FileStorage extends AutoCloseable
├── List<FileEntry> listFiles(String basePath, boolean recursive)  // name + size
├── InputStream openRange(String fileKey, long start, long length)
└── boolean exists(String fileKey)

LocalFileStorage  -> java.nio.file.Files,零额外依赖
S3FileStorage     -> aws sdk v2 S3Client(ListObjectsV2 分页 + GetObject range)
```

- 抽象放在 `internal` 包。这是"先有两个实现,再做抽象"的正当场景(local/S3 第一天就并存),不是提前抽象;接口只暴露 FileEntry/InputStream,SDK 类型不出 internal。
- 接口刻意收敛为三个方法:列文件、按范围读、存在性检查。文件族后续扩展(SFTP / HDFS)都是"换一个存储实现"而非"扩接口语义";格式解析、拆分、Reader 全部复用。
- S3 凭证:显式 `access_key`/`secret_key` 优先;未配置时走 SDK v2 默认凭证链(环境变量、配置文件、容器/实例角色),不引入 provider 类名配置项。
- SFTP(`internal/SftpFileStorage`,JSch):`listFiles` 递归遍历目录,`openRange` 流式读取后丢弃偏移字节再按长度截断(JSch 的 InputStream 变体不支持服务端 seek,偏差量成为性能瓶颈时再引入 skip 通道变体),`exists` 用 stat;会话与通道归实例所有,close 时断开。`openRange` 语义与行对齐拆分天然兼容。
- SFTP 路径声明:`storage_type = "sftp"` + 绝对路径,或 `sftp://` scheme 前缀;相对路径拒绝。密码与私钥至少配置其一;默认不校验 known_hosts(与常见同步工具一致),生产环境可开启。
- S3 客户端参数(endpoint、region、path-style、凭证)由 `FileSourceConfig` 提供并归 `S3FileStorage` 持有与关闭;Enumerator 与 Reader 各自创建自己的 FileStorage 实例,不跨生命周期共享(IO 资源所有权显式,遵循 CODE_STYLE.md §8)。
- 枚举排序:文件列表按 fileKey 字典序,split 顺序确定,保证 `enumerateSplits()` 可重复。

## 11. 测试计划

遵循"测试名称描述行为"与"能自动测试的规则不写在文档里":

```text
FileSourceConfigTest
  shouldInferStorageFromPathScheme
  shouldRejectSplitSizeBelowMinimum
  shouldFailWhenS3BucketMissing
  shouldFailWhenExplicitStorageConflictsWithScheme
  shouldRejectSchemaWhenHeaderEnabled
  shouldRejectExplicitNoneCompressionForGzExtension

FileRowSplitterTest
  shouldPreserveQuotedNewlineInsideCsvField
  shouldAlignCutToNextRowBoundary
  shouldMergeTrailingPartialRowIntoLastSplit
  shouldFailFastWhenAlignmentWindowExceeded
  shouldUseWholeFileSplitForCompressedFiles

FileSourceSplitEnumeratorTest
  shouldApplyFileNamePatternFilter
  shouldSortSplitsByFileKeyForStableOrdering

DelimitedRowConverterTest
  shouldConvertDeclaredTypesWithSafeCasting
  shouldFailFastOnUnsafeNumericText
  shouldReportAbsoluteLineNumberOnMalformedRow
  shouldMapNullValueTextToNull
  shouldProjectSelectedFields

FileSourceReaderTest
  shouldSkipConfiguredHeaderRowCountOnlyOnFirstSplit
  shouldContinueRowNumberingAcrossSplits
  shouldReturnEndOfInputAfterAllSplits
```

S3 逻辑通过 `FileStorage` 接口的内存桩(stub)测试,不依赖真实 AWS;local 路径用 `@Rule TemporaryFolder` 覆盖端到端(工厂 → 枚举 → Reader)。新增 architecture guard 断言:`com.link.up.connector.file` 不 import framework,`internal` 之外的包不 import `software.amazon.awssdk`。

## 12. 交付清单

1. 根 POM 增加 `<aws-sdk.version>`、`<jsch.version>` 属性与 dependencyManagement 条目(遵循版本统一管理)。
2. 四个模块:`file-base`、`file-local`、`file-s3`、`file-sftp` + 注册进 `link-up-connectors/pom.xml`。
3. `link-up-launcher`、`link-up-dist` 依赖清单加入三个叶模块(发行包必须单独验证,拆 Maven 模块不等于 runtime 可用)。
4. 实现顺序:base(config → internal → schema → converter → source 链路)→ 三个叶模块(存储 + 工厂 + SPI)。
5. 更新 `CONNECTOR_ADAPTATION.md` 增加 File Source 章节;`README.md` 模块表补充。

## 13. Stage 2 展望(不进入当前抽象)

- Parquet/ORC 列式读取(独立 Stage,复用 FileStorage 与 split 骨架)。
- jsonl 采样推断 schema(沿用 Mongo 保守推断策略)。
- HDFS 存储后端:FileStorage 第四实现,格式解析层零改动。
- 多数据集:目录模式 → 多逻辑表,届时声明 `MULTI_TABLE` 并接受拓扑派生约束。
- ZIP/TAR 归档压缩。
- File Sink(local/S3 写出,含 SinkPreparer 提交语义)——单独设计文档。
