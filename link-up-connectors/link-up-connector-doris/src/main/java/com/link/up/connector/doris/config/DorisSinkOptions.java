package com.link.up.connector.doris.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.Map;

/**
 * Doris Sink 配置项。
 *
 * <p>参考 SeaTunnel Doris Sink 参数设计，
 * 内部通过 Stream Load 将数据批量写入 Doris。
 */
public final class DorisSinkOptions {

    private DorisSinkOptions() {
    }

    // ── 连接配置 ──────────────────────────────────────────

    public static final Option<String> FENODES =
            Options.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 集群 FE HTTP 地址，格式 fe_ip:fe_http_port，多个逗号分隔")
                    .withSemanticType("DORIS_FENODES")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> BENODES =
            Options.key("benodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris BE HTTP 地址列表，direct_to_be=true 时使用")
                    .withSemanticType("DORIS_BENODES")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Boolean> DIRECT_TO_BE =
            Options.key("direct_to_be")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否将 Stream Load 请求直接发送到 BE 节点")
                    .withSemanticType("DORIS_DIRECT_TO_BE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> QUERY_PORT =
            Options.key("query-port")
                    .intType()
                    .defaultValue(9030)
                    .withDescription("Doris FE MySQL 协议查询端口")
                    .withSemanticType("DORIS_QUERY_PORT")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 用户名")
                    .withSemanticType("USERNAME")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("Doris 密码")
                    .withSemanticType("PASSWORD")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    // ── 目标表配置 ──────────────────────────────────────────

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 数据库名")
                    .withSemanticType("DATABASE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 表名")
                    .withSemanticType("TABLE")
                    .withScope(ConnectorOptionScope.TASK);

    // ── Stream Load 配置 ──────────────────────────────────────

    public static final Option<String> SINK_LABEL_PREFIX =
            Options.key("sink.label-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load 标签前缀，2PC 场景需全局唯一")
                    .withSemanticType("LABEL_PREFIX")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_ENABLE_2PC =
            Options.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否启用两阶段提交")
                    .withSemanticType("ENABLE_2PC")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_ENABLE_DELETE =
            Options.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否启用删除（需 Doris 表开启批量删除，仅 Unique 模型）")
                    .withSemanticType("ENABLE_DELETE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SINK_CHECK_INTERVAL_MS =
            Options.key("sink.check-interval")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("检查加载异常的时间间隔，单位毫秒")
                    .withSemanticType("CHECK_INTERVAL")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("写入失败最大重试次数")
                    .withSemanticType("MAX_RETRIES")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SINK_BUFFER_SIZE =
            Options.key("sink.buffer-size")
                    .intType()
                    .defaultValue(262144)
                    .withDescription("Stream Load 数据缓冲区大小（字节），默认 256KB")
                    .withSemanticType("BUFFER_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SINK_BUFFER_COUNT =
            Options.key("sink.buffer-count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Stream Load 数据缓冲区计数")
                    .withSemanticType("BUFFER_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> DORIS_BATCH_SIZE =
            Options.key("doris.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("每次 HTTP 请求写入的行数阈值")
                    .withSemanticType("BATCH_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    // ── 数据格式 ──────────────────────────────────────────

    public static final Option<DorisLoadFormat> LOAD_FORMAT =
            Options.key("doris.load-format")
                    .enumType(DorisLoadFormat.class)
                    .defaultValue(DorisLoadFormat.JSON)
                    .withDescription("Stream Load 数据格式：JSON 或 CSV")
                    .withSemanticType("LOAD_FORMAT")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> CSV_COLUMN_SEPARATOR =
            Options.key("doris.csv.column-separator")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("CSV 格式的列分隔符")
                    .withSemanticType("CSV_SEPARATOR")
                    .withScope(ConnectorOptionScope.TASK);

    // ── Doris 额外配置 ──────────────────────────────────────

    /**
     * 透传给 Doris Stream Load 的额外配置。
     *
     * <p>例如 format、read_json_by_line、column_separator 等。
     */
    public static final Option<Map<String, String>> DORIS_CONFIG =
            Options.key("doris.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("透传给 Doris Stream Load 的额外 HTTP Header 配置")
                    .withSemanticType("DORIS_CONFIG")
                    .withScope(ConnectorOptionScope.TASK);

    // ── 建表配置 ──────────────────────────────────────────

    public static final Option<String> SINK_CREATE_TABLE_DDL =
            Options.key("sink.create-table-ddl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("用户自定义建表 DDL，优先级高于自动生成。" +
                            "支持 Doris 完整 CREATE TABLE 语法，包括表模型、分布方式、PROPERTIES 等")
                    .withSemanticType("CREATE_TABLE_DDL")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_KEY_TYPE =
            Options.key("sink.key-type")
                    .stringType()
                    .defaultValue("DUPLICATE")
                    .withDescription("Doris 表模型类型：DUPLICATE（明细模型）、UNIQUE（主键模型）、AGGREGATE（聚合模型）。" +
                            "仅在未指定 sink.create-table-ddl 时生效")
                    .withSemanticType("KEY_TYPE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SINK_BUCKETS =
            Options.key("sink.buckets")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Doris 建表 DISTRIBUTED BY HASH 的 bucket 数量，仅在自动生成 DDL 时生效")
                    .withSemanticType("BUCKETS")
                    .withScope(ConnectorOptionScope.TASK);

    // ── Stream Load 扩展参数 ──────────────────────────────────

    public static final Option<Integer> SINK_LOAD_TIMEOUT_SEC =
            Options.key("sink.load-timeout")
                    .intType()
                    .defaultValue(600)
                    .withDescription("Stream Load 导入超时时间（秒），可设置范围 1~259200")
                    .withSemanticType("TIMEOUT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Double> SINK_MAX_FILTER_RATIO =
            Options.key("sink.max-filter-ratio")
                    .doubleType()
                    .defaultValue(0.0)
                    .withDescription("Stream Load 最大容忍可过滤的数据比例（0~1），默认零容忍")
                    .withSemanticType("MAX_FILTER_RATIO")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> SINK_COLUMNS =
            Options.key("sink.columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load 列映射，指定导入文件中的列和表中的列的对应关系。" +
                            "例如：user_id,name,age 或 user_id,name,age=age+1")
                    .withSemanticType("COLUMNS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_WHERE =
            Options.key("sink.where")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load 数据过滤条件，例如：age>=35")
                    .withSemanticType("WHERE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_PARTITIONS =
            Options.key("sink.partitions")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load 指定导入分区，例如：p1, p2")
                    .withSemanticType("PARTITIONS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_STRICT_MODE =
            Options.key("sink.strict-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Stream Load 是否开启严格模式")
                    .withSemanticType("STRICT_MODE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> SINK_TIMEZONE =
            Options.key("sink.timezone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load 导入所使用的时区，默认为集群当前时区")
                    .withSemanticType("TIMEZONE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Long> SINK_EXEC_MEM_LIMIT =
            Options.key("sink.exec-mem-limit")
                    .longType()
                    .defaultValue(2147483648L)
                    .withDescription("Stream Load 导入内存限制（字节），默认 2GB")
                    .withSemanticType("EXEC_MEM_LIMIT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> SINK_JSONPATHS =
            Options.key("sink.jsonpaths")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load JSON 列映射路径，例如：[\"$.userid\", \"$.username\"]")
                    .withSemanticType("JSONPATHS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_STRIP_OUTER_ARRAY =
            Options.key("sink.strip-outer-array")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Stream Load JSON 格式是否展平外层数组")
                    .withSemanticType("STRIP_OUTER_ARRAY")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_JSON_ROOT =
            Options.key("sink.json-root")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load JSON 根节点路径，例如：$.comment")
                    .withSemanticType("JSON_ROOT")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SINK_SEND_BATCH_PARALLELISM =
            Options.key("sink.send-batch-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Stream Load 发送批处理数据的并行度")
                    .withSemanticType("SEND_BATCH_PARALLELISM")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Boolean> SINK_LOAD_TO_SINGLE_TABLET =
            Options.key("sink.load-to-single-tablet")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Stream Load 是否只导入数据到对应分区的一个 Tablet")
                    .withSemanticType("LOAD_TO_SINGLE_TABLET")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> SINK_LINE_DELIMITER =
            Options.key("sink.line-delimiter")
                    .stringType()
                    .defaultValue("\n")
                    .withDescription("Stream Load 换行符，默认 \\n")
                    .withSemanticType("LINE_DELIMITER")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_ENCLOSE =
            Options.key("sink.enclose")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load CSV 包围符，用于防止分隔符截断字段")
                    .withSemanticType("ENCLOSE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_ESCAPE =
            Options.key("sink.escape")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load CSV 转义符")
                    .withSemanticType("ESCAPE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_NUM_AS_STRING =
            Options.key("sink.num-as-string")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Stream Load JSON 格式是否将数字类型转为字符串，避免精度丢失")
                    .withSemanticType("NUM_AS_STRING")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_FUZZY_PARSE =
            Options.key("sink.fuzzy-parse")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Stream Load JSON 格式是否以第一行为 schema 解析，可提升效率")
                    .withSemanticType("FUZZY_PARSE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_COMPRESS_TYPE =
            Options.key("sink.compress-type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load CSV 文件压缩格式，支持 gz, lzo, bz2, lz4, lzop, deflate。" +
                            "仅对 CSV 格式生效")
                    .withSemanticType("COMPRESS_TYPE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_TRIM_DOUBLE_QUOTES =
            Options.key("sink.trim-double-quotes")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Stream Load CSV 格式是否裁剪每个字段最外层的双引号")
                    .withSemanticType("TRIM_DOUBLE_QUOTES")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SINK_SKIP_LINES =
            Options.key("sink.skip-lines")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Stream Load CSV 格式跳过前 N 行（设置 format=csv_with_names 时自动失效）")
                    .withSemanticType("SKIP_LINES")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SINK_LOAD_COMMENT =
            Options.key("sink.load-comment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load 导入任务附加备注信息")
                    .withSemanticType("COMMENT")
                    .withScope(ConnectorOptionScope.TASK);

    // ── 超时 ──────────────────────────────────────────────

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("HTTP 连接超时，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(300000)
                    .withDescription("HTTP Socket 读取超时，单位毫秒（Stream Load 可能较慢）")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.TASK);
}
