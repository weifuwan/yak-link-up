package com.link.up.connector.starrocks.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;
import java.util.Map;

/** StarRocks Native Source options. */
public final class StarRocksSourceOptions {

    private StarRocksSourceOptions() {
    }

    public static final Option<List<String>> NODE_URLS =
            Options.key("node_urls")
                    .listType()
                    .noDefaultValue()
                    .withFallbackKeys("nodeUrls")
                    .withDescription("StarRocks FE HTTP 地址列表，例如 [\"127.0.0.1:8030\"]")
                    .withSemanticType("STARROCKS_FE_NODES")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks 用户名")
                    .withSemanticType("USERNAME")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .defaultValue("")
                    .sensitive()
                    .withDescription("StarRocks 密码")
                    .withSemanticType("PASSWORD")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks 数据库名")
                    .withSemanticType("DATABASE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks 表名；与 table_list 二选一")
                    .withSemanticType("TABLE")
                    .withScope(ConnectorOptionScope.TASK);

    @SuppressWarnings("rawtypes")
    public static final Option<List<Map>> TABLE_LIST =
            Options.key("table_list")
                    .listType(Map.class)
                    .noDefaultValue()
                    .withDescription("多表读取配置，每项包含 table、schema.fields，可选 scan_filter")
                    .withSemanticType("TABLE_LIST")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Map<String, Object>> SCHEMA_FIELDS =
            Options.key("schema.fields")
                    .mapObjectType()
                    .noDefaultValue()
                    .withDescription("Native Scan 输出 Schema；key 为字段名，value 为 StarRocks 类型")
                    .withSemanticType("SCHEMA_FIELDS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SCAN_FILTER =
            Options.key("scan_filter")
                    .stringType()
                    .defaultValue("")
                    .withDescription("下推到 StarRocks Query Plan 的 WHERE 条件，不包含 WHERE 关键字")
                    .withSemanticType("SQL_FILTER")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> REQUEST_TABLET_SIZE =
            Options.key("request_tablet_size")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("单个 Source Split 最多包含的 Tablet 数量")
                    .withSemanticType("SPLIT_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SCAN_CONNECT_TIMEOUT_MS =
            Options.key("scan_connect_timeout_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("连接 StarRocks BE Scanner 的超时时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SCAN_QUERY_TIMEOUT_SEC =
            Options.key("scan_query_timeout_sec")
                    .intType()
                    .defaultValue(3600)
                    .withDescription("BE Scanner 查询超时，单位秒；-1 表示不限制")
                    .withSemanticType("TIMEOUT_SECONDS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SCAN_KEEP_ALIVE_MIN =
            Options.key("scan_keep_alive_min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("BE Scanner Keep Alive 时间，单位分钟")
                    .withSemanticType("DURATION_MINUTES")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SCAN_BATCH_ROWS =
            Options.key("scan_batch_rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("每次从 BE 获取的最大行数")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Long> SCAN_MEM_LIMIT =
            Options.key("scan_mem_limit")
                    .longType()
                    .defaultValue(1024L * 1024L * 1024L)
                    .withDescription("单个 BE Scanner 查询可使用的最大内存字节数")
                    .withSemanticType("MEMORY_BYTES")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("获取 FE Query Plan 的最大重试次数")
                    .withSemanticType("RETRY_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Map<String, String>> SCAN_PARAMS =
            Options.key("scan.params")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("透传给 StarRocks BE Scanner 的附加参数")
                    .withSemanticType("STARROCKS_SCAN_PARAMS")
                    .withScope(ConnectorOptionScope.RUNTIME);
}
