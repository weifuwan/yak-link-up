package com.link.up.connector.starrocks.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;
import java.util.Map;

/** StarRocks bounded Stream Load Sink options. */
public final class StarRocksSinkOptions {

    private StarRocksSinkOptions() {
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
                    .withDescription("StarRocks 目标数据库名")
                    .withSemanticType("DATABASE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks 目标表名")
                    .withSemanticType("TABLE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> LABEL_PREFIX =
            Options.key("label_prefix")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("labelPrefix")
                    .withDescription("Stream Load label 前缀；未配置时由 database/table 生成")
                    .withSemanticType("STARROCKS_LABEL_PREFIX")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<StarRocksLoadFormat> LOAD_FORMAT =
            Options.key("load_format")
                    .enumType(StarRocksLoadFormat.class)
                    .defaultValue(StarRocksLoadFormat.JSON)
                    .withFallbackKeys("starrocks.config.format")
                    .withDescription("Stream Load payload 格式：JSON 或 CSV")
                    .withSemanticType("LOAD_FORMAT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> BATCH_MAX_ROWS =
            Options.key("batch_max_rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("单次 Stream Load 最大行数")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Long> BATCH_MAX_BYTES =
            Options.key("batch_max_bytes")
                    .longType()
                    .defaultValue(5L * 1024L * 1024L)
                    .withDescription("单次 Stream Load payload 刷新阈值，单位字节")
                    .withSemanticType("MEMORY_BYTES")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Stream Load 网络/服务端瞬态失败最大重试次数")
                    .withSemanticType("RETRY_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Stream Load 重试基础退避时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("Stream Load 最大重试退避时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("连接 StarRocks FE/BE 的超时时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(180000)
                    .withDescription("Stream Load 读写超时时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> LABEL_STATE_TIMEOUT_MS =
            Options.key("label_state_timeout_ms")
                    .intType()
                    .defaultValue(180000)
                    .withDescription("Label Already Exists 时等待最终状态的最长时间")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> LABEL_STATE_POLL_MS =
            Options.key("label_state_poll_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("查询 Stream Load label 状态的轮询间隔")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> COLUMN_SEPARATOR =
            Options.key("column_separator")
                    .stringType()
                    .defaultValue("\t")
                    .withFallbackKeys("starrocks.config.column_separator")
                    .withDescription("CSV Stream Load 列分隔符")
                    .withSemanticType("DELIMITER")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> ROW_DELIMITER =
            Options.key("row_delimiter")
                    .stringType()
                    .defaultValue("\n")
                    .withFallbackKeys("starrocks.config.row_delimiter")
                    .withDescription("CSV Stream Load 行分隔符")
                    .withSemanticType("DELIMITER")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Map<String, String>> STREAM_LOAD_PARAMS =
            Options.key("stream_load.params")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("透传给 StarRocks Stream Load 的附加 header 参数")
                    .withSemanticType("STARROCKS_STREAM_LOAD_PARAMS")
                    .withScope(ConnectorOptionScope.RUNTIME);
}
