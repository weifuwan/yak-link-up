package com.link.up.connector.http.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.Map;

/**
 * HTTP Source 配置项。
 *
 * <p>参考 SeaTunnel HTTP Source 参数设计，
 * 并结合 Link-Up 配置体系进行优化。
 */
public final class HttpSourceOptions {

    private HttpSourceOptions() {
    }

    // ── 基础请求配置 ──────────────────────────────────────

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HTTP 请求地址")
                    .withSemanticType("HTTP_URL")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<HttpMethod> METHOD =
            Options.key("method")
                    .enumType(HttpMethod.class)
                    .defaultValue(HttpMethod.GET)
                    .withDescription("HTTP 请求方法，支持 GET、POST")
                    .withSemanticType("HTTP_METHOD")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Map<String, String>> HEADERS =
            Options.key("headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("HTTP 请求头")
                    .withSemanticType("HTTP_HEADERS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Map<String, String>> PARAMS =
            Options.key("params")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("HTTP 请求参数（追加到 URL 查询字符串）")
                    .withSemanticType("HTTP_PARAMS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> BODY =
            Options.key("body")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HTTP 请求体，POST 时作为 JSON Body 发送")
                    .withSemanticType("HTTP_BODY")
                    .withScope(ConnectorOptionScope.TASK);

    // ── 数据格式与 Schema ──────────────────────────────────

    public static final Option<HttpFormat> FORMAT =
            Options.key("format")
                    .enumType(HttpFormat.class)
                    .defaultValue(HttpFormat.JSON)
                    .withDescription("响应数据格式：json 或 text")
                    .withSemanticType("DATA_FORMAT")
                    .withScope(ConnectorOptionScope.TASK);

    /**
     * 用户定义的输出 Schema 字段映射。
     *
     * <p>key 为字段名，value 为类型名（string、int、bigint 等）。
     */
    public static final Option<Map<String, Object>> SCHEMA_FIELDS =
            Options.key("schema.fields")
                    .mapObjectType()
                    .noDefaultValue()
                    .withDescription("用户定义的输出 Schema 字段映射")
                    .withSemanticType("SCHEMA_FIELDS")
                    .withScope(ConnectorOptionScope.TASK);

    // ── JSON 提取 ──────────────────────────────────────────

    /**
     * 从响应 JSON 中提取数据数组的 JsonPath。
     *
     * <p>例如 {@code $.data.list} 或 {@code $.store.book.*}。
     */
    public static final Option<String> CONTENT_FIELD =
            Options.key("content_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("从响应 JSON 中提取数据数组的 JsonPath 表达式")
                    .withSemanticType("JSON_PATH")
                    .withScope(ConnectorOptionScope.TASK);

    /**
     * 字段名到 JsonPath 的映射。
     *
     * <p>每个字段的值是一个 JsonPath 表达式，
     * 用于从响应 JSON 中提取该字段的值数组。
     */
    public static final Option<Map<String, Object>> JSON_FIELD =
            Options.key("json_field")
                    .mapObjectType()
                    .noDefaultValue()
                    .withDescription("字段名到 JsonPath 表达式的映射")
                    .withSemanticType("JSON_FIELD_MAPPING")
                    .withScope(ConnectorOptionScope.TASK);

    // ── 分页配置 ──────────────────────────────────────────

    public static final Option<String> PAGE_FIELD =
            Options.key("paging.page_field")
                    .stringType()
                    .defaultValue("page")
                    .withDescription("分页字段名")
                    .withSemanticType("PAGE_FIELD")
                    .withScope(ConnectorOptionScope.TASK)
                    .withFallbackKeys("pageing.page_field");

    public static final Option<Long> TOTAL_PAGE_SIZE =
            Options.key("paging.total_page_size")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("总页数，0 表示根据返回行数判断是否继续")
                    .withSemanticType("TOTAL_PAGES")
                    .withScope(ConnectorOptionScope.TASK)
                    .withFallbackKeys("pageing.total_page_size");

    public static final Option<Integer> PAGE_BATCH_SIZE =
            Options.key("paging.batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("每页返回行数，用于判断是否继续翻页")
                    .withSemanticType("PAGE_BATCH_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME)
                    .withFallbackKeys("pageing.batch_size");

    public static final Option<Integer> START_PAGE_NUMBER =
            Options.key("paging.start_page_number")
                    .intType()
                    .defaultValue(1)
                    .withDescription("起始页码")
                    .withSemanticType("START_PAGE")
                    .withScope(ConnectorOptionScope.TASK)
                    .withFallbackKeys("pageing.start_page_number");

    public static final Option<PageType> PAGE_TYPE =
            Options.key("paging.page_type")
                    .enumType(PageType.class)
                    .defaultValue(PageType.PAGE_NUMBER)
                    .withDescription("分页类型：PAGE_NUMBER 或 CURSOR")
                    .withSemanticType("PAGE_TYPE")
                    .withScope(ConnectorOptionScope.TASK)
                    .withFallbackKeys("pageing.page_type");

    public static final Option<String> CURSOR_FIELD =
            Options.key("paging.cursor_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cursor 分页时请求参数中的游标字段名")
                    .withSemanticType("CURSOR_FIELD")
                    .withScope(ConnectorOptionScope.TASK)
                    .withFallbackKeys("pageing.cursor_field");

    public static final Option<String> CURSOR_RESPONSE_FIELD =
            Options.key("paging.cursor_response_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cursor 分页时从响应中提取游标值的 JsonPath")
                    .withSemanticType("CURSOR_RESPONSE_FIELD")
                    .withScope(ConnectorOptionScope.TASK)
                    .withFallbackKeys("pageing.cursor_response_field");

    /**
     * 是否使用占位符替换（${page}、${cursor}）。
     *
     * <p>为 true 时，headers、params、body 中的 {@code ${page}} 等占位符
     * 会被替换为实际值；为 false 时，仅按 key 进行替换。
     */
    public static final Option<Boolean> USE_PLACEHOLDER_REPLACEMENT =
            Options.key("paging.use_placeholder_replacement")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否使用占位符替换分页参数")
                    .withSemanticType("PLACEHOLDER_REPLACEMENT")
                    .withScope(ConnectorOptionScope.TASK)
                    .withFallbackKeys("pageing.use_placeholder_replacement");

    // ── 重试与超时 ──────────────────────────────────────────

    public static final Option<Integer> RETRY =
            Options.key("retry")
                    .intType()
                    .defaultValue(3)
                    .withDescription("请求失败最大重试次数")
                    .withSemanticType("RETRY_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS =
            Options.key("retry_backoff_multiplier_ms")
                    .intType()
                    .defaultValue(100)
                    .withDescription("重试退避乘子，单位毫秒")
                    .withSemanticType("RETRY_BACKOFF")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> RETRY_BACKOFF_MAX_MS =
            Options.key("retry_backoff_max_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("重试退避上限，单位毫秒")
                    .withSemanticType("RETRY_BACKOFF_MAX")
                    .withScope(ConnectorOptionScope.RUNTIME);

    /**
     * 触发重试的 HTTP 状态码列表。
     *
     * <p>默认包含 429（Too Many Requests）、500、502、503、504，
     * 这些状态码通常表示瞬时故障，重试有较大概率成功。
     */
    public static final Option<String> RETRYABLE_STATUS_CODES =
            Options.key("retryable_status_codes")
                    .stringType()
                    .defaultValue("429,500,502,503,504")
                    .withDescription("触发重试的 HTTP 状态码列表，逗号分隔")
                    .withSemanticType("RETRYABLE_STATUS_CODES")
                    .withScope(ConnectorOptionScope.RUNTIME);

    /**
     * 重试抖动因子上限（毫秒）。
     *
     * <p>在指数退避等待时间上叠加随机抖动，避免多个并发请求同时重试
     * 造成惊群效应。设为 0 则禁用抖动。
     */
    public static final Option<Integer> RETRY_JITTER_MS =
            Options.key("retry_jitter_ms")
                    .intType()
                    .defaultValue(100)
                    .withDescription("重试抖动因子上限（毫秒），0 表示禁用抖动")
                    .withSemanticType("RETRY_JITTER")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(12000)
                    .withDescription("连接超时时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Socket 读取超时时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.TASK);

    // ── 连接池 ──────────────────────────────────────────

    /**
     * 连接池最大空闲连接数。
     *
     * <p>OkHttp 默认值为 5。对于高并发分页抓取场景，适当增大
     * 可以减少连接建立开销。
     */
    public static final Option<Integer> POOL_MAX_IDLE_CONNECTIONS =
            Options.key("pool.max_idle_connections")
                    .intType()
                    .defaultValue(8)
                    .withDescription("连接池最大空闲连接数")
                    .withSemanticType("POOL_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    /**
     * 连接池空闲连接保活时长（毫秒）。
     *
     * <p>超过该时长的空闲连接将被回收。默认 5 分钟。
     */
    public static final Option<Long> POOL_KEEP_ALIVE_DURATION_MS =
            Options.key("pool.keep_alive_duration_ms")
                    .longType()
                    .defaultValue(300000L)
                    .withDescription("连接池空闲连接保活时长（毫秒），默认 300000（5 分钟）")
                    .withSemanticType("POOL_KEEP_ALIVE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    // ── 其他 ──────────────────────────────────────────

    /**
     * 当 format=text 时，是否将多行文本拆分为多行数据。
     */
    public static final Option<Boolean> ENABLE_MULTI_LINES =
            Options.key("enable_multi_lines")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("text 格式下是否将多行文本拆分为多行数据")
                    .withSemanticType("MULTI_LINES")
                    .withScope(ConnectorOptionScope.TASK);

    /**
     * 当 JSON 字段缺失时，是否返回 null 而非报错。
     */
    public static final Option<Boolean> JSON_FIELD_MISSED_RETURN_NULL =
            Options.key("json_field_missed_return_null")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("JSON 字段缺失时是否返回 null，否则报错")
                    .withSemanticType("MISS_FIELD_POLICY")
                    .withScope(ConnectorOptionScope.TASK);
}
