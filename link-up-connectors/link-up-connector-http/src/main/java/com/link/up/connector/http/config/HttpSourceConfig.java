package com.link.up.connector.http.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * HTTP Source 解析后的不可变配置。
 */
public final class HttpSourceConfig {

    private final String url;
    private final HttpMethod method;
    private final Map<String, String> headers;
    private final Map<String, String> params;
    private final String body;
    private final HttpFormat format;
    private final Map<String, Object> schemaFields;
    private final String contentField;
    private final Map<String, Object> jsonField;

    // 分页
    private final String pageField;
    private final long totalPageSize;
    private final int pageBatchSize;
    private final int startPageNumber;
    private final PageType pageType;
    private final String cursorField;
    private final String cursorResponseField;
    private final boolean usePlaceholderReplacement;

    // 重试与超时
    private final int retry;
    private final int retryBackoffMultiplierMs;
    private final int retryBackoffMaxMs;
    private final Set<Integer> retryableStatusCodes;
    private final int retryJitterMs;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    // 连接池
    private final int poolMaxIdleConnections;
    private final long poolKeepAliveDurationMs;

    // 其他
    private final boolean enableMultiLines;
    private final boolean jsonFieldMissedReturnNull;

    private HttpSourceConfig(Builder builder) {
        this.url = Objects.requireNonNull(builder.url, "url must not be null");
        this.method = builder.method;
        this.headers = builder.headers;
        this.params = builder.params;
        this.body = builder.body;
        this.format = builder.format;
        this.schemaFields = builder.schemaFields;
        this.contentField = builder.contentField;
        this.jsonField = builder.jsonField;
        this.pageField = builder.pageField;
        this.totalPageSize = builder.totalPageSize;
        this.pageBatchSize = builder.pageBatchSize;
        this.startPageNumber = builder.startPageNumber;
        this.pageType = builder.pageType;
        this.cursorField = builder.cursorField;
        this.cursorResponseField = builder.cursorResponseField;
        this.usePlaceholderReplacement = builder.usePlaceholderReplacement;
        this.retry = builder.retry;
        this.retryBackoffMultiplierMs = builder.retryBackoffMultiplierMs;
        this.retryBackoffMaxMs = builder.retryBackoffMaxMs;
        this.retryableStatusCodes = builder.retryableStatusCodes;
        this.retryJitterMs = builder.retryJitterMs;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.socketTimeoutMs = builder.socketTimeoutMs;
        this.poolMaxIdleConnections = builder.poolMaxIdleConnections;
        this.poolKeepAliveDurationMs = builder.poolKeepAliveDurationMs;
        this.enableMultiLines = builder.enableMultiLines;
        this.jsonFieldMissedReturnNull = builder.jsonFieldMissedReturnNull;

        validatePagination();
    }

    /**
     * 校验分页配置一致性。
     */
    private void validatePagination() {
        if (!hasPagination()) {
            return;
        }

        if (pageType == PageType.CURSOR) {
            if (cursorField == null || cursorField.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Cursor 分页模式必须配置 'pageing.cursor_field'");
            }
            if (cursorResponseField == null || cursorResponseField.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Cursor 分页模式必须配置 'pageing.cursor_response_field'");
            }
        }
    }

    public static HttpSourceConfig of(ReadonlyConfig options) {
        Objects.requireNonNull(options, "options must not be null");

        String url = options.get(HttpSourceOptions.URL);
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalArgumentException("HTTP Source url must not be blank");
        }

        return new Builder()
                .url(url.trim())
                .method(options.get(HttpSourceOptions.METHOD))
                .headers(copyMap(options.get(HttpSourceOptions.HEADERS)))
                .params(copyMap(options.get(HttpSourceOptions.PARAMS)))
                .body(options.get(HttpSourceOptions.BODY))
                .format(options.get(HttpSourceOptions.FORMAT))
                .schemaFields(copyMap(options.get(HttpSourceOptions.SCHEMA_FIELDS)))
                .contentField(options.get(HttpSourceOptions.CONTENT_FIELD))
                .jsonField(copyMap(options.get(HttpSourceOptions.JSON_FIELD)))
                .pageField(options.get(HttpSourceOptions.PAGE_FIELD))
                .totalPageSize(options.get(HttpSourceOptions.TOTAL_PAGE_SIZE))
                .pageBatchSize(options.get(HttpSourceOptions.PAGE_BATCH_SIZE))
                .startPageNumber(options.get(HttpSourceOptions.START_PAGE_NUMBER))
                .pageType(options.get(HttpSourceOptions.PAGE_TYPE))
                .cursorField(options.get(HttpSourceOptions.CURSOR_FIELD))
                .cursorResponseField(options.get(HttpSourceOptions.CURSOR_RESPONSE_FIELD))
                .usePlaceholderReplacement(options.get(HttpSourceOptions.USE_PLACEHOLDER_REPLACEMENT))
                .retry(options.get(HttpSourceOptions.RETRY))
                .retryBackoffMultiplierMs(options.get(HttpSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS))
                .retryBackoffMaxMs(options.get(HttpSourceOptions.RETRY_BACKOFF_MAX_MS))
                .retryableStatusCodes(parseRetryableStatusCodes(options.get(HttpSourceOptions.RETRYABLE_STATUS_CODES)))
                .retryJitterMs(options.get(HttpSourceOptions.RETRY_JITTER_MS))
                .connectTimeoutMs(options.get(HttpSourceOptions.CONNECT_TIMEOUT_MS))
                .socketTimeoutMs(options.get(HttpSourceOptions.SOCKET_TIMEOUT_MS))
                .poolMaxIdleConnections(options.get(HttpSourceOptions.POOL_MAX_IDLE_CONNECTIONS))
                .poolKeepAliveDurationMs(options.get(HttpSourceOptions.POOL_KEEP_ALIVE_DURATION_MS))
                .enableMultiLines(options.get(HttpSourceOptions.ENABLE_MULTI_LINES))
                .jsonFieldMissedReturnNull(options.get(HttpSourceOptions.JSON_FIELD_MISSED_RETURN_NULL))
                .build();
    }

    private static <K, V> Map<K, V> copyMap(Map<K, V> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    private static Set<Integer> parseRetryableStatusCodes(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptySet();
        }
        Set<Integer> codes = new TreeSet<>();
        for (String token : csv.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                codes.add(Integer.parseInt(trimmed));
            }
        }
        return Collections.unmodifiableSet(codes);
    }

    public boolean hasPagination() {
        return pageField != null && !pageField.isEmpty();
    }

    // ── Getters ──────────────────────────────────────────

    public String getUrl() { return url; }
    public HttpMethod getMethod() { return method; }
    public Map<String, String> getHeaders() { return headers; }
    public Map<String, String> getParams() { return params; }
    public String getBody() { return body; }
    public HttpFormat getFormat() { return format; }
    public Map<String, Object> getSchemaFields() { return schemaFields; }
    public String getContentField() { return contentField; }
    public Map<String, Object> getJsonField() { return jsonField; }
    public String getPageField() { return pageField; }
    public long getTotalPageSize() { return totalPageSize; }
    public int getPageBatchSize() { return pageBatchSize; }
    public int getStartPageNumber() { return startPageNumber; }
    public PageType getPageType() { return pageType; }
    public String getCursorField() { return cursorField; }
    public String getCursorResponseField() { return cursorResponseField; }
    public boolean isUsePlaceholderReplacement() { return usePlaceholderReplacement; }
    public int getRetry() { return retry; }
    public int getRetryBackoffMultiplierMs() { return retryBackoffMultiplierMs; }
    public int getRetryBackoffMaxMs() { return retryBackoffMaxMs; }
    public Set<Integer> getRetryableStatusCodes() { return retryableStatusCodes; }
    public int getRetryJitterMs() { return retryJitterMs; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getSocketTimeoutMs() { return socketTimeoutMs; }
    public int getPoolMaxIdleConnections() { return poolMaxIdleConnections; }
    public long getPoolKeepAliveDurationMs() { return poolKeepAliveDurationMs; }
    public boolean isEnableMultiLines() { return enableMultiLines; }
    public boolean isJsonFieldMissedReturnNull() { return jsonFieldMissedReturnNull; }

    public static final class Builder {
        private String url;
        private HttpMethod method = HttpMethod.GET;
        private Map<String, String> headers = Collections.emptyMap();
        private Map<String, String> params = Collections.emptyMap();
        private String body;
        private HttpFormat format = HttpFormat.JSON;
        private Map<String, Object> schemaFields = Collections.emptyMap();
        private String contentField;
        private Map<String, Object> jsonField = Collections.emptyMap();
        private String pageField;
        private long totalPageSize = 0;
        private int pageBatchSize = 100;
        private int startPageNumber = 1;
        private PageType pageType = PageType.PAGE_NUMBER;
        private String cursorField;
        private String cursorResponseField;
        private boolean usePlaceholderReplacement = false;
        private int retry = 3;
        private int retryBackoffMultiplierMs = 100;
        private int retryBackoffMaxMs = 10000;
        private Set<Integer> retryableStatusCodes = new TreeSet<>();
        private int retryJitterMs = 100;
        private int connectTimeoutMs = 12000;
        private int socketTimeoutMs = 60000;
        private int poolMaxIdleConnections = 8;
        private long poolKeepAliveDurationMs = 300000L;
        private boolean enableMultiLines = false;
        private boolean jsonFieldMissedReturnNull = false;

        public Builder url(String url) { this.url = url; return this; }
        public Builder method(HttpMethod method) { this.method = method; return this; }
        public Builder headers(Map<String, String> headers) { this.headers = headers; return this; }
        public Builder params(Map<String, String> params) { this.params = params; return this; }
        public Builder body(String body) { this.body = body; return this; }
        public Builder format(HttpFormat format) { this.format = format; return this; }
        public Builder schemaFields(Map<String, Object> schemaFields) { this.schemaFields = schemaFields; return this; }
        public Builder contentField(String contentField) { this.contentField = contentField; return this; }
        public Builder jsonField(Map<String, Object> jsonField) { this.jsonField = jsonField; return this; }
        public Builder pageField(String pageField) { this.pageField = pageField; return this; }
        public Builder totalPageSize(long totalPageSize) { this.totalPageSize = totalPageSize; return this; }
        public Builder pageBatchSize(int pageBatchSize) { this.pageBatchSize = pageBatchSize; return this; }
        public Builder startPageNumber(int startPageNumber) { this.startPageNumber = startPageNumber; return this; }
        public Builder pageType(PageType pageType) { this.pageType = pageType; return this; }
        public Builder cursorField(String cursorField) { this.cursorField = cursorField; return this; }
        public Builder cursorResponseField(String cursorResponseField) { this.cursorResponseField = cursorResponseField; return this; }
        public Builder usePlaceholderReplacement(boolean v) { this.usePlaceholderReplacement = v; return this; }
        public Builder retry(int retry) { this.retry = retry; return this; }
        public Builder retryBackoffMultiplierMs(int v) { this.retryBackoffMultiplierMs = v; return this; }
        public Builder retryBackoffMaxMs(int v) { this.retryBackoffMaxMs = v; return this; }
        public Builder retryableStatusCodes(Set<Integer> v) { this.retryableStatusCodes = v; return this; }
        public Builder retryJitterMs(int v) { this.retryJitterMs = v; return this; }
        public Builder connectTimeoutMs(int v) { this.connectTimeoutMs = v; return this; }
        public Builder socketTimeoutMs(int v) { this.socketTimeoutMs = v; return this; }
        public Builder poolMaxIdleConnections(int v) { this.poolMaxIdleConnections = v; return this; }
        public Builder poolKeepAliveDurationMs(long v) { this.poolKeepAliveDurationMs = v; return this; }
        public Builder enableMultiLines(boolean v) { this.enableMultiLines = v; return this; }
        public Builder jsonFieldMissedReturnNull(boolean v) { this.jsonFieldMissedReturnNull = v; return this; }

        public HttpSourceConfig build() {
            return new HttpSourceConfig(this);
        }
    }
}
