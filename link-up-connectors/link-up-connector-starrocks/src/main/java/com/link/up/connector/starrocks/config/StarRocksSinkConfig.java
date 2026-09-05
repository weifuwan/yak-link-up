package com.link.up.connector.starrocks.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Parsed immutable configuration for the StarRocks bounded Stream Load Sink. */
public final class StarRocksSinkConfig {

    private static final long MAX_IN_MEMORY_PAYLOAD_BYTES = Integer.MAX_VALUE - 4096L;
    private static final Pattern LABEL_PREFIX_PATTERN = Pattern.compile("[A-Za-z0-9_-]+_");
    private static final Set<String> RESERVED_STREAM_LOAD_HEADERS;

    static {
        Set<String> reserved = new LinkedHashSet<String>();
        reserved.add("authorization");
        reserved.add("label");
        reserved.add("format");
        reserved.add("expect");
        reserved.add("content-type");
        reserved.add("strip_outer_array");
        reserved.add("read_json_by_line");
        reserved.add("column_separator");
        reserved.add("row_delimiter");
        reserved.add("columns");
        RESERVED_STREAM_LOAD_HEADERS = Collections.unmodifiableSet(reserved);
    }

    private final List<String> nodeUrls;
    private final String username;
    private final String password;
    private final String database;
    private final String table;
    private final String labelPrefix;
    private final StarRocksLoadFormat loadFormat;
    private final int batchMaxRows;
    private final long batchMaxBytes;
    private final int maxRetries;
    private final int retryBackoffMs;
    private final int maxRetryBackoffMs;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final int labelStateTimeoutMs;
    private final int labelStatePollMs;
    private final String columnSeparator;
    private final String rowDelimiter;
    private final Map<String, String> streamLoadParams;

    private StarRocksSinkConfig(
            List<String> nodeUrls,
            String username,
            String password,
            String database,
            String table,
            String labelPrefix,
            StarRocksLoadFormat loadFormat,
            int batchMaxRows,
            long batchMaxBytes,
            int maxRetries,
            int retryBackoffMs,
            int maxRetryBackoffMs,
            int connectTimeoutMs,
            int socketTimeoutMs,
            int labelStateTimeoutMs,
            int labelStatePollMs,
            String columnSeparator,
            String rowDelimiter,
            Map<String, String> streamLoadParams) {
        this.nodeUrls = nodeUrls;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
        this.labelPrefix = labelPrefix;
        this.loadFormat = loadFormat;
        this.batchMaxRows = batchMaxRows;
        this.batchMaxBytes = batchMaxBytes;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.maxRetryBackoffMs = maxRetryBackoffMs;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        this.labelStateTimeoutMs = labelStateTimeoutMs;
        this.labelStatePollMs = labelStatePollMs;
        this.columnSeparator = columnSeparator;
        this.rowDelimiter = rowDelimiter;
        this.streamLoadParams = streamLoadParams;
    }

    public static StarRocksSinkConfig of(ReadonlyConfig options) {
        Objects.requireNonNull(options, "options must not be null");

        List<String> nodeUrls = normalizeNodes(options.get(StarRocksSinkOptions.NODE_URLS));
        String username = requireText(options.get(StarRocksSinkOptions.USERNAME), "username");
        String password = options.get(StarRocksSinkOptions.PASSWORD);
        String database = requireText(options.get(StarRocksSinkOptions.DATABASE), "database");
        String table = requireText(options.get(StarRocksSinkOptions.TABLE), "table");

        StarRocksLoadFormat loadFormat = options.get(StarRocksSinkOptions.LOAD_FORMAT);
        if (loadFormat == null) {
            loadFormat = StarRocksLoadFormat.JSON;
        }

        int batchMaxRows = positive(options.get(StarRocksSinkOptions.BATCH_MAX_ROWS), "batch_max_rows");
        long batchMaxBytes = positive(options.get(StarRocksSinkOptions.BATCH_MAX_BYTES), "batch_max_bytes");
        if (batchMaxBytes > MAX_IN_MEMORY_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "StarRocks Sink batch_max_bytes must be <= " + MAX_IN_MEMORY_PAYLOAD_BYTES);
        }

        int maxRetries = nonNegative(options.get(StarRocksSinkOptions.MAX_RETRIES), "max_retries");
        int retryBackoffMs = nonNegative(options.get(StarRocksSinkOptions.RETRY_BACKOFF_MS), "retry_backoff_ms");
        int maxRetryBackoffMs = nonNegative(
                options.get(StarRocksSinkOptions.MAX_RETRY_BACKOFF_MS),
                "max_retry_backoff_ms");
        if (maxRetryBackoffMs < retryBackoffMs) {
            throw new IllegalArgumentException(
                    "StarRocks Sink max_retry_backoff_ms must be >= retry_backoff_ms");
        }

        int connectTimeoutMs = positive(
                options.get(StarRocksSinkOptions.CONNECT_TIMEOUT_MS),
                "connect_timeout_ms");
        int socketTimeoutMs = positive(
                options.get(StarRocksSinkOptions.SOCKET_TIMEOUT_MS),
                "socket_timeout_ms");
        int labelStateTimeoutMs = positive(
                options.get(StarRocksSinkOptions.LABEL_STATE_TIMEOUT_MS),
                "label_state_timeout_ms");
        int labelStatePollMs = positive(
                options.get(StarRocksSinkOptions.LABEL_STATE_POLL_MS),
                "label_state_poll_ms");
        if (labelStatePollMs > labelStateTimeoutMs) {
            throw new IllegalArgumentException(
                    "StarRocks Sink label_state_poll_ms must be <= label_state_timeout_ms");
        }

        String columnSeparator = requireDelimiter(
                options.get(StarRocksSinkOptions.COLUMN_SEPARATOR),
                "column_separator");
        String rowDelimiter = requireDelimiter(
                options.get(StarRocksSinkOptions.ROW_DELIMITER),
                "row_delimiter");

        Map<String, String> streamLoadParams = immutableParams(
                options.get(StarRocksSinkOptions.STREAM_LOAD_PARAMS));
        validateStreamLoadParams(streamLoadParams);

        String configuredPrefix = options.getOptional(StarRocksSinkOptions.LABEL_PREFIX).orElse(null);
        String labelPrefix = configuredPrefix == null || configuredPrefix.trim().isEmpty()
                ? defaultLabelPrefix(database, table)
                : normalizeLabelPrefix(configuredPrefix.trim());

        return new StarRocksSinkConfig(
                nodeUrls,
                username,
                password == null ? "" : password,
                database,
                table,
                labelPrefix,
                loadFormat,
                batchMaxRows,
                batchMaxBytes,
                maxRetries,
                retryBackoffMs,
                maxRetryBackoffMs,
                connectTimeoutMs,
                socketTimeoutMs,
                labelStateTimeoutMs,
                labelStatePollMs,
                columnSeparator,
                rowDelimiter,
                streamLoadParams);
    }

    private static List<String> normalizeNodes(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("StarRocks Sink node_urls must not be empty");
        }
        LinkedHashSet<String> normalized = new LinkedHashSet<String>();
        for (String node : nodes) {
            String value = requireText(node, "node_urls item");
            while (value.endsWith("/")) {
                value = value.substring(0, value.length() - 1);
            }
            normalized.add(value);
        }
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("StarRocks Sink node_urls must not be empty");
        }
        return Collections.unmodifiableList(new ArrayList<String>(normalized));
    }

    private static Map<String, String> immutableParams(Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> copy = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = requireText(entry.getKey(), "stream_load.params key");
            String value = entry.getValue();
            if (value == null) {
                throw new IllegalArgumentException(
                        "StarRocks Sink stream_load.params value must not be null: " + key);
            }
            copy.put(key, value);
        }
        return Collections.unmodifiableMap(copy);
    }

    private static void validateStreamLoadParams(Map<String, String> params) {
        for (String key : params.keySet()) {
            if (RESERVED_STREAM_LOAD_HEADERS.contains(key.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "StarRocks Sink stream_load.params must not override connector-owned header: "
                                + key);
            }
        }
    }

    private static String defaultLabelPrefix(String database, String table) {
        return normalizeLabelPrefix("link_up_" + sanitizeLabelPart(database) + "_" + sanitizeLabelPart(table) + "_");
    }

    private static String sanitizeLabelPart(String value) {
        return value.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private static String normalizeLabelPrefix(String value) {
        String prefix = value.endsWith("_") ? value : value + "_";
        if (!LABEL_PREFIX_PATTERN.matcher(prefix).matches()) {
            throw new IllegalArgumentException(
                    "StarRocks Sink label_prefix may contain only letters, digits, '_' and '-'");
        }
        return prefix;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("StarRocks Sink '" + name + "' must not be blank");
        }
        return value.trim();
    }

    private static String requireDelimiter(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("StarRocks Sink '" + name + "' must not be empty");
        }
        return value;
    }

    private static int positive(Integer value, String name) {
        if (value == null || value.intValue() <= 0) {
            throw new IllegalArgumentException("StarRocks Sink '" + name + "' must be > 0");
        }
        return value.intValue();
    }

    private static long positive(Long value, String name) {
        if (value == null || value.longValue() <= 0L) {
            throw new IllegalArgumentException("StarRocks Sink '" + name + "' must be > 0");
        }
        return value.longValue();
    }

    private static int nonNegative(Integer value, String name) {
        if (value == null || value.intValue() < 0) {
            throw new IllegalArgumentException("StarRocks Sink '" + name + "' must be >= 0");
        }
        return value.intValue();
    }

    public List<String> getNodeUrls() { return nodeUrls; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public String getLabelPrefix() { return labelPrefix; }
    public StarRocksLoadFormat getLoadFormat() { return loadFormat; }
    public int getBatchMaxRows() { return batchMaxRows; }
    public long getBatchMaxBytes() { return batchMaxBytes; }
    public int getMaxRetries() { return maxRetries; }
    public int getRetryBackoffMs() { return retryBackoffMs; }
    public int getMaxRetryBackoffMs() { return maxRetryBackoffMs; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getSocketTimeoutMs() { return socketTimeoutMs; }
    public int getLabelStateTimeoutMs() { return labelStateTimeoutMs; }
    public int getLabelStatePollMs() { return labelStatePollMs; }
    public String getColumnSeparator() { return columnSeparator; }
    public String getRowDelimiter() { return rowDelimiter; }
    public Map<String, String> getStreamLoadParams() { return streamLoadParams; }
}
