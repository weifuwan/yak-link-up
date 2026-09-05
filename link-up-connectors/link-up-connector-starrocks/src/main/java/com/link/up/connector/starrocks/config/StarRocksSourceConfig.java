package com.link.up.connector.starrocks.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.connector.starrocks.schema.StarRocksSchemaParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable runtime configuration for StarRocks Native Source. */
public final class StarRocksSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> nodeUrls;
    private final String username;
    private final String password;
    private final String database;
    private final List<StarRocksSourceTableConfig> tableConfigs;
    private final int requestTabletSize;
    private final int connectTimeoutMs;
    private final int queryTimeoutSec;
    private final int keepAliveMin;
    private final int batchRows;
    private final long memLimit;
    private final int maxRetries;
    private final Map<String, String> scanParams;

    private StarRocksSourceConfig(
            List<String> nodeUrls,
            String username,
            String password,
            String database,
            List<StarRocksSourceTableConfig> tableConfigs,
            int requestTabletSize,
            int connectTimeoutMs,
            int queryTimeoutSec,
            int keepAliveMin,
            int batchRows,
            long memLimit,
            int maxRetries,
            Map<String, String> scanParams) {
        this.nodeUrls = Collections.unmodifiableList(new ArrayList<String>(nodeUrls));
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableConfigs =
                Collections.unmodifiableList(new ArrayList<StarRocksSourceTableConfig>(tableConfigs));
        this.requestTabletSize = requestTabletSize;
        this.connectTimeoutMs = connectTimeoutMs;
        this.queryTimeoutSec = queryTimeoutSec;
        this.keepAliveMin = keepAliveMin;
        this.batchRows = batchRows;
        this.memLimit = memLimit;
        this.maxRetries = maxRetries;
        this.scanParams =
                Collections.unmodifiableMap(new LinkedHashMap<String, String>(scanParams));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static StarRocksSourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        List<String> nodeUrls = normalizeNodes(config.get(StarRocksSourceOptions.NODE_URLS));
        String username = requireText(config.get(StarRocksSourceOptions.USERNAME), "username");
        String password = config.get(StarRocksSourceOptions.PASSWORD);
        String database = requireText(config.get(StarRocksSourceOptions.DATABASE), "database");
        String commonFilter = config.get(StarRocksSourceOptions.SCAN_FILTER);

        String singleTable = config.getOptional(StarRocksSourceOptions.TABLE).orElse(null);
        List<Map> tableList =
                config.getOptional(StarRocksSourceOptions.TABLE_LIST)
                        .orElse(Collections.<Map>emptyList());

        if (hasText(singleTable) == !tableList.isEmpty()) {
            throw new IllegalArgumentException(
                    "Exactly one of StarRocks source table or table_list must be configured");
        }

        List<StarRocksSourceTableConfig> tables = new ArrayList<StarRocksSourceTableConfig>();
        if (hasText(singleTable)) {
            Map<String, Object> schemaFields =
                    config.getOptional(StarRocksSourceOptions.SCHEMA_FIELDS)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "schema.fields is required for StarRocks Native Source"));
            tables.add(
                    new StarRocksSourceTableConfig(
                            database,
                            singleTable,
                            commonFilter,
                            StarRocksSchemaParser.parse(schemaFields)));
        } else {
            for (Map item : tableList) {
                String table = requireText(stringValue(item.get("table")), "table_list[].table");
                Map<String, Object> schemaFields = extractSchemaFields(item);
                String filter = stringValue(item.get("scan_filter"));
                if (!hasText(filter)) {
                    filter = commonFilter;
                }
                tables.add(
                        new StarRocksSourceTableConfig(
                                database,
                                table,
                                filter,
                                StarRocksSchemaParser.parse(schemaFields)));
            }
        }

        int requestTabletSize = config.get(StarRocksSourceOptions.REQUEST_TABLET_SIZE);
        int connectTimeoutMs = config.get(StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT_MS);
        int queryTimeoutSec = config.get(StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC);
        int keepAliveMin = config.get(StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN);
        int batchRows = config.get(StarRocksSourceOptions.SCAN_BATCH_ROWS);
        long memLimit = config.get(StarRocksSourceOptions.SCAN_MEM_LIMIT);
        int maxRetries = config.get(StarRocksSourceOptions.MAX_RETRIES);

        if (requestTabletSize <= 0) {
            throw new IllegalArgumentException("request_tablet_size must be greater than 0");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("scan_connect_timeout_ms must be greater than 0");
        }
        if (queryTimeoutSec == 0 || queryTimeoutSec < -1) {
            throw new IllegalArgumentException("scan_query_timeout_sec must be -1 or greater than 0");
        }
        if (keepAliveMin <= 0) {
            throw new IllegalArgumentException("scan_keep_alive_min must be greater than 0");
        }
        if (batchRows <= 0) {
            throw new IllegalArgumentException("scan_batch_rows must be greater than 0");
        }
        if (memLimit <= 0) {
            throw new IllegalArgumentException("scan_mem_limit must be greater than 0");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("max_retries must not be negative");
        }

        Map<String, String> scanParams =
                config.getOptional(StarRocksSourceOptions.SCAN_PARAMS)
                        .orElse(Collections.<String, String>emptyMap());

        return new StarRocksSourceConfig(
                nodeUrls,
                username,
                password == null ? "" : password,
                database,
                tables,
                requestTabletSize,
                connectTimeoutMs,
                queryTimeoutSec,
                keepAliveMin,
                batchRows,
                memLimit,
                maxRetries,
                scanParams);
    }

    private static List<String> normalizeNodes(List<String> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("node_urls must contain at least one FE address");
        }
        List<String> result = new ArrayList<String>();
        for (String value : values) {
            String node = requireText(value, "node_urls item");
            while (node.endsWith("/")) {
                node = node.substring(0, node.length() - 1);
            }
            result.add(node);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractSchemaFields(Map<?, ?> item) {
        Object direct = item.get("schema.fields");
        if (direct instanceof Map) {
            return (Map<String, Object>) direct;
        }
        Object schema = item.get("schema");
        if (schema instanceof Map) {
            Object fields = ((Map<?, ?>) schema).get("fields");
            if (fields instanceof Map) {
                return (Map<String, Object>) fields;
            }
        }
        throw new IllegalArgumentException(
                "table_list[].schema.fields is required for StarRocks Native Source");
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String requireText(String value, String name) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }

    public List<String> getNodeUrls() {
        return nodeUrls;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public List<StarRocksSourceTableConfig> getTableConfigs() {
        return tableConfigs;
    }

    public List<CatalogTable> getCatalogTables() {
        List<CatalogTable> result = new ArrayList<CatalogTable>(tableConfigs.size());
        for (StarRocksSourceTableConfig tableConfig : tableConfigs) {
            result.add(tableConfig.getCatalogTable());
        }
        return Collections.unmodifiableList(result);
    }

    public int getRequestTabletSize() {
        return requestTabletSize;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getQueryTimeoutSec() {
        return queryTimeoutSec;
    }

    public int getKeepAliveMin() {
        return keepAliveMin;
    }

    public int getBatchRows() {
        return batchRows;
    }

    public long getMemLimit() {
        return memLimit;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Map<String, String> getScanParams() {
        return scanParams;
    }
}
