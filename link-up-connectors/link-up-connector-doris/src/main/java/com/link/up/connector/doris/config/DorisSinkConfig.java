package com.link.up.connector.doris.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Doris Sink 解析后的不可变配置。
 */
public final class DorisSinkConfig {

    private final String fenodes;
    private final String benodes;
    private final boolean directToBe;
    private final int queryPort;
    private final String username;
    private final String password;
    private final String database;
    private final String table;
    private final String sinkLabelPrefix;
    private final boolean enable2pc;
    private final boolean enableDelete;
    private final int checkIntervalMs;
    private final int maxRetries;
    private final int bufferSize;
    private final int bufferCount;
    private final int batchSize;
    private final DorisLoadFormat loadFormat;
    private final String csvColumnSeparator;
    private final Map<String, String> dorisConfig;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    // 建表配置
    private final String createTableDdl;
    private final String keyType;
    private final int buckets;

    // Stream Load 扩展参数
    private final int loadTimeoutSec;
    private final double maxFilterRatio;
    private final String columns;
    private final String where;
    private final String partitions;
    private final boolean strictMode;
    private final String timezone;
    private final long execMemLimit;
    private final String jsonpaths;
    private final boolean stripOuterArray;
    private final String jsonRoot;
    private final int sendBatchParallelism;
    private final boolean loadToSingleTablet;
    private final String lineDelimiter;
    private final String enclose;
    private final String escape;
    private final boolean numAsString;
    private final boolean fuzzyParse;
    private final String compressType;
    private final boolean trimDoubleQuotes;
    private final int skipLines;
    private final String loadComment;

    private DorisSinkConfig(Builder b) {
        this.fenodes = b.fenodes;
        this.benodes = b.benodes;
        this.directToBe = b.directToBe;
        this.queryPort = b.queryPort;
        this.username = b.username;
        this.password = b.password;
        this.database = b.database;
        this.table = b.table;
        this.sinkLabelPrefix = b.sinkLabelPrefix;
        this.enable2pc = b.enable2pc;
        this.enableDelete = b.enableDelete;
        this.checkIntervalMs = b.checkIntervalMs;
        this.maxRetries = b.maxRetries;
        this.bufferSize = b.bufferSize;
        this.bufferCount = b.bufferCount;
        this.batchSize = b.batchSize;
        this.loadFormat = b.loadFormat;
        this.csvColumnSeparator = b.csvColumnSeparator;
        this.dorisConfig = b.dorisConfig;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.socketTimeoutMs = b.socketTimeoutMs;
        this.createTableDdl = b.createTableDdl;
        this.keyType = b.keyType;
        this.buckets = b.buckets;
        this.loadTimeoutSec = b.loadTimeoutSec;
        this.maxFilterRatio = b.maxFilterRatio;
        this.columns = b.columns;
        this.where = b.where;
        this.partitions = b.partitions;
        this.strictMode = b.strictMode;
        this.timezone = b.timezone;
        this.execMemLimit = b.execMemLimit;
        this.jsonpaths = b.jsonpaths;
        this.stripOuterArray = b.stripOuterArray;
        this.jsonRoot = b.jsonRoot;
        this.sendBatchParallelism = b.sendBatchParallelism;
        this.loadToSingleTablet = b.loadToSingleTablet;
        this.lineDelimiter = b.lineDelimiter;
        this.enclose = b.enclose;
        this.escape = b.escape;
        this.numAsString = b.numAsString;
        this.fuzzyParse = b.fuzzyParse;
        this.compressType = b.compressType;
        this.trimDoubleQuotes = b.trimDoubleQuotes;
        this.skipLines = b.skipLines;
        this.loadComment = b.loadComment;
    }

    public static DorisSinkConfig of(ReadonlyConfig options) {
        Objects.requireNonNull(options, "options must not be null");

        String fenodes = options.get(DorisSinkOptions.FENODES);
        if (fenodes == null || fenodes.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'fenodes' must not be blank");
        }
        String username = options.get(DorisSinkOptions.USERNAME);
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'username' must not be blank");
        }
        String database = options.get(DorisSinkOptions.DATABASE);
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'database' must not be blank");
        }
        String table = options.get(DorisSinkOptions.TABLE);
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'table' must not be blank");
        }
        String labelPrefix = options.get(DorisSinkOptions.SINK_LABEL_PREFIX);
        if (labelPrefix == null || labelPrefix.trim().isEmpty()) {
            labelPrefix = "link_up_" + database + "_" + table;
        }

        return new Builder()
                .fenodes(fenodes.trim())
                .benodes(options.get(DorisSinkOptions.BENODES))
                .directToBe(options.get(DorisSinkOptions.DIRECT_TO_BE))
                .queryPort(options.get(DorisSinkOptions.QUERY_PORT))
                .username(username.trim())
                .password(options.get(DorisSinkOptions.PASSWORD))
                .database(database.trim())
                .table(table.trim())
                .sinkLabelPrefix(labelPrefix.trim())
                .enable2pc(options.get(DorisSinkOptions.SINK_ENABLE_2PC))
                .enableDelete(options.get(DorisSinkOptions.SINK_ENABLE_DELETE))
                .checkIntervalMs(options.get(DorisSinkOptions.SINK_CHECK_INTERVAL_MS))
                .maxRetries(options.get(DorisSinkOptions.SINK_MAX_RETRIES))
                .bufferSize(options.get(DorisSinkOptions.SINK_BUFFER_SIZE))
                .bufferCount(options.get(DorisSinkOptions.SINK_BUFFER_COUNT))
                .batchSize(options.get(DorisSinkOptions.DORIS_BATCH_SIZE))
                .loadFormat(options.get(DorisSinkOptions.LOAD_FORMAT))
                .csvColumnSeparator(options.get(DorisSinkOptions.CSV_COLUMN_SEPARATOR))
                .dorisConfig(copyMap(options.get(DorisSinkOptions.DORIS_CONFIG)))
                .connectTimeoutMs(options.get(DorisSinkOptions.CONNECT_TIMEOUT_MS))
                .socketTimeoutMs(options.get(DorisSinkOptions.SOCKET_TIMEOUT_MS))
                .createTableDdl(options.get(DorisSinkOptions.SINK_CREATE_TABLE_DDL))
                .keyType(options.get(DorisSinkOptions.SINK_KEY_TYPE))
                .buckets(options.get(DorisSinkOptions.SINK_BUCKETS))
                .loadTimeoutSec(options.get(DorisSinkOptions.SINK_LOAD_TIMEOUT_SEC))
                .maxFilterRatio(options.get(DorisSinkOptions.SINK_MAX_FILTER_RATIO))
                .columns(options.get(DorisSinkOptions.SINK_COLUMNS))
                .where(options.get(DorisSinkOptions.SINK_WHERE))
                .partitions(options.get(DorisSinkOptions.SINK_PARTITIONS))
                .strictMode(options.get(DorisSinkOptions.SINK_STRICT_MODE))
                .timezone(options.get(DorisSinkOptions.SINK_TIMEZONE))
                .execMemLimit(options.get(DorisSinkOptions.SINK_EXEC_MEM_LIMIT))
                .jsonpaths(options.get(DorisSinkOptions.SINK_JSONPATHS))
                .stripOuterArray(options.get(DorisSinkOptions.SINK_STRIP_OUTER_ARRAY))
                .jsonRoot(options.get(DorisSinkOptions.SINK_JSON_ROOT))
                .sendBatchParallelism(options.get(DorisSinkOptions.SINK_SEND_BATCH_PARALLELISM))
                .loadToSingleTablet(options.get(DorisSinkOptions.SINK_LOAD_TO_SINGLE_TABLET))
                .lineDelimiter(options.get(DorisSinkOptions.SINK_LINE_DELIMITER))
                .enclose(options.get(DorisSinkOptions.SINK_ENCLOSE))
                .escape(options.get(DorisSinkOptions.SINK_ESCAPE))
                .numAsString(options.get(DorisSinkOptions.SINK_NUM_AS_STRING))
                .fuzzyParse(options.get(DorisSinkOptions.SINK_FUZZY_PARSE))
                .compressType(options.get(DorisSinkOptions.SINK_COMPRESS_TYPE))
                .trimDoubleQuotes(options.get(DorisSinkOptions.SINK_TRIM_DOUBLE_QUOTES))
                .skipLines(options.get(DorisSinkOptions.SINK_SKIP_LINES))
                .loadComment(options.get(DorisSinkOptions.SINK_LOAD_COMMENT))
                .build();
    }

    private static <K, V> Map<K, V> copyMap(Map<K, V> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    /**
     * 解析 FE 节点列表。
     */
    public List<String> getFeNodeList() {
        return parseNodes(fenodes);
    }

    /**
     * 解析 BE 节点列表。
     */
    public List<String> getBeNodeList() {
        if (benodes == null || benodes.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return parseNodes(benodes);
    }

    private static List<String> parseNodes(String nodes) {
        String[] parts = nodes.split(",");
        List<String> result = new java.util.ArrayList<>(parts.length);
        for (String p : parts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    // ── Getters ──────────────────────────────────────────

    public String getFenodes() { return fenodes; }
    public String getBenodes() { return benodes; }
    public boolean isDirectToBe() { return directToBe; }
    public int getQueryPort() { return queryPort; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public String getSinkLabelPrefix() { return sinkLabelPrefix; }
    public boolean isEnable2pc() { return enable2pc; }
    public boolean isEnableDelete() { return enableDelete; }
    public int getCheckIntervalMs() { return checkIntervalMs; }
    public int getMaxRetries() { return maxRetries; }
    public int getBufferSize() { return bufferSize; }
    public int getBufferCount() { return bufferCount; }
    public int getBatchSize() { return batchSize; }
    public DorisLoadFormat getLoadFormat() { return loadFormat; }
    public String getCsvColumnSeparator() { return csvColumnSeparator; }
    public Map<String, String> getDorisConfig() { return dorisConfig; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getSocketTimeoutMs() { return socketTimeoutMs; }

    // 建表配置
    public String getCreateTableDdl() { return createTableDdl; }
    public String getKeyType() { return keyType; }
    public int getBuckets() { return buckets; }

    // Stream Load 扩展参数
    public int getLoadTimeoutSec() { return loadTimeoutSec; }
    public double getMaxFilterRatio() { return maxFilterRatio; }
    public String getColumns() { return columns; }
    public String getWhere() { return where; }
    public String getPartitions() { return partitions; }
    public boolean isStrictMode() { return strictMode; }
    public String getTimezone() { return timezone; }
    public long getExecMemLimit() { return execMemLimit; }
    public String getJsonpaths() { return jsonpaths; }
    public boolean isStripOuterArray() { return stripOuterArray; }
    public String getJsonRoot() { return jsonRoot; }
    public int getSendBatchParallelism() { return sendBatchParallelism; }
    public boolean isLoadToSingleTablet() { return loadToSingleTablet; }
    public String getLineDelimiter() { return lineDelimiter; }
    public String getEnclose() { return enclose; }
    public String getEscape() { return escape; }
    public boolean isNumAsString() { return numAsString; }
    public boolean isFuzzyParse() { return fuzzyParse; }
    public String getCompressType() { return compressType; }
    public boolean isTrimDoubleQuotes() { return trimDoubleQuotes; }
    public int getSkipLines() { return skipLines; }
    public String getLoadComment() { return loadComment; }

    public static final class Builder {
        private String fenodes;
        private String benodes;
        private boolean directToBe = false;
        private int queryPort = 9030;
        private String username;
        private String password;
        private String database;
        private String table;
        private String sinkLabelPrefix;
        private boolean enable2pc = false;
        private boolean enableDelete = false;
        private int checkIntervalMs = 10000;
        private int maxRetries = 3;
        private int bufferSize = 262144;
        private int bufferCount = 3;
        private int batchSize = 1024;
        private DorisLoadFormat loadFormat = DorisLoadFormat.JSON;
        private String csvColumnSeparator = ",";
        private Map<String, String> dorisConfig = Collections.emptyMap();
        private int connectTimeoutMs = 30000;
        private int socketTimeoutMs = 300000;
        private String createTableDdl;
        private String keyType = "DUPLICATE";
        private int buckets = 10;
        private int loadTimeoutSec = 600;
        private double maxFilterRatio = 0.0;
        private String columns;
        private String where;
        private String partitions;
        private boolean strictMode = false;
        private String timezone;
        private long execMemLimit = 2147483648L;
        private String jsonpaths;
        private boolean stripOuterArray = false;
        private String jsonRoot;
        private int sendBatchParallelism = 1;
        private boolean loadToSingleTablet = false;
        private String lineDelimiter = "\n";
        private String enclose;
        private String escape;
        private boolean numAsString = false;
        private boolean fuzzyParse = false;
        private String compressType;
        private boolean trimDoubleQuotes = false;
        private int skipLines = 0;
        private String loadComment;

        public Builder fenodes(String v) { this.fenodes = v; return this; }
        public Builder benodes(String v) { this.benodes = v; return this; }
        public Builder directToBe(boolean v) { this.directToBe = v; return this; }
        public Builder queryPort(int v) { this.queryPort = v; return this; }
        public Builder username(String v) { this.username = v; return this; }
        public Builder password(String v) { this.password = v; return this; }
        public Builder database(String v) { this.database = v; return this; }
        public Builder table(String v) { this.table = v; return this; }
        public Builder sinkLabelPrefix(String v) { this.sinkLabelPrefix = v; return this; }
        public Builder enable2pc(boolean v) { this.enable2pc = v; return this; }
        public Builder enableDelete(boolean v) { this.enableDelete = v; return this; }
        public Builder checkIntervalMs(int v) { this.checkIntervalMs = v; return this; }
        public Builder maxRetries(int v) { this.maxRetries = v; return this; }
        public Builder bufferSize(int v) { this.bufferSize = v; return this; }
        public Builder bufferCount(int v) { this.bufferCount = v; return this; }
        public Builder batchSize(int v) { this.batchSize = v; return this; }
        public Builder loadFormat(DorisLoadFormat v) { this.loadFormat = v; return this; }
        public Builder csvColumnSeparator(String v) { this.csvColumnSeparator = v; return this; }
        public Builder dorisConfig(Map<String, String> v) { this.dorisConfig = v; return this; }
        public Builder connectTimeoutMs(int v) { this.connectTimeoutMs = v; return this; }
        public Builder socketTimeoutMs(int v) { this.socketTimeoutMs = v; return this; }
        public Builder createTableDdl(String v) { this.createTableDdl = v; return this; }
        public Builder keyType(String v) { this.keyType = v; return this; }
        public Builder buckets(int v) { this.buckets = v; return this; }
        public Builder loadTimeoutSec(int v) { this.loadTimeoutSec = v; return this; }
        public Builder maxFilterRatio(double v) { this.maxFilterRatio = v; return this; }
        public Builder columns(String v) { this.columns = v; return this; }
        public Builder where(String v) { this.where = v; return this; }
        public Builder partitions(String v) { this.partitions = v; return this; }
        public Builder strictMode(boolean v) { this.strictMode = v; return this; }
        public Builder timezone(String v) { this.timezone = v; return this; }
        public Builder execMemLimit(long v) { this.execMemLimit = v; return this; }
        public Builder jsonpaths(String v) { this.jsonpaths = v; return this; }
        public Builder stripOuterArray(boolean v) { this.stripOuterArray = v; return this; }
        public Builder jsonRoot(String v) { this.jsonRoot = v; return this; }
        public Builder sendBatchParallelism(int v) { this.sendBatchParallelism = v; return this; }
        public Builder loadToSingleTablet(boolean v) { this.loadToSingleTablet = v; return this; }
        public Builder lineDelimiter(String v) { this.lineDelimiter = v; return this; }
        public Builder enclose(String v) { this.enclose = v; return this; }
        public Builder escape(String v) { this.escape = v; return this; }
        public Builder numAsString(boolean v) { this.numAsString = v; return this; }
        public Builder fuzzyParse(boolean v) { this.fuzzyParse = v; return this; }
        public Builder compressType(String v) { this.compressType = v; return this; }
        public Builder trimDoubleQuotes(boolean v) { this.trimDoubleQuotes = v; return this; }
        public Builder skipLines(int v) { this.skipLines = v; return this; }
        public Builder loadComment(String v) { this.loadComment = v; return this; }

        public DorisSinkConfig build() {
            return new DorisSinkConfig(this);
        }
    }
}
