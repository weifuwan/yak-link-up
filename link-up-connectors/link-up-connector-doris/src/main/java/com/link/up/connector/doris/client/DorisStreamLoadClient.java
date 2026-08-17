package com.link.up.connector.doris.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.connector.doris.config.DorisLoadFormat;
import com.link.up.connector.doris.config.DorisSinkConfig;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Doris Stream Load 客户端。
 *
 * <p>通过 HTTP PUT 将数据批量写入 Doris，
 * 支持 JSON 和 CSV 两种数据格式。
 */
public final class DorisStreamLoadClient implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(DorisStreamLoadClient.class);

    private static final MediaType TEXT_PLAIN =
            MediaType.parse("text/plain; charset=utf-8");

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final int MAX_REDIRECTS = 3;

    private final DorisSinkConfig config;
    private final OkHttpClient httpClient;
    private final AtomicInteger feRoundRobin = new AtomicInteger(0);

    public DorisStreamLoadClient(DorisSinkConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .followRedirects(false)
                .build();
    }

    /**
     * 执行一次 Stream Load。
     *
     * @param data Stream Load 数据内容（JSON lines 或 CSV）
     * @return Stream Load 响应结果
     */
    public StreamLoadResponse load(String data) throws IOException {
        return load(data, generateLabel());
    }

    /**
     * 执行一次 Stream Load（指定 label）。
     *
     * <p><b>精确一次性保证：</b>重试时复用同一个 label。
     * Doris 对相同 label 的 Stream Load 请求会做幂等校验，
     * 避免网络超时重试导致的数据重复写入。
     */
    public StreamLoadResponse load(String data, String label) throws IOException {
        // 重试时复用同一个 label，保证幂等性
        // 如果 Doris 已处理该 label 的请求，重复提交会返回 Label Already Exists 错误
        // 而不是产生重复数据
        IOException lastException = null;
        int maxAttempts = Math.max(1, config.getMaxRetries() + 1);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            String targetNode = selectTargetNode();
            String url = buildStreamLoadUrl(targetNode);

            LOG.debug("Stream Load to {}, label={}, attempt={}/{}", url, label, attempt, maxAttempts);

            Map<String, String> headers = buildHeaders(label);

            Request request = new Request.Builder()
                    .url(url)
                    .put(RequestBody.create(
                            data.getBytes(StandardCharsets.UTF_8), TEXT_PLAIN))
                    .build();

            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request = request.newBuilder()
                        .header(entry.getKey(), entry.getValue())
                        .build();
            }

            try {
                return doLoad(request);
            } catch (IOException e) {
                lastException = e;
                if (attempt < maxAttempts) {
                    LOG.warn("Stream Load failed (attempt {}/{}), label={}, retrying: {}",
                            attempt, maxAttempts, label, e.getMessage());
                }
            }
        }

        throw lastException;
    }

    private StreamLoadResponse doLoad(Request request) throws IOException {
        return doLoadWithRedirect(request, 0);
    }

    private StreamLoadResponse doLoadWithRedirect(Request request, int redirectCount)
            throws IOException {
        if (redirectCount > MAX_REDIRECTS) {
            throw new IOException("Stream Load 重定向次数超过上限 (" + MAX_REDIRECTS + ")");
        }

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";

            // 处理 307 redirect
            if (response.code() == 307) {
                String redirectUrl = response.header("Location");
                if (redirectUrl != null && !redirectUrl.isEmpty()) {
                    LOG.debug("Stream Load redirect to {} (count={})", redirectUrl, redirectCount + 1);
                    Request redirectRequest = request.newBuilder().url(redirectUrl).build();
                    return doLoadWithRedirect(redirectRequest, redirectCount + 1);
                }
            }

            // 检查 HTTP 错误码，区分可重试和不可重试异常
            if (!response.isSuccessful()) {
                if (response.code() == 401 || response.code() == 403) {
                    throw new IOException("Doris Stream Load 认证失败: httpStatus=" + response.code()
                            + ", body=" + body);
                }
                if (response.code() >= 400 && response.code() < 500) {
                    throw new IOException("Doris Stream Load 客户端错误: httpStatus=" + response.code()
                            + ", body=" + body);
                }
                // 5xx 服务端错误，可重试
                throw new IOException("Doris Stream Load 服务端错误: httpStatus=" + response.code()
                        + ", body=" + body);
            }

            return StreamLoadResponse.parse(response.code(), body);
        }
    }

    private String selectTargetNode() {
        if (config.isDirectToBe()) {
            List<String> beNodes = config.getBeNodeList();
            if (!beNodes.isEmpty()) {
                int idx = Math.abs(feRoundRobin.getAndIncrement() % beNodes.size());
                return beNodes.get(idx);
            }
            LOG.warn("direct_to_be=true but no benodes configured, falling back to fenodes");
        }

        List<String> feNodes = config.getFeNodeList();
        int idx = Math.abs(feRoundRobin.getAndIncrement() % feNodes.size());
        return feNodes.get(idx);
    }

    private String buildStreamLoadUrl(String node) {
        String host = node;
        if (!host.startsWith("http://") && !host.startsWith("https://")) {
            host = "http://" + host;
        }
        return host + "/api/" + config.getDatabase() + "/" + config.getTable() + "/_stream_load";
    }

    private Map<String, String> buildHeaders(String label) {
        Map<String, String> headers = new LinkedHashMap<>();

        // Basic Auth
        String credentials = config.getUsername() + ":" + (config.getPassword() != null ? config.getPassword() : "");
        String encoded = Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));
        headers.put("Authorization", "Basic " + encoded);

        // Label
        headers.put("label", label);

        // 数据格式
        DorisLoadFormat format = config.getLoadFormat();
        if (format == DorisLoadFormat.JSON) {
            headers.put("format", "json");
            headers.put("read_json_by_line", "true");
        } else {
            headers.put("format", "csv");
            headers.put("column_separator", config.getCsvColumnSeparator());
        }

        // 启用删除
        if (config.isEnableDelete()) {
            headers.put("merge_type", "DELETE");
        }

        // 2PC
        if (config.isEnable2pc()) {
            headers.put("two_phase_commit", "true");
        }

        // ── Stream Load 扩展参数 ──────────────────────────

        // 超时
        if (config.getLoadTimeoutSec() > 0) {
            headers.put("timeout", String.valueOf(config.getLoadTimeoutSec()));
        }

        // 最大容错率
        if (config.getMaxFilterRatio() > 0) {
            headers.put("max_filter_ratio", String.valueOf(config.getMaxFilterRatio()));
        }

        // 列映射
        addHeaderIfPresent(headers, "columns", config.getColumns());

        // 过滤条件
        addHeaderIfPresent(headers, "where", config.getWhere());

        // 分区
        addHeaderIfPresent(headers, "partitions", config.getPartitions());

        // 严格模式
        if (config.isStrictMode()) {
            headers.put("strict_mode", "true");
        }

        // 时区
        addHeaderIfPresent(headers, "timezone", config.getTimezone());

        // 内存限制
        if (config.getExecMemLimit() > 0) {
            headers.put("exec_mem_limit", String.valueOf(config.getExecMemLimit()));
        }

        // JSON 相关参数
        addHeaderIfPresent(headers, "jsonpaths", config.getJsonpaths());
        if (config.isStripOuterArray()) {
            headers.put("strip_outer_array", "true");
        }
        addHeaderIfPresent(headers, "json_root", config.getJsonRoot());
        if (config.isNumAsString()) {
            headers.put("num_as_string", "true");
        }
        if (config.isFuzzyParse()) {
            headers.put("fuzzy_parse", "true");
        }

        // 发送并行度
        if (config.getSendBatchParallelism() > 1) {
            headers.put("send_batch_parallelism", String.valueOf(config.getSendBatchParallelism()));
        }

        // 单 Tablet 导入
        if (config.isLoadToSingleTablet()) {
            headers.put("load_to_single_tablet", "true");
        }

        // CSV 扩展参数
        addHeaderIfPresent(headers, "line_delimiter", config.getLineDelimiter());
        addHeaderIfPresent(headers, "enclose", config.getEnclose());
        addHeaderIfPresent(headers, "escape", config.getEscape());

        // CSV 压缩格式
        addHeaderIfPresent(headers, "compress_type", config.getCompressType());

        // CSV 裁剪双引号
        if (config.isTrimDoubleQuotes()) {
            headers.put("trim_double_quotes", "true");
        }

        // CSV 跳过前 N 行
        if (config.getSkipLines() > 0) {
            headers.put("skip_lines", String.valueOf(config.getSkipLines()));
        }

        // 导入备注
        addHeaderIfPresent(headers, "comment", config.getLoadComment());

        // 透传 doris.config（优先级最高，可覆盖以上参数）
        for (Map.Entry<String, String> entry : config.getDorisConfig().entrySet()) {
            headers.put(entry.getKey(), entry.getValue());
        }

        // Expect: 100-continue（大数据量时避免发送失败请求体）
        headers.put("Expect", "100-continue");

        return headers;
    }

    private static void addHeaderIfPresent(Map<String, String> headers, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            headers.put(key, value);
        }
    }

    private String generateLabel() {
        return config.getSinkLabelPrefix() + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    /**
     * 提交一个 2PC 预提交事务。
     *
     * <p>Doris 2PC API：
     * {@code PUT /api/{db}/{table}/_stream_load_2pc}
     * Body: {@code {"txn_id": <id>, "operation": "commit"}}
     *
     * @param txnId Stream Load 返回的事务 ID
     */
    public void commitTransaction(String txnId) throws IOException {
        executeTransactionOperation(txnId, "commit");
    }

    /**
     * 回滚一个 2PC 预提交事务。
     *
     * @param txnId Stream Load 返回的事务 ID
     */
    public void abortTransaction(String txnId) throws IOException {
        executeTransactionOperation(txnId, "abort");
    }

    /**
     * 批量提交事务。
     *
     * @param txnIds 事务 ID 列表
     */
    public void commitTransactions(List<String> txnIds) throws IOException {
        IOException lastException = null;
        for (String txnId : txnIds) {
            try {
                commitTransaction(txnId);
            } catch (IOException e) {
                LOG.error("Failed to commit txnId={}: {}", txnId, e.getMessage());
                if (lastException == null) {
                    lastException = e;
                } else {
                    lastException.addSuppressed(e);
                }
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * 批量回滚事务。
     *
     * @param txnIds 事务 ID 列表
     */
    public void abortTransactions(List<String> txnIds) {
        for (String txnId : txnIds) {
            try {
                abortTransaction(txnId);
            } catch (IOException e) {
                LOG.warn("Failed to abort txnId={}: {}", txnId, e.getMessage());
            }
        }
    }

    private void executeTransactionOperation(String txnId, String operation) throws IOException {
        String targetNode = selectTargetNode();
        String url = build2pcUrl(targetNode);

        String body = "{\"txn_id\": " + txnId + ", \"operation\": \"" + operation + "\"}";

        LOG.debug("2PC {} txnId={} to {}", operation, txnId, url);

        String credentials = config.getUsername() + ":" + (config.getPassword() != null ? config.getPassword() : "");
        String encoded = Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));

        Request request = new Request.Builder()
                .url(url)
                .put(RequestBody.create(
                        body.getBytes(StandardCharsets.UTF_8), TEXT_PLAIN))
                .header("Authorization", "Basic " + encoded)
                .header("Content-Type", "application/json")
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";

            if (!response.isSuccessful()) {
                throw new IOException("Doris 2PC " + operation + " failed: txnId=" + txnId
                        + ", httpStatus=" + response.code()
                        + ", body=" + responseBody);
            }

            // 检查响应中的 Status 字段
            try {
                JsonNode root = JSON_MAPPER.readTree(responseBody);
                String status = root.has("status") ? root.get("status").asText() : null;
                if (status != null && !"OK".equalsIgnoreCase(status) && !"Success".equalsIgnoreCase(status)) {
                    String msg = root.has("message") ? root.get("message").asText() : null;
                    throw new IOException("Doris 2PC " + operation + " failed: txnId=" + txnId
                            + ", status=" + status + ", message=" + msg);
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn("Failed to parse 2PC response JSON, body={}", responseBody, e);
            }

            LOG.debug("2PC {} txnId={} success", operation, txnId);
        }
    }

    private String build2pcUrl(String node) {
        String host = node;
        if (!host.startsWith("http://") && !host.startsWith("https://")) {
            host = "http://" + host;
        }
        return host + "/api/" + config.getDatabase() + "/" + config.getTable() + "/_stream_load_2pc";
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    /**
     * Stream Load 响应结果。
     */
    public static final class StreamLoadResponse {
        private final int httpStatus;
        private final String body;
        private final String status;
        private final String message;
        private final long numberTotalRows;
        private final long numberLoadedRows;
        private final long numberFilteredRows;
        private final long numberUnselectedRows;
        private final String txnId;
        private final String label;
        private final String txnState;

        private StreamLoadResponse(int httpStatus, String body, String status, String message,
                                   long totalRows, long loadedRows, long filteredRows,
                                   long unselectedRows, String txnId, String label, String txnState) {
            this.httpStatus = httpStatus;
            this.body = body;
            this.status = status;
            this.message = message;
            this.numberTotalRows = totalRows;
            this.numberLoadedRows = loadedRows;
            this.numberFilteredRows = filteredRows;
            this.numberUnselectedRows = unselectedRows;
            this.txnId = txnId;
            this.label = label;
            this.txnState = txnState;
        }

        /**
         * 解析 Stream Load 响应 JSON。
         *
         * <p>Doris Stream Load 返回格式：
         * <pre>
         * {
         *   "Status": "Success",
         *   "Message": "...",
         *   "NumberTotalRows": 100,
         *   "NumberLoadedRows": 100,
         *   "TxnId": 12345,
         *   "Label": "...",
         *   "TxnState": "PREPARE",
         *   ...
         * }
         * </pre>
         *
         * <p>注意：TxnId 在 Doris 响应中是数字类型（无引号），
         * 需要同时支持字符串和数字两种解析方式。
         */
        public static StreamLoadResponse parse(int httpStatus, String body) {
            try {
                JsonNode root = JSON_MAPPER.readTree(body);

                String status = getTextValue(root, "Status");
                String message = getTextValue(root, "Message");
                long totalRows = getLongValue(root, "NumberTotalRows");
                long loadedRows = getLongValue(root, "NumberLoadedRows");
                long filteredRows = getLongValue(root, "NumberFilteredRows");
                long unselectedRows = getLongValue(root, "NumberUnselectedRows");
                // TxnId 在 Doris 响应中可能是数字或字符串
                String txnId = getTextValue(root, "TxnId");
                if (txnId == null) {
                    long txnIdLong = getLongValue(root, "TxnId");
                    txnId = txnIdLong >= 0 ? String.valueOf(txnIdLong) : null;
                }
                String label = getTextValue(root, "Label");
                String txnState = getTextValue(root, "TxnState");

                return new StreamLoadResponse(httpStatus, body, status, message,
                        totalRows, loadedRows, filteredRows, unselectedRows,
                        txnId, label, txnState);
            } catch (Exception e) {
                LOG.warn("Failed to parse Stream Load response JSON, body={}", body, e);
                return new StreamLoadResponse(httpStatus, body, null, body,
                        0, 0, 0, 0, null, null, null);
            }
        }

        private static String getTextValue(JsonNode node, String field) {
            JsonNode child = node.get(field);
            if (child == null || child.isNull()) return null;
            return child.isTextual() ? child.asText() : child.toString();
        }

        private static long getLongValue(JsonNode node, String field) {
            JsonNode child = node.get(field);
            if (child == null || child.isNull()) return -1;
            return child.isNumber() ? child.asLong() : -1;
        }

        public boolean isSuccess() {
            return "Success".equalsIgnoreCase(status)
                    || "OK".equalsIgnoreCase(status);
        }

        public boolean isPrepared() {
            return "PREPARE".equalsIgnoreCase(txnState);
        }

        public void checkSuccess() throws IOException {
            if (!isSuccess()) {
                throw new IOException("Doris Stream Load failed: status=" + status
                        + ", message=" + message
                        + ", httpStatus=" + httpStatus
                        + ", body=" + body);
            }
        }

        // ── Getters ──────────────────────────────────────

        public int getHttpStatus() { return httpStatus; }
        public String getBody() { return body; }
        public String getStatus() { return status; }
        public String getMessage() { return message; }
        public long getNumberTotalRows() { return numberTotalRows; }
        public long getNumberLoadedRows() { return numberLoadedRows; }
        public long getNumberFilteredRows() { return numberFilteredRows; }
        public long getNumberUnselectedRows() { return numberUnselectedRows; }
        public String getTxnId() { return txnId; }
        public String getLabel() { return label; }
        public String getTxnState() { return txnState; }

    }
}
