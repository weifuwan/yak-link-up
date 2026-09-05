package com.link.up.connector.starrocks.client.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.starrocks.config.StarRocksLoadFormat;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;
import okhttp3.HttpUrl;
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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Task-local HTTP client for StarRocks Stream Load. */
public final class StarRocksStreamLoadClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadClient.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final MediaType JSON_MEDIA_TYPE =
            MediaType.parse("application/json; charset=utf-8");
    private static final MediaType TEXT_MEDIA_TYPE =
            MediaType.parse("text/plain; charset=utf-8");
    private static final int MAX_REDIRECTS = 5;

    private final StarRocksSinkConfig config;
    private final OkHttpClient httpClient;
    private final AtomicInteger nextNode = new AtomicInteger();

    public StarRocksStreamLoadClient(StarRocksSinkConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.httpClient =
                new OkHttpClient.Builder()
                        .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .readTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                        .writeTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                        .followRedirects(false)
                        .build();
    }

    public StarRocksStreamLoadResponse load(
            byte[] payload,
            TableSchema schema)
            throws IOException {
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        if (payload.length == 0) {
            throw new IllegalArgumentException("StarRocks Stream Load payload must not be empty");
        }

        String label = createLabel();
        IOException lastFailure = null;
        int maxAttempts = Math.max(1, config.getMaxRetries() + 1);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                StarRocksStreamLoadResponse response =
                        executeStreamLoad(payload, label, schema);

                if (response.isSuccess()) {
                    return response;
                }

                if (response.isLabelAlreadyExists()) {
                    LabelResolution resolution = waitForLabelResolution(label);
                    if (resolution == LabelResolution.COMMITTED) {
                        return StarRocksStreamLoadResponse.resolvedCommitted(
                                label,
                                response.getBody());
                    }
                    if (resolution == LabelResolution.ABORTED) {
                        String oldLabel = label;
                        label = createLabel();
                        LOG.warn(
                                "StarRocks Stream Load label {} is ABORTED; retrying with new label {}",
                                oldLabel,
                                label);
                        continue;
                    }
                }

                throw new NonRetryableStreamLoadException(
                        "StarRocks Stream Load failed: status="
                                + response.getStatus()
                                + ", label="
                                + label
                                + ", message="
                                + response.getMessage()
                                + ", errorUrl="
                                + response.getErrorUrl());

            } catch (NonRetryableStreamLoadException failure) {
                throw failure;
            } catch (IOException failure) {
                lastFailure = failure;
                if (attempt >= maxAttempts) {
                    break;
                }
                LOG.warn(
                        "StarRocks Stream Load attempt {}/{} failed for label={}, retrying same label: {}",
                        attempt,
                        maxAttempts,
                        label,
                        failure.getMessage());
                sleepBeforeRetry(attempt);
            }
        }

        throw new IOException(
                "StarRocks Stream Load exceeded retry limit for label=" + label,
                lastFailure);
    }

    private StarRocksStreamLoadResponse executeStreamLoad(
            byte[] payload,
            String label,
            TableSchema schema)
            throws IOException {
        String node = selectNode();
        HttpUrl url = buildStreamLoadUrl(node);
        Map<String, String> headers = buildHeaders(label, schema);
        MediaType mediaType = config.getLoadFormat() == StarRocksLoadFormat.JSON
                ? JSON_MEDIA_TYPE
                : TEXT_MEDIA_TYPE;

        Request.Builder builder =
                new Request.Builder()
                        .url(url)
                        .put(RequestBody.create(payload, mediaType));
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.header(entry.getKey(), entry.getValue());
        }
        return executeWithRedirect(builder.build(), 0);
    }

    private StarRocksStreamLoadResponse executeWithRedirect(
            Request request,
            int redirectCount)
            throws IOException {
        if (redirectCount > MAX_REDIRECTS) {
            throw new IOException(
                    "StarRocks Stream Load redirect count exceeded " + MAX_REDIRECTS);
        }

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() == 307 || response.code() == 308) {
                String location = response.header("Location");
                if (location == null || location.trim().isEmpty()) {
                    throw new IOException(
                            "StarRocks Stream Load redirect response has no Location header");
                }
                Request redirected = request.newBuilder().url(location).build();
                return executeWithRedirect(redirected, redirectCount + 1);
            }

            String body = response.body() == null ? "" : response.body().string();
            if (!response.isSuccessful()) {
                if (response.code() >= 400
                        && response.code() < 500
                        && response.code() != 408
                        && response.code() != 429) {
                    throw new NonRetryableStreamLoadException(
                            "StarRocks Stream Load HTTP client error: status="
                                    + response.code()
                                    + ", body="
                                    + body);
                }
                throw new IOException(
                        "StarRocks Stream Load HTTP error: status="
                                + response.code()
                                + ", body="
                                + body);
            }

            StarRocksStreamLoadResponse loadResponse =
                    StarRocksStreamLoadResponse.parse(body);
            LOG.debug(
                    "StarRocks Stream Load response: status={}, label={}, loaded={}, filtered={}",
                    loadResponse.getStatus(),
                    loadResponse.getLabel(),
                    loadResponse.getNumberLoadedRows(),
                    loadResponse.getNumberFilteredRows());
            return loadResponse;
        }
    }

    Map<String, String> buildHeaders(
            String label,
            TableSchema schema) {
        Map<String, String> headers = new LinkedHashMap<String, String>();
        headers.putAll(config.getStreamLoadParams());
        headers.put("Authorization", basicAuth());
        headers.put("label", label);
        headers.put("Expect", "100-continue");

        if (config.getLoadFormat() == StarRocksLoadFormat.JSON) {
            headers.put("format", "json");
            headers.put("strip_outer_array", "true");
        } else {
            headers.put("format", "csv");
            headers.put("column_separator", config.getColumnSeparator());
            headers.put("row_delimiter", config.getRowDelimiter());
            headers.put("columns", columnsHeader(schema));
        }
        return headers;
    }

    private LabelResolution waitForLabelResolution(String label) throws IOException {
        long deadlineNanos =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(config.getLabelStateTimeoutMs());

        while (System.nanoTime() < deadlineNanos) {
            String state = queryLabelState(label);
            if ("VISIBLE".equalsIgnoreCase(state)
                    || "COMMITTED".equalsIgnoreCase(state)) {
                return LabelResolution.COMMITTED;
            }
            if ("ABORTED".equalsIgnoreCase(state)) {
                return LabelResolution.ABORTED;
            }
            if (!"PREPARE".equalsIgnoreCase(state)) {
                throw new IOException(
                        "StarRocks Stream Load label has unknown final state: label="
                                + label
                                + ", state="
                                + state);
            }
            sleepForLabelPoll(deadlineNanos);
        }

        throw new IOException(
                "Timed out waiting for StarRocks Stream Load label state: label=" + label);
    }

    private String queryLabelState(String label) throws IOException {
        HttpUrl url = buildLabelStateUrl(selectNode(), label);
        Request request =
                new Request.Builder()
                        .url(url)
                        .get()
                        .header("Authorization", basicAuth())
                        .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() == null ? "" : response.body().string();
            if (!response.isSuccessful()) {
                throw new IOException(
                        "Failed to query StarRocks Stream Load label state: status="
                                + response.code()
                                + ", body="
                                + body);
            }
            JsonNode root = JSON_MAPPER.readTree(body);
            JsonNode state = root == null ? null : root.get("state");
            if (state == null || state.isNull() || state.asText().trim().isEmpty()) {
                throw new IOException(
                        "StarRocks label state response has no state: " + body);
            }
            return state.asText().trim();
        }
    }

    private HttpUrl buildStreamLoadUrl(String node) {
        return baseUrl(node)
                .newBuilder()
                .encodedPath("/")
                .query(null)
                .addPathSegment("api")
                .addPathSegment(config.getDatabase())
                .addPathSegment(config.getTable())
                .addPathSegment("_stream_load")
                .build();
    }

    private HttpUrl buildLabelStateUrl(String node, String label) {
        return baseUrl(node)
                .newBuilder()
                .encodedPath("/")
                .query(null)
                .addPathSegment("api")
                .addPathSegment(config.getDatabase())
                .addPathSegment("get_load_state")
                .addQueryParameter("label", label)
                .build();
    }

    private static HttpUrl baseUrl(String node) {
        String value = node;
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "http://" + value;
        }
        HttpUrl url = HttpUrl.parse(value);
        if (url == null) {
            throw new IllegalArgumentException("Invalid StarRocks node URL: " + node);
        }
        return url;
    }

    private String selectNode() {
        int size = config.getNodeUrls().size();
        int index = Math.floorMod(nextNode.getAndIncrement(), size);
        return config.getNodeUrls().get(index);
    }

    private String createLabel() {
        return config.getLabelPrefix()
                + UUID.randomUUID().toString().replace("-", "");
    }

    private String basicAuth() {
        String auth = config.getUsername() + ":" + config.getPassword();
        return "Basic "
                + Base64.getEncoder().encodeToString(
                        auth.getBytes(StandardCharsets.UTF_8));
    }

    private static String columnsHeader(TableSchema schema) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            String name = schema.getColumn(i).getName();
            builder.append('`').append(name.replace("`", "``")).append('`');
        }
        return builder.toString();
    }

    private void sleepBeforeRetry(int attempt) throws IOException {
        long delay = Math.min(
                (long) config.getRetryBackoffMs() * Math.max(1, attempt),
                (long) config.getMaxRetryBackoffMs());
        sleep(delay, "retrying StarRocks Stream Load");
    }

    private void sleepForLabelPoll(long deadlineNanos) throws IOException {
        long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        long delay = Math.min(config.getLabelStatePollMs(), Math.max(1L, remainingMs));
        sleep(delay, "waiting for StarRocks label state");
    }

    private static void sleep(long millis, String operation) throws IOException {
        if (millis <= 0L) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while " + operation, failure);
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private enum LabelResolution {
        COMMITTED,
        ABORTED
    }

    private static final class NonRetryableStreamLoadException extends IOException {
        private static final long serialVersionUID = 1L;

        private NonRetryableStreamLoadException(String message) {
            super(message);
        }
    }
}
