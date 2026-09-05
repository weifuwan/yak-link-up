package com.link.up.connector.starrocks.client.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.Column;
import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPlan;
import com.link.up.connector.starrocks.config.StarRocksSourceConfig;
import com.link.up.connector.starrocks.config.StarRocksSourceTableConfig;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Obtains opaque native scan plans from StarRocks FE over HTTP. */
public final class StarRocksQueryPlanClient implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(StarRocksQueryPlanClient.class);

    private static final MediaType JSON =
            MediaType.parse("application/json; charset=utf-8");

    private final StarRocksSourceConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OkHttpClient httpClient;

    public StarRocksQueryPlanClient(StarRocksSourceConfig config) {
        this.config = config;
        long readTimeoutMs =
                config.getQueryTimeoutSec() < 0
                        ? 0L
                        : Math.max(1000L, config.getQueryTimeoutSec() * 1000L);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .writeTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
        if (readTimeoutMs == 0L) {
            builder.readTimeout(0L, TimeUnit.MILLISECONDS);
        } else {
            builder.readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS);
        }
        this.httpClient = builder.build();
    }

    public StarRocksQueryPlan fetchQueryPlan(
            StarRocksSourceTableConfig tableConfig) throws IOException {

        String sql = buildQuerySql(tableConfig);
        String requestBody =
                objectMapper.writeValueAsString(
                        java.util.Collections.singletonMap("sql", sql));

        IOException lastFailure = null;
        int attempts = Math.max(1, config.getMaxRetries() + 1);
        List<String> nodes = config.getNodeUrls();

        for (int attempt = 0; attempt < attempts; attempt++) {
            String node = nodes.get(attempt % nodes.size());
            String url =
                    normalizeHttpNode(node)
                            + "/api/"
                            + tableConfig.getDatabase()
                            + "/"
                            + tableConfig.getTable()
                            + "/_query_plan";

            Request request =
                    new Request.Builder()
                            .url(url)
                            .header("Authorization", basicAuth())
                            .header("Accept", "application/json")
                            .post(
                                    RequestBody.create(
                                            requestBody.getBytes(StandardCharsets.UTF_8),
                                            JSON))
                            .build();

            try (Response response = httpClient.newCall(request).execute()) {
                ResponseBody body = response.body();
                String responseText = body == null ? "" : body.string();
                if (!response.isSuccessful()) {
                    throw new IOException(
                            "StarRocks FE query-plan request failed: httpStatus="
                                    + response.code()
                                    + ", node="
                                    + node
                                    + ", body="
                                    + abbreviate(responseText, 1000));
                }

                StarRocksQueryPlan plan =
                        objectMapper.readValue(responseText, StarRocksQueryPlan.class);
                validatePlan(plan, tableConfig, node);
                return plan;
            } catch (IOException failure) {
                lastFailure = failure;
                LOG.warn(
                        "StarRocks FE query-plan request failed: table={}.{}, node={}, attempt={}/{}, error={}",
                        tableConfig.getDatabase(),
                        tableConfig.getTable(),
                        node,
                        attempt + 1,
                        attempts,
                        failure.getMessage());
            }
        }

        throw new IOException(
                "Unable to obtain StarRocks query plan for "
                        + tableConfig.getDatabase()
                        + "."
                        + tableConfig.getTable()
                        + " after "
                        + attempts
                        + " attempts",
                lastFailure);
    }

    static String buildQuerySql(StarRocksSourceTableConfig tableConfig) {
        List<String> fields = new ArrayList<String>();
        for (Column column : tableConfig.getCatalogTable().getTableSchema().getColumns()) {
            fields.add(quoteIdentifier(column.getName()));
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(
                    "StarRocks Native Source requires at least one projected field");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
                .append(String.join(", ", fields))
                .append(" FROM ")
                .append(quoteIdentifier(tableConfig.getDatabase()))
                .append('.')
                .append(quoteIdentifier(tableConfig.getTable()));
        if (hasText(tableConfig.getScanFilter())) {
            sql.append(" WHERE ").append(tableConfig.getScanFilter().trim());
        }
        return sql.toString();
    }

    private void validatePlan(
            StarRocksQueryPlan plan,
            StarRocksSourceTableConfig table,
            String node) throws IOException {
        if (plan == null || !hasText(plan.getOpaquedQueryPlan())) {
            throw new IOException(
                    "StarRocks FE returned an invalid query plan: table="
                            + table.getDatabase()
                            + "."
                            + table.getTable()
                            + ", node="
                            + node);
        }
        if (plan.getPartitions() == null) {
            throw new IOException(
                    "StarRocks FE query plan does not contain partitions: table="
                            + table.getDatabase()
                            + "."
                            + table.getTable());
        }
    }

    private String basicAuth() {
        String credentials = config.getUsername() + ":" + config.getPassword();
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    private static String normalizeHttpNode(String node) {
        String value = node.trim();
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "http://" + value;
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String quoteIdentifier(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String abbreviate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }
}
