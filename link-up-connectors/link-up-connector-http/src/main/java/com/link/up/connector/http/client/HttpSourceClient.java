package com.link.up.connector.http.client;

import com.link.up.connector.http.config.HttpFormat;
import com.link.up.connector.http.config.HttpMethod;
import com.link.up.connector.http.config.HttpSourceConfig;
import okhttp3.ConnectionPool;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * HTTP 请求客户端。
 *
 * <p>封装 OkHttp，提供带连接池和增强重试策略的 HTTP 请求执行能力。
 *
 * <h3>连接池</h3>
 * <p>通过 {@code pool.max_idle_connections} 和 {@code pool.keep_alive_duration_ms}
 * 显式配置 OkHttp {@link ConnectionPool}，避免高并发分页场景下频繁建立连接。
 *
 * <h3>重试策略</h3>
 * <ul>
 *     <li>指数退避：{@code backoff = multiplier * 2^(attempt-1)}，上限 {@code retry_backoff_max_ms}</li>
 *     <li>抖动因子：在退避时间上叠加 {@code [0, retry_jitter_ms)} 随机值，防止惊群</li>
 *     <li>可重试状态码：通过 {@code retryable_status_codes} 配置哪些 HTTP 状态码触发重试</li>
 *     <li>连接级异常（连接超时、连接拒绝等）：安全重试</li>
 *     <li>响应体读取异常：不重试，因为服务端可能已返回数据，重试会导致下游重复写入</li>
 *     <li>POST 请求：默认不重试，因为服务端可能已执行副作用</li>
 * </ul>
 */
public final class HttpSourceClient implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpSourceClient.class);

    private static final MediaType JSON_MEDIA_TYPE =
            MediaType.parse("application/json; charset=utf-8");

    private final HttpSourceConfig config;
    private final OkHttpClient httpClient;

    public HttpSourceClient(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");

        ConnectionPool connectionPool = new ConnectionPool(
                config.getPoolMaxIdleConnections(),
                config.getPoolKeepAliveDurationMs(),
                TimeUnit.MILLISECONDS);

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .connectionPool(connectionPool)
                .build();
    }

    /**
     * 执行一次 HTTP 请求，返回响应体字符串。
     *
     * <p>包含增强重试逻辑：
     * <ul>
     *     <li>连接级异常（连接超时、连接拒绝等）安全重试</li>
     *     <li>响应体读取异常不重试（服务端可能已返回数据，重试导致下游重复写入）</li>
     *     <li>POST 请求默认不重试（服务端可能已执行副作用）</li>
     *     <li>可重试的 HTTP 状态码（如 429、500、502、503、504）重试</li>
     *     <li>其他 4xx/5xx 状态码不重试，直接抛出异常</li>
     *     <li>指数退避 + 随机抖动，避免惊群效应</li>
     * </ul>
     *
     * @param effectiveHeaders 当前请求头（可能已替换分页占位符）
     * @param effectiveParams  当前请求参数（可能已替换分页占位符）
     * @param effectiveBody    当前请求体（可能已替换分页占位符）
     * @return 响应体文本
     */
    public String execute(
            Map<String, String> effectiveHeaders,
            Map<String, String> effectiveParams,
            String effectiveBody) throws IOException {

        IOException lastException = null;
        int maxAttempts = Math.max(1, config.getRetry() + 1);
        Set<Integer> retryableCodes = config.getRetryableStatusCodes();

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return doExecute(effectiveHeaders, effectiveParams, effectiveBody);
            } catch (HttpRetryableException retryable) {
                lastException = retryable;
                if (attempt < maxAttempts && isRetryable(retryable.getStatusCode(), retryable, retryableCodes)) {
                    long backoff = computeBackoff(attempt, maxAttempts);
                    LOG.warn("HTTP request returned retryable status {} (attempt {}/{}), retrying in {} ms: {}",
                            retryable.getStatusCode(), attempt, maxAttempts, backoff, retryable.getMessage());
                    sleep(backoff);
                } else {
                    throw retryable;
                }
            } catch (ResponseReadException e) {
                /*
                 * 响应体读取失败：服务端已处理请求并返回了数据，
                 * 但客户端在读取响应体时网络中断。
                 * 此时重试会导致相同数据被再次获取，造成下游重复写入。
                 */
                LOG.error("HTTP response read failed, server may have returned data. "
                        + "Skipping retry to avoid duplicate downstream writes: {}", e.getMessage());
                throw e;
            } catch (IOException e) {
                lastException = e;
                if (attempt < maxAttempts && isMethodRetryable()) {
                    long backoff = computeBackoff(attempt, maxAttempts);
                    LOG.warn("HTTP request failed (attempt {}/{}), retrying in {} ms: {}",
                            attempt, maxAttempts, backoff, e.getMessage());
                    sleep(backoff);
                } else if (!isMethodRetryable()) {
                    LOG.warn("HTTP {} request failed, POST requests are not retried by default "
                            + "to avoid duplicate side effects: {}", config.getMethod(), e.getMessage());
                    throw e;
                }
            }
        }

        throw lastException;
    }

    /**
     * 判断当前请求方法是否允许重试。
     *
     * <p>GET 请求是幂等的，可以安全重试；
     * POST 请求可能包含副作用，默认不重试以避免重复写入。
     */
    private boolean isMethodRetryable() {
        return config.getMethod() == HttpMethod.GET;
    }

    /**
     * 判断给定的 HTTP 状态码是否可重试。
     */
    private boolean isRetryable(int statusCode, HttpRetryableException exception, Set<Integer> retryableCodes) {
        return retryableCodes.contains(statusCode);
    }

    /**
     * 计算指数退避 + 抖动的等待时间。
     *
     * <p>公式：{@code min(multiplier * 2^(attempt-1), max_backoff) + random(0, jitter)}
     */
    private long computeBackoff(int attempt, int maxAttempts) {
        long baseBackoff = Math.min(
                (long) config.getRetryBackoffMultiplierMs() * (1L << (attempt - 1)),
                config.getRetryBackoffMaxMs());

        int jitterMax = config.getRetryJitterMs();
        if (jitterMax > 0) {
            long jitter = ThreadLocalRandom.current().nextLong(0, jitterMax + 1);
            return baseBackoff + jitter;
        }

        return baseBackoff;
    }

    private String doExecute(
            Map<String, String> effectiveHeaders,
            Map<String, String> effectiveParams,
            String effectiveBody) throws IOException {

        String url = buildUrl(effectiveParams);

        Request.Builder requestBuilder = new Request.Builder().url(url);

        // 设置请求头
        if (effectiveHeaders != null) {
            for (Map.Entry<String, String> entry : effectiveHeaders.entrySet()) {
                requestBuilder.header(entry.getKey(), entry.getValue());
            }
        }

        // 设置请求方法和请求体
        HttpMethod method = config.getMethod();
        if (method == HttpMethod.POST) {
            String bodyContent = effectiveBody != null ? effectiveBody : config.getBody();
            RequestBody requestBody;
            if (bodyContent != null && !bodyContent.isEmpty()) {
                String contentType = getContentType(effectiveHeaders);
                if ("application/x-www-form-urlencoded".equalsIgnoreCase(contentType)) {
                    requestBody = buildFormBody(bodyContent, effectiveParams);
                } else {
                    requestBody = RequestBody.create(bodyContent, JSON_MEDIA_TYPE);
                }
            } else {
                // POST 无 body 时发送空 JSON
                requestBody = RequestBody.create("{}", JSON_MEDIA_TYPE);
            }
            requestBuilder.post(requestBody);
        } else {
            requestBuilder.get();
        }

        LOG.debug("HTTP {} {}", method, url);

        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            if (!response.isSuccessful()) {
                int statusCode = response.code();
                String message = "HTTP request failed with status " + statusCode + " for URL: " + url;

                /*
                 * 可重试的状态码包装为 HttpRetryableException，
                 * 由上层 execute() 决定是否重试。
                 */
                if (config.getRetryableStatusCodes().contains(statusCode)) {
                    ResponseBody errorBody = response.body();
                    String errorDetail = errorBody != null ? errorBody.string() : "";
                    throw new HttpRetryableException(statusCode, message + " (body: " + truncate(errorDetail, 500) + ")");
                }

                LOG.warn("HTTP request failed: status={}, url={}", statusCode, url);
                throw new IOException(message);
            }
            ResponseBody responseBody = response.body();
            String body;
            try {
                body = responseBody != null ? responseBody.string() : "";
            } catch (IOException e) {
                /*
                 * 响应体读取失败（如连接被重置、超时等）。
                 * 服务端已处理请求并可能已返回了部分/全部数据，
                 * 包装为 ResponseReadException 以阻止上层重试，
                 * 避免下游重复写入。
                 */
                throw new ResponseReadException(
                        "Failed to read HTTP response body for URL: " + url
                                + ". Data may have been partially received.", e);
            }
            LOG.debug("HTTP response: status={}, bodyLength={}",
                    response.code(), body.length());
            return body;
        }
    }

    private String buildUrl(Map<String, String> effectiveParams) {
        StringBuilder urlBuilder = new StringBuilder(config.getUrl());

        // 合并静态 params 和分页 params
        Map<String, String> allParams = new LinkedHashMap<>();
        if (config.getParams() != null) {
            allParams.putAll(config.getParams());
        }
        if (effectiveParams != null) {
            allParams.putAll(effectiveParams);
        }

        if (!allParams.isEmpty()) {
            char separator = config.getUrl().contains("?") ? '&' : '?';
            for (Map.Entry<String, String> entry : allParams.entrySet()) {
                urlBuilder.append(separator)
                        .append(urlEncode(entry.getKey()))
                        .append('=')
                        .append(urlEncode(entry.getValue()));
                separator = '&';
            }
        }

        return urlBuilder.toString();
    }

    private RequestBody buildFormBody(
            String bodyContent,
            Map<String, String> effectiveParams) {
        FormBody.Builder formBuilder = new FormBody.Builder();

        // 解析 body 中的 form 参数（简单 key=value&key2=value2 格式）
        if (bodyContent != null && !bodyContent.isEmpty()) {
            for (String pair : bodyContent.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    formBuilder.add(
                            pair.substring(0, eq).trim(),
                            pair.substring(eq + 1).trim());
                }
            }
        }

        // 追加 params
        if (effectiveParams != null) {
            for (Map.Entry<String, String> entry : effectiveParams.entrySet()) {
                formBuilder.add(entry.getKey(), entry.getValue());
            }
        }

        return formBuilder.build();
    }

    private String getContentType(Map<String, String> headers) {
        if (headers == null) {
            return null;
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if ("content-type".equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (Exception e) {
            return value;
        }
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength) + "...";
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    /**
     * 响应体读取异常。
     *
     * <p>当 HTTP 响应头已接收但读取响应体失败时抛出。
     * 此时服务端可能已处理请求并返回了数据，
     * 重试会导致下游重复写入，因此上层不应重试此异常。
     */
    static final class ResponseReadException extends IOException {
        ResponseReadException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * 可重试的 HTTP 异常。
     *
     * <p>当服务端返回的状态码在 {@code retryable_status_codes} 列表中时，
     * 抛出该异常以触发重试逻辑。
     */
    static final class HttpRetryableException extends IOException {
        private final int statusCode;

        HttpRetryableException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        int getStatusCode() {
            return statusCode;
        }
    }
}
