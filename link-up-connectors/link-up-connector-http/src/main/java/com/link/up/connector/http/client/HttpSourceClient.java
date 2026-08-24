package com.link.up.connector.http.client;

import com.link.up.connector.http.config.HttpSourceConfig;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Executes HTTP source requests with connector-level retry semantics.
 *
 * <p>Request construction belongs to {@link HttpRequestFactory}; retry
 * decisions belong to {@link HttpRetryPolicy}. This client owns OkHttp
 * resources and response safety semantics.</p>
 */
public final class HttpSourceClient
        implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpSourceClient.class);

    private final HttpSourceConfig config;
    private final OkHttpClient httpClient;
    private final HttpRequestFactory requestFactory;
    private final HttpRetryPolicy retryPolicy;

    public HttpSourceClient(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(
                config,
                "config must not be null");

        ConnectionPool connectionPool =
                new ConnectionPool(
                        config.getPoolMaxIdleConnections(),
                        config.getPoolKeepAliveDurationMs(),
                        TimeUnit.MILLISECONDS);

        this.httpClient =
                new OkHttpClient.Builder()
                        .connectTimeout(
                                config.getConnectTimeoutMs(),
                                TimeUnit.MILLISECONDS)
                        .readTimeout(
                                config.getSocketTimeoutMs(),
                                TimeUnit.MILLISECONDS)
                        .writeTimeout(
                                config.getSocketTimeoutMs(),
                                TimeUnit.MILLISECONDS)
                        .connectionPool(
                                connectionPool)
                        .build();

        this.requestFactory =
                new HttpRequestFactory(config);
        this.retryPolicy =
                new HttpRetryPolicy(config);
    }

    public String execute(
            Map<String, String> effectiveHeaders,
            Map<String, String> effectiveParams,
            String effectiveBody)
            throws IOException {

        IOException lastFailure = null;
        int maxAttempts =
                retryPolicy.maxAttempts();

        for (int attempt = 1;
             attempt <= maxAttempts;
             attempt++) {

            try {
                return doExecute(
                        effectiveHeaders,
                        effectiveParams,
                        effectiveBody);

            } catch (HttpRetryableException retryable) {
                lastFailure = retryable;

                if (attempt >= maxAttempts
                        || !retryPolicy.isStatusRetryable(
                                retryable.getStatusCode())) {
                    throw retryable;
                }

                retry(
                        attempt,
                        maxAttempts,
                        "HTTP request returned retryable status "
                                + retryable.getStatusCode(),
                        retryable);

            } catch (ResponseReadException readFailure) {
                LOG.error(
                        "HTTP response read failed; skipping retry to avoid "
                                + "duplicate downstream writes: {}",
                        readFailure.getMessage());
                throw readFailure;

            } catch (IOException failure) {
                lastFailure = failure;

                if (!retryPolicy.isMethodRetryable()) {
                    LOG.warn(
                            "HTTP {} request failed; non-idempotent requests "
                                    + "are not retried by default: {}",
                            config.getMethod(),
                            failure.getMessage());
                    throw failure;
                }

                if (attempt >= maxAttempts) {
                    throw failure;
                }

                retry(
                        attempt,
                        maxAttempts,
                        "HTTP request failed",
                        failure);
            }
        }

        throw lastFailure == null
                ? new IOException(
                        "HTTP request failed without an execution attempt")
                : lastFailure;
    }

    @Override
    public void close() {
        httpClient.dispatcher()
                .executorService()
                .shutdown();
        httpClient.connectionPool()
                .evictAll();
    }

    private String doExecute(
            Map<String, String> headers,
            Map<String, String> params,
            String body)
            throws IOException {

        Request request =
                requestFactory.create(
                        headers,
                        params,
                        body);

        String url =
                request.url()
                        .toString();

        LOG.debug(
                "HTTP {} {}",
                config.getMethod(),
                url);

        try (Response response =
                     httpClient.newCall(request)
                             .execute()) {

            if (!response.isSuccessful()) {
                throwHttpFailure(
                        response,
                        url);
            }

            String responseBody =
                    readResponseBody(
                            response,
                            url);

            LOG.debug(
                    "HTTP response: status={}, bodyLength={}",
                    response.code(),
                    responseBody.length());

            return responseBody;
        }
    }

    private void throwHttpFailure(
            Response response,
            String url)
            throws IOException {

        int statusCode =
                response.code();

        String message =
                "HTTP request failed with status "
                        + statusCode
                        + " for URL: "
                        + url;

        if (retryPolicy.isStatusRetryable(statusCode)) {
            ResponseBody errorBody =
                    response.body();

            String detail =
                    errorBody == null
                            ? ""
                            : errorBody.string();

            throw new HttpRetryableException(
                    statusCode,
                    message
                            + " (body: "
                            + truncate(
                                    detail,
                                    500)
                            + ")");
        }

        LOG.warn(
                "HTTP request failed: status={}, url={}",
                statusCode,
                url);

        throw new IOException(message);
    }

    private String readResponseBody(
            Response response,
            String url)
            throws IOException {

        ResponseBody responseBody =
                response.body();

        try {
            return responseBody == null
                    ? ""
                    : responseBody.string();

        } catch (IOException failure) {
            throw new ResponseReadException(
                    "Failed to read HTTP response body for URL: "
                            + url
                            + ". Data may have been partially received.",
                    failure);
        }
    }

    private void retry(
            int attempt,
            int maxAttempts,
            String reason,
            IOException failure) {

        long backoff =
                retryPolicy.backoffMillis(
                        attempt);

        LOG.warn(
                "{} (attempt {}/{}), retrying in {} ms: {}",
                reason,
                attempt,
                maxAttempts,
                backoff,
                failure.getMessage());

        sleep(backoff);
    }

    private static String truncate(
            String value,
            int maxLength) {

        if (value == null) {
            return "";
        }

        return value.length() <= maxLength
                ? value
                : value.substring(
                        0,
                        maxLength)
                        + "...";
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Response-body read failure. Retrying may duplicate downstream data. */
    static final class ResponseReadException
            extends IOException {

        ResponseReadException(
                String message,
                Throwable cause) {

            super(
                    message,
                    cause);
        }
    }

    /** HTTP response that is eligible for connector-configured retry. */
    static final class HttpRetryableException
            extends IOException {

        private final int statusCode;

        HttpRetryableException(
                int statusCode,
                String message) {

            super(message);
            this.statusCode = statusCode;
        }

        int getStatusCode() {
            return statusCode;
        }
    }
}
