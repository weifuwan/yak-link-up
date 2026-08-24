package com.link.up.connector.http.client;

import com.link.up.connector.http.config.HttpMethod;
import com.link.up.connector.http.config.HttpSourceConfig;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/** Retry decisions for one HTTP source client. */
final class HttpRetryPolicy {

    private final HttpSourceConfig config;

    HttpRetryPolicy(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(
                config,
                "config must not be null");
    }

    int maxAttempts() {
        return Math.max(
                1,
                config.getRetry() + 1);
    }

    boolean isMethodRetryable() {
        return config.getMethod() == HttpMethod.GET;
    }

    boolean isStatusRetryable(int statusCode) {
        return config.getRetryableStatusCodes()
                .contains(statusCode);
    }

    long backoffMillis(int attempt) {
        long multiplier =
                config.getRetryBackoffMultiplierMs();

        long exponential =
                attempt >= 63
                        ? Long.MAX_VALUE
                        : safeMultiply(
                                multiplier,
                                1L << Math.max(0, attempt - 1));

        long base =
                Math.min(
                        exponential,
                        config.getRetryBackoffMaxMs());

        int jitterMax =
                config.getRetryJitterMs();

        if (jitterMax <= 0) {
            return base;
        }

        return base
                + ThreadLocalRandom.current()
                .nextLong(
                        0L,
                        (long) jitterMax + 1L);
    }

    private static long safeMultiply(
            long left,
            long right) {

        if (left == 0L || right == 0L) {
            return 0L;
        }

        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }

        return left * right;
    }
}
