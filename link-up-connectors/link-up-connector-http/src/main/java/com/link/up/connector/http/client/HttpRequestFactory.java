package com.link.up.connector.http.client;

import com.link.up.connector.http.config.HttpMethod;
import com.link.up.connector.http.config.HttpSourceConfig;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Builds one OkHttp request from immutable source config and page overrides. */
final class HttpRequestFactory {

    private static final MediaType JSON_MEDIA_TYPE =
            MediaType.parse(
                    "application/json; charset=utf-8");

    private final HttpSourceConfig config;

    HttpRequestFactory(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(
                config,
                "config must not be null");
    }

    Request create(
            Map<String, String> effectiveHeaders,
            Map<String, String> effectiveParams,
            String effectiveBody) {

        String url =
                buildUrl(effectiveParams);

        Request.Builder builder =
                new Request.Builder()
                        .url(url);

        applyHeaders(
                builder,
                effectiveHeaders);

        if (config.getMethod() == HttpMethod.POST) {
            builder.post(
                    requestBody(
                            effectiveHeaders,
                            effectiveParams,
                            effectiveBody));
        } else {
            builder.get();
        }

        return builder.build();
    }

    private void applyHeaders(
            Request.Builder builder,
            Map<String, String> headers) {

        if (headers == null) {
            return;
        }

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.header(
                    entry.getKey(),
                    entry.getValue());
        }
    }

    private RequestBody requestBody(
            Map<String, String> headers,
            Map<String, String> params,
            String effectiveBody) {

        String body =
                effectiveBody != null
                        ? effectiveBody
                        : config.getBody();

        if (body == null || body.isEmpty()) {
            return RequestBody.create(
                    "{}",
                    JSON_MEDIA_TYPE);
        }

        if ("application/x-www-form-urlencoded"
                .equalsIgnoreCase(
                        contentType(headers))) {
            return formBody(
                    body,
                    params);
        }

        return RequestBody.create(
                body,
                JSON_MEDIA_TYPE);
    }

    private String buildUrl(
            Map<String, String> effectiveParams) {

        StringBuilder url =
                new StringBuilder(
                        config.getUrl());

        Map<String, String> params =
                new LinkedHashMap<String, String>();

        if (config.getParams() != null) {
            params.putAll(
                    config.getParams());
        }

        if (effectiveParams != null) {
            params.putAll(
                    effectiveParams);
        }

        if (params.isEmpty()) {
            return url.toString();
        }

        char separator =
                config.getUrl().contains("?")
                        ? '&'
                        : '?';

        for (Map.Entry<String, String> entry : params.entrySet()) {
            url.append(separator)
                    .append(
                            encode(
                                    entry.getKey()))
                    .append('=')
                    .append(
                            encode(
                                    entry.getValue()));

            separator = '&';
        }

        return url.toString();
    }

    private RequestBody formBody(
            String body,
            Map<String, String> params) {

        FormBody.Builder builder =
                new FormBody.Builder();

        for (String pair : body.split("&")) {
            int separator =
                    pair.indexOf('=');

            if (separator <= 0) {
                continue;
            }

            builder.add(
                    pair.substring(
                            0,
                            separator)
                            .trim(),
                    pair.substring(
                            separator + 1)
                            .trim());
        }

        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                builder.add(
                        entry.getKey(),
                        entry.getValue());
            }
        }

        return builder.build();
    }

    private static String contentType(
            Map<String, String> headers) {

        if (headers == null) {
            return null;
        }

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if ("content-type"
                    .equalsIgnoreCase(
                            entry.getKey())) {
                return entry.getValue();
            }
        }

        return null;
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(
                    value,
                    "UTF-8");
        } catch (Exception ignored) {
            return value;
        }
    }
}
