package com.link.up.connector.http.source;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable request values for one HTTP source page. */
final class HttpPageRequest {

    private final Map<String, String> headers;
    private final Map<String, String> params;
    private final String body;

    HttpPageRequest(
            Map<String, String> headers,
            Map<String, String> params,
            String body) {

        this.headers = immutable(headers, "headers");
        this.params = immutable(params, "params");
        this.body = body;
    }

    Map<String, String> getHeaders() {
        return headers;
    }

    Map<String, String> getParams() {
        return params;
    }

    String getBody() {
        return body;
    }

    private static Map<String, String> immutable(
            Map<String, String> values,
            String name) {

        Objects.requireNonNull(values, name + " must not be null");
        return Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(values));
    }
}
