package com.link.up.server.dto;

import com.link.up.api.exception.FluxErrorCategory;
import com.link.up.api.exception.FluxErrorPhase;
import com.link.up.api.exception.FluxRetryScope;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Stable REST error response with additive structured metadata. */
public final class ErrorResponse {

    private final String code;
    private final String message;
    private final String requestId;
    private final FluxErrorCategory category;
    private final FluxErrorPhase phase;
    private final Boolean retryable;
    private final FluxRetryScope retryScope;
    private final Map<String, String> parameters;

    public ErrorResponse(
            String code,
            String message,
            String requestId) {

        this(
                code,
                message,
                requestId,
                null,
                null,
                null,
                null,
                null);
    }

    public ErrorResponse(
            String code,
            String message,
            String requestId,
            FluxErrorCategory category,
            FluxErrorPhase phase,
            Boolean retryable,
            FluxRetryScope retryScope,
            Map<String, String> parameters) {

        this.code = code;
        this.message = message;
        this.requestId = requestId;
        this.category = category;
        this.phase = phase;
        this.retryable = retryable;
        this.retryScope = retryScope;
        this.parameters = immutable(parameters);
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public String getRequestId() {
        return requestId;
    }

    public FluxErrorCategory getCategory() {
        return category;
    }

    public FluxErrorPhase getPhase() {
        return phase;
    }

    public Boolean getRetryable() {
        return retryable;
    }

    public FluxRetryScope getRetryScope() {
        return retryScope;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    private static Map<String, String> immutable(
            Map<String, String> values) {

        if (values == null || values.isEmpty()) {
            return null;
        }

        return Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(values));
    }
}
