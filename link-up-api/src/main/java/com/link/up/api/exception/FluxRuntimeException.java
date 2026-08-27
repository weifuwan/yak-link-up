package com.link.up.api.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Flux global exception, used to tell user more clearly error messages.
 *
 * <p>Structured metadata is delegated to {@link FluxErrorCode}; legacy error
 * codes automatically retain UNKNOWN/NONE defaults.</p>
 */
public class FluxRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper();

    private final FluxErrorCode fluxErrorCode;
    private final Map<String, String> params;

    public FluxRuntimeException(
            FluxErrorCode fluxErrorCode,
            String errorMessage) {

        super(fluxErrorCode.getErrorMessage()
                + " - "
                + errorMessage);
        this.fluxErrorCode = fluxErrorCode;
        this.params = new HashMap<String, String>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                fluxErrorCode.getDescription(),
                params);
    }

    public FluxRuntimeException(
            FluxErrorCode fluxErrorCode,
            String errorMessage,
            Throwable cause) {

        super(
                fluxErrorCode.getErrorMessage()
                        + " - "
                        + errorMessage,
                cause);
        this.fluxErrorCode = fluxErrorCode;
        this.params = new HashMap<String, String>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                fluxErrorCode.getDescription(),
                params);
    }

    public FluxRuntimeException(
            FluxErrorCode fluxErrorCode,
            Throwable cause) {

        super(fluxErrorCode.getErrorMessage(), cause);
        this.fluxErrorCode = fluxErrorCode;
        this.params = new HashMap<String, String>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                fluxErrorCode.getDescription(),
                params);
    }

    public FluxRuntimeException(
            FluxErrorCode fluxErrorCode,
            Map<String, String> params) {

        super(ExceptionParamsUtil.getDescription(
                fluxErrorCode.getErrorMessage(),
                params));
        this.fluxErrorCode = fluxErrorCode;
        this.params = params;
    }

    public FluxRuntimeException(
            FluxErrorCode fluxErrorCode,
            Map<String, String> params,
            Throwable cause) {

        super(
                ExceptionParamsUtil.getDescription(
                        fluxErrorCode.getErrorMessage(),
                        params),
                cause);
        this.fluxErrorCode = fluxErrorCode;
        this.params = params;
    }

    public FluxErrorCode getFluxErrorCode() {
        return fluxErrorCode;
    }

    public FluxErrorCategory getErrorCategory() {
        return fluxErrorCode.getCategory();
    }

    public FluxErrorPhase getErrorPhase() {
        return fluxErrorCode.getPhase();
    }

    public boolean isRetryable() {
        return fluxErrorCode.isRetryable();
    }

    public FluxRetryScope getRetryScope() {
        return fluxErrorCode.getRetryScope();
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Map<String, String> getParamsValueAsMap(String key) {
        try {
            return OBJECT_MAPPER.readValue(
                    params.get(key),
                    new TypeReference<Map<String, String>>() {
                    });
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException(
                    "Could not decode exception parameter '"
                            + key
                            + "'",
                    failure);
        }
    }

    public <T> T getParamsValueAs(String key) {
        try {
            return OBJECT_MAPPER.readValue(
                    params.get(key),
                    new TypeReference<T>() {
                    });
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException(
                    "Could not decode exception parameter '"
                            + key
                            + "'",
                    failure);
        }
    }
}
