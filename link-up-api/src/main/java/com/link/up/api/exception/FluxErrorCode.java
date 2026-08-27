package com.link.up.api.exception;

/**
 * Link-Up 统一错误码接口。
 *
 * <p>Existing error-code implementations remain source compatible. New code
 * should override the metadata methods so callers can make decisions without
 * matching exception messages.</p>
 */
public interface FluxErrorCode {

    String getCode();

    String getDescription();

    default FluxErrorCategory getCategory() {
        return FluxErrorCategory.UNKNOWN;
    }

    default FluxErrorPhase getPhase() {
        return FluxErrorPhase.UNKNOWN;
    }

    default boolean isRetryable() {
        return false;
    }

    default FluxRetryScope getRetryScope() {
        return FluxRetryScope.NONE;
    }

    default String getErrorMessage() {
        return String.format(
                "ErrorCode:[%s], ErrorDescription:[%s]",
                getCode(),
                getDescription());
    }
}
