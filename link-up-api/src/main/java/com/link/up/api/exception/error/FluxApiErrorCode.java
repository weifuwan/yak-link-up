package com.link.up.api.exception.error;

import com.link.up.api.exception.FluxErrorCategory;
import com.link.up.api.exception.FluxErrorCode;
import com.link.up.api.exception.FluxErrorPhase;
import com.link.up.api.exception.FluxRetryScope;

/** Stable error directory for Connector API and metadata boundaries. */
public enum FluxApiErrorCode implements FluxErrorCode {

    CONFIG_VALIDATION_FAILED(
            "API-01",
            "Configuration item validation failed",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.OPTION_VALIDATION,
            false,
            FluxRetryScope.NONE),

    OPTION_VALIDATION_FAILED(
            "API-02",
            "Option item validation failed",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.OPTION_VALIDATION,
            false,
            FluxRetryScope.NONE),

    CATALOG_INITIALIZE_FAILED(
            "API-03",
            "Catalog initialization failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.SOURCE_DISCOVERY,
            true,
            FluxRetryScope.JOB),

    DATABASE_NOT_EXISTED(
            "API-04",
            "Database does not exist",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.SOURCE_DISCOVERY,
            false,
            FluxRetryScope.NONE),

    TABLE_NOT_EXISTED(
            "API-05",
            "Table does not exist",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.SOURCE_DISCOVERY,
            false,
            FluxRetryScope.NONE),

    FACTORY_INITIALIZE_FAILED(
            "API-06",
            "Factory initialization failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.CONNECTOR_RESOLUTION,
            false,
            FluxRetryScope.NONE),

    DATABASE_ALREADY_EXISTED(
            "API-07",
            "Database already exists",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.SINK_PREPARATION,
            false,
            FluxRetryScope.NONE),

    TABLE_ALREADY_EXISTED(
            "API-08",
            "Table already exists",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.SINK_PREPARATION,
            false,
            FluxRetryScope.NONE),

    HANDLE_SAVE_MODE_FAILED(
            "API-09",
            "Save-mode handling failed",
            FluxErrorCategory.PREPARATION,
            FluxErrorPhase.SINK_PREPARATION,
            false,
            FluxRetryScope.NONE),

    SOURCE_ALREADY_HAS_DATA(
            "API-10",
            "The target data source already has data",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.SINK_PREPARATION,
            false,
            FluxRetryScope.NONE),

    SINK_TABLE_NOT_EXIST(
            "API-11",
            "The Sink table does not exist",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.SINK_PREPARATION,
            false,
            FluxRetryScope.NONE),

    LIST_DATABASES_FAILED(
            "API-12",
            "Listing databases failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.SOURCE_DISCOVERY,
            true,
            FluxRetryScope.JOB),

    LIST_TABLES_FAILED(
            "API-13",
            "Listing tables failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.SOURCE_DISCOVERY,
            true,
            FluxRetryScope.JOB),

    GET_PRIMARY_KEY_FAILED(
            "API-14",
            "Reading primary-key metadata failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.SOURCE_DISCOVERY,
            true,
            FluxRetryScope.JOB),

    METADATA_PROVIDER_INITIALIZE_FAILED(
            "API-15",
            "Metadata provider initialization failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.SOURCE_DISCOVERY,
            true,
            FluxRetryScope.JOB),

    CONNECTOR_INITIALIZE_FAILED(
            "API-16",
            "Connector initialization failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.CONNECTOR_RESOLUTION,
            false,
            FluxRetryScope.NONE);

    private final String code;
    private final String description;
    private final FluxErrorCategory category;
    private final FluxErrorPhase phase;
    private final boolean retryable;
    private final FluxRetryScope retryScope;

    FluxApiErrorCode(
            String code,
            String description,
            FluxErrorCategory category,
            FluxErrorPhase phase,
            boolean retryable,
            FluxRetryScope retryScope) {

        this.code = code;
        this.description = description;
        this.category = category;
        this.phase = phase;
        this.retryable = retryable;
        this.retryScope = retryScope;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public FluxErrorCategory getCategory() {
        return category;
    }

    @Override
    public FluxErrorPhase getPhase() {
        return phase;
    }

    @Override
    public boolean isRetryable() {
        return retryable;
    }

    @Override
    public FluxRetryScope getRetryScope() {
        return retryScope;
    }
}
