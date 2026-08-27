package com.link.up.framework.planning;

import com.link.up.api.exception.FluxErrorCategory;
import com.link.up.api.exception.FluxErrorCode;
import com.link.up.api.exception.FluxErrorPhase;
import com.link.up.api.exception.FluxRetryScope;

/** Stable machine-readable errors owned by the planning boundary. */
public enum PlanningErrorCode implements FluxErrorCode {

    INVALID_REQUEST(
            "PLAN-001",
            "Planning request is invalid",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.PROTOCOL,
            false,
            FluxRetryScope.NONE),

    INVALID_DEFINITION(
            "PLAN-002",
            "Job definition could not be compiled",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.COMPILE,
            false,
            FluxRetryScope.NONE),

    CONNECTOR_NOT_FOUND(
            "PLAN-003",
            "Connector could not be resolved",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.CONNECTOR_RESOLUTION,
            false,
            FluxRetryScope.NONE),

    CONNECTOR_OPTIONS_INVALID(
            "PLAN-004",
            "Connector options are invalid",
            FluxErrorCategory.VALIDATION,
            FluxErrorPhase.OPTION_VALIDATION,
            false,
            FluxRetryScope.NONE),

    REQUIRED_CAPABILITY_MISSING(
            "PLAN-005",
            "A required Connector capability is missing",
            FluxErrorCategory.CAPABILITY,
            FluxErrorPhase.CAPABILITY_NEGOTIATION,
            false,
            FluxRetryScope.NONE),

    SOURCE_PREPARATION_FAILED(
            "PLAN-006",
            "Source preparation or schema discovery failed",
            FluxErrorCategory.PREPARATION,
            FluxErrorPhase.SOURCE_DISCOVERY,
            true,
            FluxRetryScope.JOB),

    SPLIT_DISCOVERY_FAILED(
            "PLAN-007",
            "Source split discovery failed",
            FluxErrorCategory.DISCOVERY,
            FluxErrorPhase.SPLIT_DISCOVERY,
            true,
            FluxRetryScope.JOB),

    PHYSICAL_PLANNING_FAILED(
            "PLAN-008",
            "Physical Job planning failed",
            FluxErrorCategory.PLANNING,
            FluxErrorPhase.PHYSICAL_PLANNING,
            false,
            FluxRetryScope.NONE),

    SINK_PREPARATION_FAILED(
            "PLAN-009",
            "Sink preparation failed",
            FluxErrorCategory.PREPARATION,
            FluxErrorPhase.SINK_PREPARATION,
            false,
            FluxRetryScope.NONE),

    INTERNAL_FAILURE(
            "PLAN-010",
            "Unexpected planning failure",
            FluxErrorCategory.INTERNAL,
            FluxErrorPhase.PHYSICAL_PLANNING,
            false,
            FluxRetryScope.NONE);

    private final String code;
    private final String description;
    private final FluxErrorCategory category;
    private final FluxErrorPhase phase;
    private final boolean retryable;
    private final FluxRetryScope retryScope;

    PlanningErrorCode(
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
