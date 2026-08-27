package com.link.up.server.runtime.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.link.up.server.runtime.ServerJobStatus;

import java.util.Objects;

/**
 * Secret-safe lifecycle fact stored inside a {@link JobEventEnvelope}.
 *
 * <p>The optional execution payload is emitted only from durable checkpoints
 * and contains a bounded, typed projection of Pipeline/Task facts. Connector
 * options, arbitrary payload maps and Throwable messages remain excluded.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobRuntimeEvent {

    private final JobRuntimeEventType type;
    private final ServerJobStatus previousStatus;
    private final ServerJobStatus status;
    private final String reason;
    private final String runId;
    private final String failureType;
    private final JobExecutionFacts execution;

    public JobRuntimeEvent(
            JobRuntimeEventType type,
            ServerJobStatus previousStatus,
            ServerJobStatus status,
            String reason,
            String runId,
            String failureType) {
        this(
                type,
                previousStatus,
                status,
                reason,
                runId,
                failureType,
                null);
    }

    @JsonCreator
    public JobRuntimeEvent(
            @JsonProperty("type") JobRuntimeEventType type,
            @JsonProperty("previousStatus") ServerJobStatus previousStatus,
            @JsonProperty("status") ServerJobStatus status,
            @JsonProperty("reason") String reason,
            @JsonProperty("runId") String runId,
            @JsonProperty("failureType") String failureType,
            @JsonProperty("execution") JobExecutionFacts execution) {

        this.type = Objects.requireNonNull(
                type,
                "type must not be null");
        this.previousStatus = previousStatus;
        this.status = status;
        this.reason = safeOptionalText(reason, 200);
        this.runId = safeOptionalText(runId, 200);
        this.failureType = safeOptionalText(failureType, 300);
        this.execution = execution;
    }

    public static JobRuntimeEvent transition(
            JobRuntimeEventType type,
            ServerJobStatus previousStatus,
            ServerJobStatus status,
            String reason) {

        if (isTerminalType(type)) {
            throw new IllegalArgumentException(
                    "Use terminal(...) for terminal Job events");
        }

        return new JobRuntimeEvent(
                type,
                previousStatus,
                status,
                reason,
                null,
                null,
                null);
    }

    public static JobRuntimeEvent logCreated(
            ServerJobStatus status,
            String runId) {

        return new JobRuntimeEvent(
                JobRuntimeEventType.JOB_LOG_CREATED,
                status,
                status,
                "job-log-created",
                requireText(runId, "runId"),
                null,
                null);
    }

    public static JobRuntimeEvent cancellationRequested(
            ServerJobStatus status) {

        return new JobRuntimeEvent(
                JobRuntimeEventType.JOB_CANCEL_REQUESTED,
                status,
                status,
                "cancellation-requested",
                null,
                null,
                null);
    }

    public static JobRuntimeEvent terminal(
            JobRuntimeEventType type,
            ServerJobStatus previousStatus,
            ServerJobStatus status,
            String reason,
            String failureType) {

        return terminal(
                type,
                previousStatus,
                status,
                reason,
                failureType,
                null);
    }

    public static JobRuntimeEvent terminal(
            JobRuntimeEventType type,
            ServerJobStatus previousStatus,
            ServerJobStatus status,
            String reason,
            String failureType,
            JobExecutionFacts execution) {

        if (!isTerminalType(type)) {
            throw new IllegalArgumentException(
                    "type must describe a terminal Job event");
        }

        return new JobRuntimeEvent(
                type,
                previousStatus,
                status,
                reason,
                null,
                failureType,
                execution);
    }

    private static boolean isTerminalType(
            JobRuntimeEventType type) {

        return type == JobRuntimeEventType.JOB_SUCCEEDED
                || type == JobRuntimeEventType.JOB_FAILED
                || type == JobRuntimeEventType.JOB_CANCELED
                || type == JobRuntimeEventType.JOB_LOST;
    }

    private static String safeOptionalText(
            String value,
            int maxLength) {

        if (value == null) {
            return null;
        }

        String normalized = value.trim()
                .replace('\r', ' ')
                .replace('\n', ' ')
                .replace('\t', ' ');

        if (normalized.isEmpty()) {
            return null;
        }

        return normalized.length() <= maxLength
                ? normalized
                : normalized.substring(0, maxLength);
    }

    private static String requireText(
            String value,
            String name) {

        String normalized = safeOptionalText(value, 500);
        if (normalized == null) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return normalized;
    }

    public JobRuntimeEventType getType() { return type; }
    public ServerJobStatus getPreviousStatus() { return previousStatus; }
    public ServerJobStatus getStatus() { return status; }
    public String getReason() { return reason; }
    public String getRunId() { return runId; }
    public String getFailureType() { return failureType; }
    public JobExecutionFacts getExecution() { return execution; }
}
