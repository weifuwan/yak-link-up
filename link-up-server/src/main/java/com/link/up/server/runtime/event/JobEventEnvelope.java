package com.link.up.server.runtime.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** Versioned, job-scoped lifecycle envelope persisted as one JSON line. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobEventEnvelope {

    /**
     * v2 removes the terminal Pipeline/Task execution snapshot from new events.
     * v1 remains readable; unknown legacy event payload fields are ignored.
     */
    public static final int CURRENT_SCHEMA_VERSION = 2;

    private final int schemaVersion;
    private final String eventId;
    private final String jobId;
    private final String attemptId;
    private final int attemptNumber;
    private final long sequence;
    private final long occurredAtMillis;
    private final JobRuntimeEvent event;

    @JsonCreator
    public JobEventEnvelope(
            @JsonProperty("schemaVersion") int schemaVersion,
            @JsonProperty("eventId") String eventId,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("attemptId") String attemptId,
            @JsonProperty("attemptNumber") int attemptNumber,
            @JsonProperty("sequence") long sequence,
            @JsonProperty("occurredAtMillis") long occurredAtMillis,
            @JsonProperty("event") JobRuntimeEvent event) {

        if (schemaVersion <= 0) {
            throw new IllegalArgumentException(
                    "schemaVersion must be greater than 0");
        }
        if (attemptNumber <= 0) {
            throw new IllegalArgumentException(
                    "attemptNumber must be greater than 0");
        }
        if (sequence <= 0L) {
            throw new IllegalArgumentException(
                    "sequence must be greater than 0");
        }
        if (occurredAtMillis <= 0L) {
            throw new IllegalArgumentException(
                    "occurredAtMillis must be greater than 0");
        }

        this.schemaVersion = schemaVersion;
        this.eventId = requireText(eventId, "eventId");
        this.jobId = requireText(jobId, "jobId");
        this.attemptId = requireText(attemptId, "attemptId");
        this.attemptNumber = attemptNumber;
        this.sequence = sequence;
        this.occurredAtMillis = occurredAtMillis;
        this.event = Objects.requireNonNull(
                event,
                "event must not be null");
    }

    public static JobEventEnvelope create(
            String jobId,
            String attemptId,
            int attemptNumber,
            long sequence,
            long occurredAtMillis,
            JobRuntimeEvent event) {

        String normalizedJobId = requireText(
                jobId,
                "jobId");

        return new JobEventEnvelope(
                CURRENT_SCHEMA_VERSION,
                normalizedJobId + ":" + sequence,
                normalizedJobId,
                attemptId,
                attemptNumber,
                sequence,
                occurredAtMillis,
                event);
    }

    private static String requireText(
            String value,
            String name) {

        if (value == null
                || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return value.trim();
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public String getEventId() {
        return eventId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public long getSequence() {
        return sequence;
    }

    public long getOccurredAtMillis() {
        return occurredAtMillis;
    }

    public JobRuntimeEvent getEvent() {
        return event;
    }
}
