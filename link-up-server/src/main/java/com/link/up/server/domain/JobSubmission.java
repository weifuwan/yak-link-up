package com.link.up.server.domain;

import com.link.up.framework.job.JobDefinition;

import java.util.Objects;
import java.util.UUID;

/**
 * Immutable command describing one Worker execution submission.
 */
public final class JobSubmission {

    private final String externalExecutionId;
    private final String idempotencyKey;
    private final int definitionVersion;
    private final String configDigest;
    private final JobDefinition definition;

    public JobSubmission(
            String externalExecutionId,
            String idempotencyKey,
            int definitionVersion,
            String configDigest,
            JobDefinition definition) {

        this.externalExecutionId =
                requireText(
                        externalExecutionId,
                        "externalExecutionId");
        this.idempotencyKey =
                requireText(
                        idempotencyKey,
                        "idempotencyKey");

        if (definitionVersion <= 0) {
            throw new IllegalArgumentException(
                    "definitionVersion must be greater than 0");
        }

        this.definitionVersion = definitionVersion;
        this.configDigest =
                requireText(
                        configDigest,
                        "configDigest");
        this.definition =
                Objects.requireNonNull(
                        definition,
                        "definition must not be null");
    }

    public static JobSubmission legacy(
            JobDefinition definition) {

        String token =
                UUID.randomUUID()
                        .toString();

        return new JobSubmission(
                "legacy-" + token,
                token,
                1,
                "legacy-" + token,
                definition);
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

    public String getExternalExecutionId() {
        return externalExecutionId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public int getDefinitionVersion() {
        return definitionVersion;
    }

    public String getConfigDigest() {
        return configDigest;
    }

    public JobDefinition getDefinition() {
        return definition;
    }
}
