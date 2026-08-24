package com.link.up.server.application;

import com.link.up.server.domain.JobSubmission;
import com.link.up.server.runtime.JobExecutionMetadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** In-process identity index for idempotent submission lookup. */
final class JobSubmissionRegistry {

    private final ConcurrentMap<String, String>
            jobIdsByIdempotencyKey =
            new ConcurrentHashMap<String, String>();
    private final ConcurrentMap<String, String>
            jobIdsByExternalExecutionId =
            new ConcurrentHashMap<String, String>();

    String lookup(JobSubmission submission) {
        String byIdempotency =
                jobIdsByIdempotencyKey.get(
                        submission.getIdempotencyKey());
        String byExternal =
                jobIdsByExternalExecutionId.get(
                        submission.getExternalExecutionId());

        if (byIdempotency == null && byExternal == null) {
            return null;
        }
        if (byIdempotency != null
                && byExternal != null
                && !byIdempotency.equals(byExternal)) {
            throw new JobSubmissionConflictException(
                    "idempotencyKey and externalExecutionId reference different jobs");
        }
        return byIdempotency != null
                ? byIdempotency
                : byExternal;
    }

    void register(
            String jobId,
            JobSubmission submission) {
        register(
                jobId,
                submission.getIdempotencyKey(),
                submission.getExternalExecutionId());
    }

    void restore(
            String jobId,
            JobExecutionMetadata metadata) {
        if (metadata == null) {
            return;
        }
        register(
                jobId,
                metadata.getIdempotencyKey(),
                metadata.getExternalExecutionId());
    }

    private void register(
            String jobId,
            String idempotencyKey,
            String externalExecutionId) {

        String existingByIdempotency =
                jobIdsByIdempotencyKey.putIfAbsent(
                        idempotencyKey,
                        jobId);
        if (existingByIdempotency != null
                && !existingByIdempotency.equals(jobId)) {
            throw new JobSubmissionConflictException(
                    "idempotencyKey already belongs to job "
                            + existingByIdempotency);
        }

        String existingByExternal =
                jobIdsByExternalExecutionId.putIfAbsent(
                        externalExecutionId,
                        jobId);
        if (existingByExternal != null
                && !existingByExternal.equals(jobId)) {
            jobIdsByIdempotencyKey.remove(
                    idempotencyKey,
                    jobId);
            throw new JobSubmissionConflictException(
                    "externalExecutionId already belongs to job "
                            + existingByExternal);
        }
    }

    void unregister(
            String jobId,
            JobSubmission submission) {
        jobIdsByIdempotencyKey.remove(
                submission.getIdempotencyKey(),
                jobId);
        jobIdsByExternalExecutionId.remove(
                submission.getExternalExecutionId(),
                jobId);
    }

    String findByExternalExecutionId(
            String externalExecutionId) {
        return jobIdsByExternalExecutionId.get(
                externalExecutionId);
    }
}
