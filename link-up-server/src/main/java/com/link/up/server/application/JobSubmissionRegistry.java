package com.link.up.server.application;

import com.link.up.server.domain.JobSubmission;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-process identity index for idempotent submission lookup.
 *
 * <p>The registry owns only index mechanics. Content equivalence is checked by
 * the application service against persisted execution metadata.</p>
 */
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

        String existingByIdempotency =
                jobIdsByIdempotencyKey.putIfAbsent(
                        submission.getIdempotencyKey(),
                        jobId);

        if (existingByIdempotency != null) {
            throw new JobSubmissionConflictException(
                    "idempotencyKey already belongs to job "
                            + existingByIdempotency);
        }

        String existingByExternal =
                jobIdsByExternalExecutionId.putIfAbsent(
                        submission.getExternalExecutionId(),
                        jobId);

        if (existingByExternal != null) {
            jobIdsByIdempotencyKey.remove(
                    submission.getIdempotencyKey(),
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
