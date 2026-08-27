package com.link.up.server.application;

import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.ServerJobStatus;

import java.util.List;

/** Pure application policy for safe manual retry. */
final class JobRetryPolicy {

    JobRetryDecision evaluate(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata) {

        int nextAttempt = metadata == null
                ? 1
                : metadata.getAttemptCount() + 1;
        ServerJobStatus status = snapshot.getStatus();

        if (!status.isTerminal()) {
            return JobRetryDecision.deny(
                    JobRetryDecision.JOB_ACTIVE,
                    "The Job is still active and cannot be retried.",
                    nextAttempt);
        }
        if (status == ServerJobStatus.SUCCEEDED) {
            return JobRetryDecision.deny(
                    JobRetryDecision.ALREADY_SUCCEEDED,
                    "A succeeded Job must not be retried.",
                    nextAttempt);
        }
        if (status == ServerJobStatus.CANCELED) {
            return JobRetryDecision.deny(
                    JobRetryDecision.CANCELED_OUTCOME,
                    "Cancellation may race with Sink commits; retry is not automatically safe.",
                    nextAttempt);
        }
        if (status == ServerJobStatus.LOST) {
            return JobRetryDecision.deny(
                    JobRetryDecision.LOST_OUTCOME_UNKNOWN,
                    "LOST means the final Sink outcome is unknown.",
                    nextAttempt);
        }
        if (status != ServerJobStatus.FAILED
                || metadata == null) {
            return evidenceUnavailable(nextAttempt);
        }

        List<JobAttemptMetadata> attempts =
                metadata.getAttempts();
        if (attempts.isEmpty()) {
            return evidenceUnavailable(nextAttempt);
        }

        JobAttemptMetadata last =
                attempts.get(attempts.size() - 1);
        if (last.getStatus() != JobAttemptStatus.FAILED) {
            return evidenceUnavailable(nextAttempt);
        }

        if (last.getErrorCode() != null
                && !last.isFailureRetryable()) {
            return JobRetryDecision.deny(
                    JobRetryDecision.NON_RETRYABLE_FAILURE,
                    "The previous attempt failed with non-retryable structured error "
                            + last.getErrorCode()
                            + ".",
                    nextAttempt);
        }

        if (!last.isCommitEvidenceAvailable()) {
            return evidenceUnavailable(nextAttempt);
        }
        if (last.getUnknownStateRecordCount() > 0L) {
            return JobRetryDecision.deny(
                    JobRetryDecision.UNKNOWN_COMMIT_STATE,
                    "The previous attempt contains records with unknown Sink commit state.",
                    nextAttempt);
        }
        if (last.isPartialDataCommit()
                || last.getDataCommittedTaskCount() > 0
                || last.getSuccessfullyCommittedRecordCount() > 0L) {
            return JobRetryDecision.deny(
                    JobRetryDecision.DATA_ALREADY_COMMITTED,
                    "The previous attempt confirmed committed data; automatic replay is unsafe.",
                    nextAttempt);
        }

        return JobRetryDecision.allow(
                JobRetryDecision.SAFE_NO_DATA_COMMITTED,
                "The previous failed attempt has commit evidence showing no committed or unknown data.",
                nextAttempt);
    }

    private JobRetryDecision evidenceUnavailable(
            int nextAttempt) {
        return JobRetryDecision.deny(
                JobRetryDecision.EVIDENCE_UNAVAILABLE,
                "Commit evidence is unavailable, so retry safety cannot be proven.",
                nextAttempt);
    }
}
