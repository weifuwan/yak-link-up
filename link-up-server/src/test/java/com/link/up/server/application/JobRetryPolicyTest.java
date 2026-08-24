package com.link.up.server.application;

import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobRecoverySnapshotFactory;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.ServerJobStatus;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class JobRetryPolicyTest {

    private final JobRetryPolicy policy = new JobRetryPolicy();

    @Test
    public void shouldRejectLostOutcomeEvenWhenOldAttemptLooksEmpty() {
        JobSnapshot snapshot = snapshot(ServerJobStatus.LOST);
        JobRetryDecision decision = policy.evaluate(
                snapshot,
                metadata(attempt(
                        JobAttemptStatus.LOST,
                        true,
                        0,
                        0L,
                        0L,
                        false)));

        assertFalse(decision.isEligible());
        assertEquals(
                JobRetryDecision.LOST_OUTCOME_UNKNOWN,
                decision.getCode());
    }

    @Test
    public void shouldRejectOldCheckpointWithoutCommitEvidence() {
        JobSnapshot snapshot = snapshot(ServerJobStatus.FAILED);
        JobRetryDecision decision = policy.evaluate(
                snapshot,
                metadata(attempt(
                        JobAttemptStatus.FAILED,
                        false,
                        0,
                        0L,
                        0L,
                        false)));

        assertFalse(decision.isEligible());
        assertEquals(
                JobRetryDecision.EVIDENCE_UNAVAILABLE,
                decision.getCode());
    }

    @Test
    public void shouldRejectUnknownCommitState() {
        JobSnapshot snapshot = snapshot(ServerJobStatus.FAILED);
        JobRetryDecision decision = policy.evaluate(
                snapshot,
                metadata(attempt(
                        JobAttemptStatus.FAILED,
                        true,
                        0,
                        0L,
                        1L,
                        false)));

        assertFalse(decision.isEligible());
        assertEquals(
                JobRetryDecision.UNKNOWN_COMMIT_STATE,
                decision.getCode());
    }

    private static JobSnapshot snapshot(ServerJobStatus status) {
        return JobRecoverySnapshotFactory.restoreBasic(
                "retry-policy-job",
                "retry-policy",
                status,
                1L,
                2L,
                3L,
                status == ServerJobStatus.FAILED ? "FLUX-JOB-FAILED" : "FLUX-JOB-LOST",
                "test");
    }

    private static JobExecutionMetadata metadata(JobAttemptMetadata attempt) {
        return new JobExecutionMetadata(
                "external",
                "key",
                1,
                "digest",
                1L,
                1L,
                4L,
                4L,
                false,
                Collections.emptyList(),
                Collections.emptyMap(),
                null,
                null,
                Collections.singletonList(attempt));
    }

    private static JobAttemptMetadata attempt(
            JobAttemptStatus status,
            boolean evidence,
            int committedTasks,
            long committedRecords,
            long unknownRecords,
            boolean partialDataCommit) {
        return new JobAttemptMetadata(
                1,
                "retry-policy-job-attempt-1",
                status,
                1L,
                1L,
                2L,
                3L,
                null,
                null,
                null,
                null,
                "test advice",
                evidence,
                committedTasks,
                committedRecords,
                unknownRecords,
                partialDataCommit,
                "TASK_LOCAL");
    }
}
