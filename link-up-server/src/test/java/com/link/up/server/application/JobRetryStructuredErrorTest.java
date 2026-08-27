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
import static org.junit.Assert.assertTrue;

public class JobRetryStructuredErrorTest {

    private final JobRetryPolicy policy =
            new JobRetryPolicy();

    @Test
    public void nonRetryableStructuredFailureMustBlockReplay() {
        JobRetryDecision decision = policy.evaluate(
                snapshot(),
                metadata(
                        attempt(
                                "PLAN-005",
                                "CAPABILITY",
                                "CAPABILITY_NEGOTIATION",
                                false,
                                "NONE",
                                0L)));

        assertFalse(decision.isEligible());
        assertEquals(
                JobRetryDecision.NON_RETRYABLE_FAILURE,
                decision.getCode());
    }

    @Test
    public void retryableStructuredFailureStillRequiresSafeCommitEvidence() {
        JobRetryDecision safe = policy.evaluate(
                snapshot(),
                metadata(
                        attempt(
                                "PLAN-006",
                                "PREPARATION",
                                "SOURCE_DISCOVERY",
                                true,
                                "JOB",
                                0L)));
        JobRetryDecision unknown = policy.evaluate(
                snapshot(),
                metadata(
                        attempt(
                                "PLAN-006",
                                "PREPARATION",
                                "SOURCE_DISCOVERY",
                                true,
                                "JOB",
                                1L)));

        assertTrue(safe.isEligible());
        assertEquals(
                JobRetryDecision.SAFE_NO_DATA_COMMITTED,
                safe.getCode());
        assertFalse(unknown.isEligible());
        assertEquals(
                JobRetryDecision.UNKNOWN_COMMIT_STATE,
                unknown.getCode());
    }

    private JobSnapshot snapshot() {
        return JobRecoverySnapshotFactory.restoreBasic(
                "structured-retry-job",
                "structured-retry",
                ServerJobStatus.FAILED,
                1L,
                2L,
                3L,
                "FLUX-JOB-FAILED",
                "test");
    }

    private JobExecutionMetadata metadata(
            JobAttemptMetadata attempt) {

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

    private JobAttemptMetadata attempt(
            String errorCode,
            String errorCategory,
            String errorPhase,
            boolean retryable,
            String retryScope,
            long unknownRecords) {

        return new JobAttemptMetadata(
                1,
                "structured-retry-job-attempt-1",
                JobAttemptStatus.FAILED,
                1L,
                1L,
                2L,
                3L,
                null,
                null,
                "PlanningException",
                "safe message",
                "test advice",
                errorCode,
                errorCategory,
                errorPhase,
                retryable,
                retryScope,
                true,
                0,
                0L,
                unknownRecords,
                false,
                "TASK_LOCAL");
    }
}
