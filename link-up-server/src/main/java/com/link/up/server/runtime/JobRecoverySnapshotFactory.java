package com.link.up.server.runtime;

import java.util.Collections;

/** Helpers for restoring persisted control-plane checkpoints after restart. */
public final class JobRecoverySnapshotFactory {

    private JobRecoverySnapshotFactory() {
    }

    public static JobSnapshot restoreBasic(
            String jobId,
            String jobName,
            ServerJobStatus status,
            long createTimeMillis,
            long startTimeMillis,
            long endTimeMillis,
            String errorCode,
            String errorMessage) {

        return new JobSnapshot(
                jobId,
                jobName,
                status,
                createTimeMillis,
                startTimeMillis,
                endTimeMillis,
                duration(startTimeMillis, endTimeMillis),
                emptyMetrics(),
                emptyCommit(),
                Collections.<JobSnapshot.Pipeline>emptyList(),
                errorCode,
                errorMessage);
    }

    public static JobSnapshot recoverLost(
            JobSnapshot checkpoint,
            long recoveryTimeMillis,
            String reason) {

        if (checkpoint.getStatus().isTerminal()) {
            return checkpoint;
        }

        return new JobSnapshot(
                checkpoint.getJobId(),
                checkpoint.getJobName(),
                ServerJobStatus.LOST,
                checkpoint.getCreateTimeMillis(),
                checkpoint.getStartTimeMillis(),
                recoveryTimeMillis,
                duration(
                        checkpoint.getStartTimeMillis(),
                        recoveryTimeMillis),
                checkpoint.getMetrics(),
                checkpoint.getCommitSummary(),
                checkpoint.getPipelines(),
                "FLUX-JOB-LOST",
                reason);
    }

    private static JobSnapshot.Metrics emptyMetrics() {
        return new JobSnapshot.Metrics(
                0L, 0L, 0L, 0D,
                0L, 0L, 0L, 0L, 0D,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L,
                0L, 0L,
                0D, 0D, 0D);
    }

    private static JobSnapshot.Commit emptyCommit() {
        return new JobSnapshot.Commit(
                0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L,
                false,
                false,
                null,
                null);
    }

    private static long duration(
            long startTimeMillis,
            long endTimeMillis) {
        if (startTimeMillis <= 0L
                || endTimeMillis <= 0L) {
            return 0L;
        }
        return Math.max(
                0L,
                endTimeMillis - startTimeMillis);
    }
}
