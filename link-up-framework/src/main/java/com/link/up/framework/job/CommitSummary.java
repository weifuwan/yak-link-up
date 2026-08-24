package com.link.up.framework.job;

import com.link.up.api.sink.CommitScope;

import java.util.Objects;

/**
 * Immutable summary of task-local sink commit observations.
 */
public final class CommitSummary {

    private final int totalTaskCount;
    private final int finishedTaskCount;
    private final int committedTaskCount;
    private final int emptyCommittedTaskCount;
    private final int failedOrUncommittedTaskCount;

    private final long attemptedRecordCount;
    private final long successfullyWrittenRecordCount;
    private final long successfullyCommittedRecordCount;
    private final long failedRecordCount;
    private final long unknownStateRecordCount;

    private final CommitScope commitScope;
    private final String retryAdvice;

    public CommitSummary(
            int total,
            int finished,
            int committed,
            int empty,
            int failed,
            long attempted,
            long written,
            long committedRecords,
            long failedRecords,
            long unknown,
            CommitScope scope,
            String advice) {

        this.totalTaskCount = total;
        this.finishedTaskCount = finished;
        this.committedTaskCount = committed;
        this.emptyCommittedTaskCount = empty;
        this.failedOrUncommittedTaskCount = failed;
        this.attemptedRecordCount = attempted;
        this.successfullyWrittenRecordCount = written;
        this.successfullyCommittedRecordCount = committedRecords;
        this.failedRecordCount = failedRecords;
        this.unknownStateRecordCount = unknown;
        this.commitScope = Objects.requireNonNull(
                scope,
                "commitScope");
        this.retryAdvice = Objects.requireNonNull(
                advice,
                "retryAdvice");
    }

    /**
     * Compatibility constructor: committed tasks are conservatively treated as
     * empty commits because record-level evidence is unavailable.
     */
    public CommitSummary(
            int committed,
            int failed,
            CommitScope scope,
            String advice) {

        this(
                committed + failed,
                committed,
                committed,
                committed,
                failed,
                0L,
                0L,
                0L,
                0L,
                0L,
                scope,
                advice);
    }

    public static CommitSummary empty() {
        return new CommitSummary(
                0,
                0,
                0,
                0,
                0,
                0L,
                0L,
                0L,
                0L,
                0L,
                CommitScope.TASK_LOCAL,
                "No sink tasks were executed.");
    }

    public int getTotalTaskCount() {
        return totalTaskCount;
    }

    public int getFinishedTaskCount() {
        return finishedTaskCount;
    }

    public int getCommittedTaskCount() {
        return committedTaskCount;
    }

    public int getEmptyCommittedTaskCount() {
        return emptyCommittedTaskCount;
    }

    public int getDataCommittedTaskCount() {
        return committedTaskCount
                - emptyCommittedTaskCount;
    }

    public int getFailedOrUncommittedTaskCount() {
        return failedOrUncommittedTaskCount;
    }

    public long getAttemptedRecordCount() {
        return attemptedRecordCount;
    }

    public long getSuccessfullyWrittenRecordCount() {
        return successfullyWrittenRecordCount;
    }

    public long getSuccessfullyCommittedRecordCount() {
        return successfullyCommittedRecordCount;
    }

    public long getFailedRecordCount() {
        return failedRecordCount;
    }

    public long getUnknownStateRecordCount() {
        return unknownStateRecordCount;
    }

    public boolean isPartialTaskCommit() {
        return committedTaskCount > 0
                && failedOrUncommittedTaskCount > 0;
    }

    public boolean isPartialDataCommit() {
        return successfullyCommittedRecordCount > 0
                && failedOrUncommittedTaskCount > 0;
    }

    public boolean isPartialCommit() {
        return isPartialDataCommit();
    }

    public CommitScope getCommitScope() {
        return commitScope;
    }

    public String getRetryAdvice() {
        return retryAdvice;
    }

    public String getWarning() {
        if (!isPartialDataCommit()) {
            return "";
        }

        return "Partial data commit detected; task-local commits cannot be rolled back globally.";
    }
}
