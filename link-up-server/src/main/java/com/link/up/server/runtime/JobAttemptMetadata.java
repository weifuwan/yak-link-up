package com.link.up.server.runtime;

import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.domain.JobExecutionAttempt;

import java.util.Objects;

/** Stable Worker read model for one execution attempt. */
public final class JobAttemptMetadata {

    private final int attemptNumber;
    private final String attemptId;
    private final JobAttemptStatus status;
    private final long createTimeMillis;
    private final long queuedTimeMillis;
    private final long startTimeMillis;
    private final long endTimeMillis;
    private final String runId;
    private final String jobLogFile;
    private final String failureType;
    private final String failureMessage;
    private final String retryAdvice;

    private final boolean commitEvidenceAvailable;
    private final int dataCommittedTaskCount;
    private final long successfullyCommittedRecordCount;
    private final long unknownStateRecordCount;
    private final boolean partialDataCommit;
    private final String commitScope;

    public JobAttemptMetadata(
            int attemptNumber,
            String attemptId,
            JobAttemptStatus status,
            long createTimeMillis,
            long queuedTimeMillis,
            long startTimeMillis,
            long endTimeMillis,
            String runId,
            String jobLogFile,
            String failureType,
            String failureMessage,
            String retryAdvice) {
        this(
                attemptNumber,
                attemptId,
                status,
                createTimeMillis,
                queuedTimeMillis,
                startTimeMillis,
                endTimeMillis,
                runId,
                jobLogFile,
                failureType,
                failureMessage,
                retryAdvice,
                false,
                0,
                0L,
                0L,
                false,
                null);
    }

    public JobAttemptMetadata(
            int attemptNumber,
            String attemptId,
            JobAttemptStatus status,
            long createTimeMillis,
            long queuedTimeMillis,
            long startTimeMillis,
            long endTimeMillis,
            String runId,
            String jobLogFile,
            String failureType,
            String failureMessage,
            String retryAdvice,
            boolean commitEvidenceAvailable,
            int dataCommittedTaskCount,
            long successfullyCommittedRecordCount,
            long unknownStateRecordCount,
            boolean partialDataCommit,
            String commitScope) {

        this.attemptNumber = attemptNumber;
        this.attemptId = Objects.requireNonNull(attemptId, "attemptId must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.createTimeMillis = createTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.runId = runId;
        this.jobLogFile = jobLogFile;
        this.failureType = failureType;
        this.failureMessage = failureMessage;
        this.retryAdvice = retryAdvice;
        this.commitEvidenceAvailable = commitEvidenceAvailable;
        this.dataCommittedTaskCount = dataCommittedTaskCount;
        this.successfullyCommittedRecordCount = successfullyCommittedRecordCount;
        this.unknownStateRecordCount = unknownStateRecordCount;
        this.partialDataCommit = partialDataCommit;
        this.commitScope = commitScope;
    }

    public static JobAttemptMetadata from(JobExecutionAttempt attempt) {
        return new JobAttemptMetadata(
                attempt.getAttemptNumber(),
                attempt.getAttemptId(),
                attempt.getStatus(),
                attempt.getCreateTimeMillis(),
                attempt.getQueuedTimeMillis(),
                attempt.getStartTimeMillis(),
                attempt.getEndTimeMillis(),
                attempt.getRunId(),
                attempt.getJobLogFile(),
                attempt.getFailureType(),
                attempt.getFailureMessage(),
                attempt.getRetryAdvice(),
                attempt.isCommitEvidenceAvailable(),
                attempt.getDataCommittedTaskCount(),
                attempt.getSuccessfullyCommittedRecordCount(),
                attempt.getUnknownStateRecordCount(),
                attempt.isPartialDataCommit(),
                attempt.getCommitScope());
    }

    public JobExecutionAttempt toDomain() {
        return JobExecutionAttempt.restore(
                attemptNumber,
                attemptId,
                status,
                createTimeMillis,
                queuedTimeMillis,
                startTimeMillis,
                endTimeMillis,
                runId,
                jobLogFile,
                failureType,
                failureMessage,
                retryAdvice,
                commitEvidenceAvailable,
                dataCommittedTaskCount,
                successfullyCommittedRecordCount,
                unknownStateRecordCount,
                partialDataCommit,
                commitScope);
    }

    public JobAttemptMetadata recoverLost(
            long endTimeMillis,
            String reason) {

        if (status.isTerminal()) {
            return this;
        }

        return new JobAttemptMetadata(
                attemptNumber,
                attemptId,
                JobAttemptStatus.LOST,
                createTimeMillis,
                queuedTimeMillis,
                startTimeMillis,
                endTimeMillis,
                runId,
                jobLogFile,
                "WorkerRestartRecovery",
                reason,
                retryAdvice,
                false,
                dataCommittedTaskCount,
                successfullyCommittedRecordCount,
                unknownStateRecordCount,
                partialDataCommit,
                commitScope);
    }

    public int getAttemptNumber() { return attemptNumber; }
    public String getAttemptId() { return attemptId; }
    public JobAttemptStatus getStatus() { return status; }
    public long getCreateTimeMillis() { return createTimeMillis; }
    public long getQueuedTimeMillis() { return queuedTimeMillis; }
    public long getStartTimeMillis() { return startTimeMillis; }
    public long getEndTimeMillis() { return endTimeMillis; }
    public String getRunId() { return runId; }
    public String getJobLogFile() { return jobLogFile; }
    public String getFailureType() { return failureType; }
    public String getFailureMessage() { return failureMessage; }
    public String getRetryAdvice() { return retryAdvice; }
    public boolean isCommitEvidenceAvailable() { return commitEvidenceAvailable; }
    public int getDataCommittedTaskCount() { return dataCommittedTaskCount; }
    public long getSuccessfullyCommittedRecordCount() { return successfullyCommittedRecordCount; }
    public long getUnknownStateRecordCount() { return unknownStateRecordCount; }
    public boolean isPartialDataCommit() { return partialDataCommit; }
    public String getCommitScope() { return commitScope; }
}
