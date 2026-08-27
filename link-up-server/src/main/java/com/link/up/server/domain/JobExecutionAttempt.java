package com.link.up.server.domain;

import com.link.up.api.exception.FluxRuntimeException;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.JobResult;
import com.link.up.server.runtime.ServerJobStatus;

/** Pure domain state for one concrete execution attempt. */
public final class JobExecutionAttempt {

    private final int attemptNumber;
    private final String attemptId;
    private final long createTimeMillis;

    private volatile JobAttemptStatus status;
    private volatile long queuedTimeMillis;
    private volatile long startTimeMillis;
    private volatile long endTimeMillis;
    private volatile String runId;
    private volatile String jobLogFile;
    private volatile String failureType;
    private volatile String failureMessage;
    private volatile String retryAdvice;

    private volatile String errorCode;
    private volatile String errorCategory;
    private volatile String errorPhase;
    private volatile boolean failureRetryable;
    private volatile String failureRetryScope;

    private volatile boolean commitEvidenceAvailable;
    private volatile int dataCommittedTaskCount;
    private volatile long successfullyCommittedRecordCount;
    private volatile long unknownStateRecordCount;
    private volatile boolean partialDataCommit;
    private volatile String commitScope;

    JobExecutionAttempt(
            String jobId,
            int attemptNumber) {

        if (attemptNumber <= 0) {
            throw new IllegalArgumentException(
                    "attemptNumber must be greater than 0");
        }

        String normalizedJobId =
                requireText(jobId, "jobId");
        this.attemptNumber = attemptNumber;
        this.attemptId = normalizedJobId
                + "-attempt-"
                + attemptNumber;
        this.createTimeMillis =
                System.currentTimeMillis();
        this.status = JobAttemptStatus.CREATED;
    }

    private JobExecutionAttempt(
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
            String errorCode,
            String errorCategory,
            String errorPhase,
            boolean failureRetryable,
            String failureRetryScope,
            boolean commitEvidenceAvailable,
            int dataCommittedTaskCount,
            long successfullyCommittedRecordCount,
            long unknownStateRecordCount,
            boolean partialDataCommit,
            String commitScope) {

        this.attemptNumber = attemptNumber;
        this.attemptId = requireText(
                attemptId,
                "attemptId");
        this.status = status;
        this.createTimeMillis = createTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.runId = runId;
        this.jobLogFile = jobLogFile;
        this.failureType = failureType;
        this.failureMessage = failureMessage;
        this.retryAdvice = retryAdvice;
        this.errorCode = errorCode;
        this.errorCategory = errorCategory;
        this.errorPhase = errorPhase;
        this.failureRetryable = failureRetryable;
        this.failureRetryScope = failureRetryScope;
        this.commitEvidenceAvailable =
                commitEvidenceAvailable;
        this.dataCommittedTaskCount =
                dataCommittedTaskCount;
        this.successfullyCommittedRecordCount =
                successfullyCommittedRecordCount;
        this.unknownStateRecordCount =
                unknownStateRecordCount;
        this.partialDataCommit = partialDataCommit;
        this.commitScope = commitScope;
    }

    /** Compatibility restore overload for persisted format v1/v2. */
    public static JobExecutionAttempt restore(
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

        return restore(
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
                null,
                null,
                null,
                false,
                null,
                commitEvidenceAvailable,
                dataCommittedTaskCount,
                successfullyCommittedRecordCount,
                unknownStateRecordCount,
                partialDataCommit,
                commitScope);
    }

    public static JobExecutionAttempt restore(
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
            String errorCode,
            String errorCategory,
            String errorPhase,
            boolean failureRetryable,
            String failureRetryScope,
            boolean commitEvidenceAvailable,
            int dataCommittedTaskCount,
            long successfullyCommittedRecordCount,
            long unknownStateRecordCount,
            boolean partialDataCommit,
            String commitScope) {

        if (attemptNumber <= 0 || status == null) {
            throw new IllegalArgumentException(
                    "Invalid restored attempt");
        }

        return new JobExecutionAttempt(
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
                errorCode,
                errorCategory,
                errorPhase,
                failureRetryable,
                failureRetryScope,
                commitEvidenceAvailable,
                dataCommittedTaskCount,
                successfullyCommittedRecordCount,
                unknownStateRecordCount,
                partialDataCommit,
                commitScope);
    }

    synchronized void markQueued() {
        requireStatus(JobAttemptStatus.CREATED);
        queuedTimeMillis = System.currentTimeMillis();
        status = JobAttemptStatus.QUEUED;
    }

    synchronized void markRunning() {
        requireStatus(JobAttemptStatus.QUEUED);
        startTimeMillis = System.currentTimeMillis();
        status = JobAttemptStatus.RUNNING;
    }

    synchronized void bindLogIdentity(
            String runId,
            String jobLogFile) {

        this.runId = requireText(runId, "runId");
        this.jobLogFile = requireText(
                jobLogFile,
                "jobLogFile");
    }

    synchronized void complete(
            ServerJobStatus finalStatus,
            JobResult result,
            Throwable failure) {

        if (status.isTerminal()) {
            return;
        }

        status = attemptStatus(finalStatus);
        endTimeMillis = System.currentTimeMillis();
        recordFailure(
                failure != null
                        ? failure
                        : result == null
                        ? null
                        : result.getFailure());

        CommitSummary summary = result == null
                ? null
                : result.getCommitSummary();

        if (summary != null) {
            commitEvidenceAvailable = true;
            dataCommittedTaskCount =
                    summary.getDataCommittedTaskCount();
            successfullyCommittedRecordCount =
                    summary.getSuccessfullyCommittedRecordCount();
            unknownStateRecordCount =
                    summary.getUnknownStateRecordCount();
            partialDataCommit =
                    summary.isPartialDataCommit();
            commitScope =
                    summary.getCommitScope().name();
            retryAdvice = normalize(
                    summary.getRetryAdvice());
        }
    }

    synchronized void completeBeforeExecution(
            Throwable failure) {

        if (status.isTerminal()) {
            return;
        }

        status = JobAttemptStatus.FAILED;
        endTimeMillis = System.currentTimeMillis();
        recordFailure(failure);
        commitEvidenceAvailable = true;
        dataCommittedTaskCount = 0;
        successfullyCommittedRecordCount = 0L;
        unknownStateRecordCount = 0L;
        partialDataCommit = false;
        commitScope = null;
        retryAdvice =
                "Execution did not start; replay is safe.";
    }

    private void recordFailure(Throwable failure) {
        if (failure == null) {
            return;
        }

        failureType = failure.getClass().getSimpleName();
        failureMessage = safeMessage(failure);

        FluxRuntimeException structured =
                structuredFailure(failure);
        if (structured == null) {
            return;
        }

        errorCode = structured.getFluxErrorCode().getCode();
        errorCategory = structured.getErrorCategory().name();
        errorPhase = structured.getErrorPhase().name();
        failureRetryable = structured.isRetryable();
        failureRetryScope =
                structured.getRetryScope().name();
    }

    private FluxRuntimeException structuredFailure(
            Throwable failure) {

        Throwable current = failure;
        int depth = 0;

        while (current != null && depth < 20) {
            if (current instanceof FluxRuntimeException) {
                return (FluxRuntimeException) current;
            }
            current = current.getCause();
            depth++;
        }

        return null;
    }

    private void requireStatus(
            JobAttemptStatus expected) {

        if (status != expected) {
            throw new IllegalStateException(
                    "Illegal attempt state transition: "
                            + status
                            + " -> expected "
                            + expected);
        }
    }

    private static JobAttemptStatus attemptStatus(
            ServerJobStatus status) {

        if (status == ServerJobStatus.SUCCEEDED) {
            return JobAttemptStatus.SUCCEEDED;
        }
        if (status == ServerJobStatus.CANCELED) {
            return JobAttemptStatus.CANCELED;
        }
        if (status == ServerJobStatus.LOST) {
            return JobAttemptStatus.LOST;
        }
        return JobAttemptStatus.FAILED;
    }

    private static String requireText(
            String value,
            String name) {

        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return normalized;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty()
                ? null
                : normalized;
    }

    private static String safeMessage(Throwable failure) {
        String message = normalize(failure.getMessage());
        if (message == null) {
            message = failure.getClass().getSimpleName();
        }
        message = message.replace('\r', ' ')
                .replace('\n', ' ');
        return message.length() <= 500
                ? message
                : message.substring(0, 500);
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
    public String getErrorCode() { return errorCode; }
    public String getErrorCategory() { return errorCategory; }
    public String getErrorPhase() { return errorPhase; }
    public boolean isFailureRetryable() { return failureRetryable; }
    public String getFailureRetryScope() { return failureRetryScope; }
    public boolean isCommitEvidenceAvailable() { return commitEvidenceAvailable; }
    public int getDataCommittedTaskCount() { return dataCommittedTaskCount; }
    public long getSuccessfullyCommittedRecordCount() { return successfullyCommittedRecordCount; }
    public long getUnknownStateRecordCount() { return unknownStateRecordCount; }
    public boolean isPartialDataCommit() { return partialDataCommit; }
    public String getCommitScope() { return commitScope; }
}
