package com.link.up.server.domain;

import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.JobResult;
import com.link.up.server.runtime.ServerJobStatus;

/**
 * Domain state for one concrete execution attempt.
 *
 * <p>An attempt contains no thread, future, scheduler or framework execution
 * handle. Phase 7 records one attempt; later retry policy may append more
 * attempts without changing the job identity.</p>
 */
public final class JobExecutionAttempt {

    private final int attemptNumber;
    private final String attemptId;
    private final long createTimeMillis;

    private volatile JobAttemptStatus status =
            JobAttemptStatus.CREATED;
    private volatile long queuedTimeMillis;
    private volatile long startTimeMillis;
    private volatile long endTimeMillis;
    private volatile String runId;
    private volatile String jobLogFile;
    private volatile String failureType;
    private volatile String failureMessage;
    private volatile String retryAdvice;

    JobExecutionAttempt(
            String jobId,
            int attemptNumber) {

        if (attemptNumber <= 0) {
            throw new IllegalArgumentException(
                    "attemptNumber must be greater than 0");
        }
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "jobId must not be blank");
        }

        this.attemptNumber = attemptNumber;
        this.attemptId =
                jobId.trim()
                        + "-attempt-"
                        + attemptNumber;
        this.createTimeMillis =
                System.currentTimeMillis();
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

        Throwable terminalFailure =
                failure != null
                        ? failure
                        : result == null
                        ? null
                        : result.getFailure();

        if (terminalFailure != null) {
            failureType =
                    terminalFailure.getClass()
                            .getSimpleName();
            failureMessage = safeMessage(
                    terminalFailure);
        }

        CommitSummary summary =
                result == null
                        ? null
                        : result.getCommitSummary();
        if (summary != null) {
            retryAdvice = normalize(
                    summary.getRetryAdvice());
        }
    }

    private void requireStatus(JobAttemptStatus expected) {
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

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public JobAttemptStatus getStatus() {
        return status;
    }

    public long getCreateTimeMillis() {
        return createTimeMillis;
    }

    public long getQueuedTimeMillis() {
        return queuedTimeMillis;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public long getEndTimeMillis() {
        return endTimeMillis;
    }

    public String getRunId() {
        return runId;
    }

    public String getJobLogFile() {
        return jobLogFile;
    }

    public String getFailureType() {
        return failureType;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public String getRetryAdvice() {
        return retryAdvice;
    }
}
