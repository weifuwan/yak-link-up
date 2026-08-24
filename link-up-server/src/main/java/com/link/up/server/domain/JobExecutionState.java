package com.link.up.server.domain;

import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.server.runtime.JobStateConflictException;
import com.link.up.server.runtime.JobStateTransition;
import com.link.up.server.runtime.ServerJobStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Mutable control-plane state for one Worker job execution.
 *
 * <p>This aggregate owns lifecycle state, timestamps, cancellation intent and
 * terminal result only. It deliberately does not own Thread, Future,
 * ExecutorService, Semaphore, or framework JobExecution instances.</p>
 */
public final class JobExecutionState {

    private final String jobId;
    private final JobSubmission submission;
    private final long createTimeMillis;
    private final List<JobStateTransition> transitions =
            new ArrayList<JobStateTransition>();

    private volatile long submittedTimeMillis;
    private volatile long queuedTimeMillis;
    private volatile long startTimeMillis;
    private volatile long endTimeMillis;
    private volatile long stateVersion;
    private volatile ServerJobStatus status =
            ServerJobStatus.CREATED;
    private volatile boolean cancellationRequested;
    private volatile JobResult result;
    private volatile Throwable failure;
    private volatile String runId;
    private volatile String jobLogFile;

    public JobExecutionState(
            String jobId,
            JobSubmission submission) {

        this.jobId = requireText(jobId, "jobId");
        this.submission = Objects.requireNonNull(
                submission,
                "submission must not be null");
        this.createTimeMillis = System.currentTimeMillis();

        transitions.add(
                new JobStateTransition(
                        stateVersion,
                        null,
                        ServerJobStatus.CREATED,
                        createTimeMillis,
                        "job-created"));
    }

    public synchronized void markSubmitted() {
        submittedTimeMillis = System.currentTimeMillis();
        transition(
                ServerJobStatus.SUBMITTED,
                "submission-accepted");
    }

    public synchronized void markQueued() {
        queuedTimeMillis = System.currentTimeMillis();
        transition(
                ServerJobStatus.QUEUED,
                "worker-queue-accepted");
    }

    public synchronized boolean markRunning() {
        if (status != ServerJobStatus.QUEUED
                || cancellationRequested) {
            return false;
        }

        startTimeMillis = System.currentTimeMillis();
        transition(
                ServerJobStatus.RUNNING,
                "execution-started");
        return true;
    }

    public synchronized void bindLogIdentity(
            String runId,
            String jobLogFile) {
        this.runId = requireText(runId, "runId");
        this.jobLogFile = requireText(jobLogFile, "jobLogFile");
    }

    public synchronized boolean requestCancellation() {
        if (status.isTerminal()) {
            throw new JobStateConflictException(
                    jobId,
                    status);
        }
        if (cancellationRequested) {
            return false;
        }
        cancellationRequested = true;
        return true;
    }

    public synchronized boolean complete(
            ServerJobStatus finalStatus,
            JobResult result,
            Throwable failure) {

        if (finalStatus == null
                || !finalStatus.isTerminal()) {
            throw new IllegalArgumentException(
                    "finalStatus must be terminal");
        }
        if (status.isTerminal()) {
            return false;
        }

        this.result = result;
        this.failure = failure;
        this.endTimeMillis = System.currentTimeMillis();
        transition(
                finalStatus,
                terminalReason(
                        finalStatus,
                        failure));
        return true;
    }

    private void transition(
            ServerJobStatus target,
            String reason) {

        ServerJobStatus previous = status;
        JobStateMachine.requireTransition(previous, target);
        stateVersion++;
        status = target;
        transitions.add(
                new JobStateTransition(
                        stateVersion,
                        previous,
                        target,
                        System.currentTimeMillis(),
                        reason));
    }

    private static String terminalReason(
            ServerJobStatus status,
            Throwable failure) {
        if (status == ServerJobStatus.SUCCEEDED) {
            return "execution-succeeded";
        }
        if (status == ServerJobStatus.CANCELED) {
            return "cancellation-completed";
        }
        if (status == ServerJobStatus.LOST) {
            return "worker-lost-execution";
        }
        return failure == null
                ? "execution-failed"
                : "execution-failed:"
                + failure.getClass().getSimpleName();
    }

    private static String requireText(
            String value,
            String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return value.trim();
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return submission.getDefinition().getName();
    }

    public JobSubmission getSubmission() {
        return submission;
    }

    public JobDefinition getDefinition() {
        return submission.getDefinition();
    }

    public long getCreateTimeMillis() {
        return createTimeMillis;
    }

    public long getSubmittedTimeMillis() {
        return submittedTimeMillis;
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

    public long getStateVersion() {
        return stateVersion;
    }

    public ServerJobStatus getStatus() {
        return status;
    }

    public boolean isCancellationRequested() {
        return cancellationRequested;
    }

    public JobResult getResult() {
        return result;
    }

    public Throwable getFailure() {
        return failure;
    }

    public String getRunId() {
        return runId;
    }

    public String getJobLogFile() {
        return jobLogFile;
    }

    public synchronized List<JobStateTransition> getTransitions() {
        return Collections.unmodifiableList(
                new ArrayList<JobStateTransition>(
                        transitions));
    }
}
