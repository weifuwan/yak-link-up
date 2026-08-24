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

/** Mutable control-plane state for one stable Job and its execution attempts. */
public final class JobExecutionState {

    private final String jobId;
    private final JobSubmission submission;
    private final long createTimeMillis;
    private final List<JobStateTransition> transitions =
            new ArrayList<JobStateTransition>();
    private final List<JobExecutionAttempt> attempts =
            new ArrayList<JobExecutionAttempt>();

    private volatile long submittedTimeMillis;
    private volatile long queuedTimeMillis;
    private volatile long startTimeMillis;
    private volatile long endTimeMillis;
    private volatile long stateVersion;
    private volatile long checkpointVersion;
    private volatile ServerJobStatus status = ServerJobStatus.CREATED;
    private volatile boolean cancellationRequested;
    private volatile JobResult result;
    private volatile Throwable failure;
    private volatile String runId;
    private volatile String jobLogFile;

    public JobExecutionState(
            String jobId,
            JobSubmission submission) {
        this(jobId, submission, System.currentTimeMillis());
        this.attempts.add(new JobExecutionAttempt(this.jobId, 1));
        transitions.add(
                new JobStateTransition(
                        stateVersion,
                        null,
                        ServerJobStatus.CREATED,
                        createTimeMillis,
                        "job-created"));
    }

    private JobExecutionState(
            String jobId,
            JobSubmission submission,
            long createTimeMillis) {
        this.jobId = requireText(jobId, "jobId");
        this.submission = Objects.requireNonNull(
                submission,
                "submission must not be null");
        this.createTimeMillis = createTimeMillis;
    }

    /** Rehydrates terminal history and opens the next attempt for an approved retry. */
    public static JobExecutionState retryFrom(
            String jobId,
            JobSubmission submission,
            long createTimeMillis,
            long submittedTimeMillis,
            long stateVersion,
            long checkpointVersion,
            ServerJobStatus previousStatus,
            List<JobStateTransition> previousTransitions,
            List<JobExecutionAttempt> previousAttempts) {

        if (previousStatus != ServerJobStatus.FAILED) {
            throw new IllegalStateException(
                    "Retry state must originate from FAILED");
        }
        if (previousAttempts == null || previousAttempts.isEmpty()) {
            throw new IllegalArgumentException(
                    "Retry state requires previous attempts");
        }

        JobExecutionState state = new JobExecutionState(
                jobId,
                submission,
                createTimeMillis);
        state.submittedTimeMillis = submittedTimeMillis;
        state.stateVersion = stateVersion;
        state.checkpointVersion = checkpointVersion;
        state.status = previousStatus;
        state.transitions.addAll(previousTransitions);
        state.attempts.addAll(previousAttempts);
        state.beginRetry();
        return state;
    }

    private synchronized void beginRetry() {
        JobStateMachine.requireRetryTransition(
                status,
                ServerJobStatus.SUBMITTED);

        JobExecutionAttempt last = currentAttempt();
        if (!last.getStatus().isTerminal()) {
            throw new IllegalStateException(
                    "Previous attempt must be terminal before retry");
        }

        int nextAttemptNumber = last.getAttemptNumber() + 1;
        attempts.add(new JobExecutionAttempt(jobId, nextAttemptNumber));
        cancellationRequested = false;
        queuedTimeMillis = 0L;
        startTimeMillis = 0L;
        endTimeMillis = 0L;
        result = null;
        failure = null;
        runId = null;
        jobLogFile = null;

        retryTransition(
                ServerJobStatus.SUBMITTED,
                "retry-attempt-created");
    }

    public synchronized void markSubmitted() {
        submittedTimeMillis = System.currentTimeMillis();
        transition(ServerJobStatus.SUBMITTED, "submission-accepted");
    }

    public synchronized void markQueued() {
        currentAttempt().markQueued();
        queuedTimeMillis = System.currentTimeMillis();
        transition(ServerJobStatus.QUEUED, "worker-queue-accepted");
    }

    public synchronized boolean markRunning() {
        if (status != ServerJobStatus.QUEUED || cancellationRequested) {
            return false;
        }
        currentAttempt().markRunning();
        startTimeMillis = System.currentTimeMillis();
        transition(ServerJobStatus.RUNNING, "execution-started");
        return true;
    }

    public synchronized void bindLogIdentity(
            String runId,
            String jobLogFile) {
        this.runId = requireText(runId, "runId");
        this.jobLogFile = requireText(jobLogFile, "jobLogFile");
        currentAttempt().bindLogIdentity(this.runId, this.jobLogFile);
        checkpointVersion++;
    }

    public synchronized boolean requestCancellation() {
        if (status.isTerminal()) {
            throw new JobStateConflictException(jobId, status);
        }
        if (cancellationRequested) {
            return false;
        }
        cancellationRequested = true;
        checkpointVersion++;
        return true;
    }

    public synchronized boolean complete(
            ServerJobStatus finalStatus,
            JobResult result,
            Throwable failure) {

        if (finalStatus == null || !finalStatus.isTerminal()) {
            throw new IllegalArgumentException(
                    "finalStatus must be terminal");
        }
        if (status.isTerminal()) {
            return false;
        }

        currentAttempt().complete(finalStatus, result, failure);
        this.result = result;
        this.failure = failure;
        this.endTimeMillis = System.currentTimeMillis();
        transition(finalStatus, terminalReason(finalStatus, failure));
        return true;
    }

    /** Records a retry scheduling/admission failure before framework execution started. */
    public synchronized boolean failBeforeExecution(Throwable failure) {
        if (status.isTerminal()) {
            return false;
        }
        currentAttempt().completeBeforeExecution(failure);
        this.result = null;
        this.failure = failure;
        this.endTimeMillis = System.currentTimeMillis();
        transition(
                ServerJobStatus.FAILED,
                "execution-not-started:"
                        + (failure == null
                        ? "Unknown"
                        : failure.getClass().getSimpleName()));
        return true;
    }

    private JobExecutionAttempt currentAttempt() {
        return attempts.get(attempts.size() - 1);
    }

    private void transition(
            ServerJobStatus target,
            String reason) {
        ServerJobStatus previous = status;
        JobStateMachine.requireTransition(previous, target);
        appendTransition(previous, target, reason);
    }

    private void retryTransition(
            ServerJobStatus target,
            String reason) {
        ServerJobStatus previous = status;
        JobStateMachine.requireRetryTransition(previous, target);
        appendTransition(previous, target, reason);
    }

    private void appendTransition(
            ServerJobStatus previous,
            ServerJobStatus target,
            String reason) {
        stateVersion++;
        checkpointVersion++;
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

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    public String getJobId() { return jobId; }
    public String getJobName() { return submission.getDefinition().getName(); }
    public JobSubmission getSubmission() { return submission; }
    public JobDefinition getDefinition() { return submission.getDefinition(); }
    public long getCreateTimeMillis() { return createTimeMillis; }
    public long getSubmittedTimeMillis() { return submittedTimeMillis; }
    public long getQueuedTimeMillis() { return queuedTimeMillis; }
    public long getStartTimeMillis() { return startTimeMillis; }
    public long getEndTimeMillis() { return endTimeMillis; }
    public long getStateVersion() { return stateVersion; }
    public long getCheckpointVersion() { return checkpointVersion; }
    public ServerJobStatus getStatus() { return status; }
    public boolean isCancellationRequested() { return cancellationRequested; }
    public JobResult getResult() { return result; }
    public Throwable getFailure() { return failure; }
    public String getRunId() { return runId; }
    public String getJobLogFile() { return jobLogFile; }

    public synchronized List<JobStateTransition> getTransitions() {
        return Collections.unmodifiableList(
                new ArrayList<JobStateTransition>(transitions));
    }

    public synchronized List<JobExecutionAttempt> getAttempts() {
        return Collections.unmodifiableList(
                new ArrayList<JobExecutionAttempt>(attempts));
    }
}
