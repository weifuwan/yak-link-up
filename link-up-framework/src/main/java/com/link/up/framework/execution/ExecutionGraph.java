package com.link.up.framework.execution;

import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.planner.JobGraph;

import java.util.Objects;

/**
 * Mutable runtime state for one execution of an immutable JobGraph.
 *
 * <p>The physical graph is planner-owned and immutable. Cancellation, metrics,
 * timestamps, status, failure, and the terminal result are execution-owned
 * state and therefore live in this object.
 */
public final class ExecutionGraph {

    private final JobGraph jobGraph;
    private final CancellationToken cancellationToken =
            new CancellationToken();
    private final JobMetrics metrics =
            new JobMetrics();
    private final long startTimeMillis;
    private final String runId;
    private final String jobLogFile;

    private volatile JobStatus status = JobStatus.CREATED;
    private volatile long endTimeMillis;
    private volatile Throwable failure;
    private volatile JobResult result;

    public ExecutionGraph(
            JobGraph jobGraph,
            long startTimeMillis,
            String runId,
            String jobLogFile) {

        this.jobGraph = Objects.requireNonNull(
                jobGraph,
                "jobGraph must not be null");

        if (startTimeMillis < 0L) {
            throw new IllegalArgumentException(
                    "startTimeMillis must not be negative");
        }

        this.startTimeMillis = startTimeMillis;
        this.runId = requireText(runId, "runId");
        this.jobLogFile = requireText(
                jobLogFile,
                "jobLogFile");
    }

    public synchronized void markRunning() {
        if (status != JobStatus.CREATED) {
            throw new IllegalStateException(
                    "ExecutionGraph cannot start from status "
                            + status);
        }
        status = JobStatus.RUNNING;
    }

    public synchronized void complete(JobResult result) {
        Objects.requireNonNull(
                result,
                "result must not be null");

        if (status != JobStatus.RUNNING) {
            throw new IllegalStateException(
                    "ExecutionGraph cannot complete from status "
                            + status);
        }

        JobStatus finalStatus = result.getStatus();
        if (!isTerminal(finalStatus)) {
            throw new IllegalArgumentException(
                    "result status must be terminal: "
                            + finalStatus);
        }

        if (!jobGraph.getJobName().equals(result.getJobName())) {
            throw new IllegalArgumentException(
                    "result jobName does not match JobGraph: "
                            + result.getJobName());
        }

        if (result.getStartTimeMillis() != startTimeMillis) {
            throw new IllegalArgumentException(
                    "result startTimeMillis does not match ExecutionGraph");
        }

        if (result.getEndTimeMillis() < startTimeMillis) {
            throw new IllegalArgumentException(
                    "result endTimeMillis must not precede execution start");
        }

        this.result = result;
        status = finalStatus;
        endTimeMillis = result.getEndTimeMillis();
        failure = result.getFailure();
    }

    synchronized void fail(Throwable cause) {
        if (isTerminal(status)) {
            return;
        }

        failure = Objects.requireNonNull(
                cause,
                "cause must not be null");
        endTimeMillis = System.currentTimeMillis();
        status = cause instanceof java.util.concurrent.CancellationException
                ? JobStatus.CANCELED
                : JobStatus.FAILED;
    }

    synchronized void requestCancellation(Throwable cause) {
        if (isTerminal(status)) {
            return;
        }
        cancellationToken.cancel(cause);
    }

    CancellationToken getCancellationToken() {
        return cancellationToken;
    }

    public JobGraph getJobGraph() {
        return jobGraph;
    }

    public JobMetrics getMetrics() {
        return metrics;
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

    public JobStatus getStatus() {
        return status;
    }

    public Throwable getFailure() {
        return failure;
    }

    public JobResult getResult() {
        return result;
    }

    public boolean isCancellationRequested() {
        return cancellationToken.isCancelled();
    }

    public boolean isTerminal() {
        return isTerminal(status);
    }

    private static boolean isTerminal(JobStatus value) {
        return value == JobStatus.SUCCEEDED
                || value == JobStatus.FAILED
                || value == JobStatus.CANCELED;
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
}
