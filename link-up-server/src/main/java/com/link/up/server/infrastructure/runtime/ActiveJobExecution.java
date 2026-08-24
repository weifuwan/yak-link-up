package com.link.up.server.infrastructure.runtime;

import com.link.up.framework.execution.JobExecution;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.server.application.port.JobRuntimeScheduler;

import java.util.Objects;

/** Runtime-only binding between one scheduled job, its thread and execution. */
final class ActiveJobExecution {

    private final String jobId;
    private final JobRuntimeScheduler.Listener listener;

    private volatile Thread thread;
    private volatile JobExecution execution;
    private volatile boolean cancellationRequested;

    ActiveJobExecution(
            String jobId,
            JobRuntimeScheduler.Listener listener) {

        this.jobId = requireText(
                jobId,
                "jobId");
        this.listener = Objects.requireNonNull(
                listener,
                "listener must not be null");
    }

    String getJobId() {
        return jobId;
    }

    JobRuntimeScheduler.Listener getListener() {
        return listener;
    }

    synchronized void bindThread(Thread thread) {
        this.thread = Objects.requireNonNull(
                thread,
                "thread must not be null");

        if (cancellationRequested) {
            thread.interrupt();
        }
    }

    synchronized void bindExecution(JobExecution execution) {
        this.execution = Objects.requireNonNull(
                execution,
                "execution must not be null");

        if (cancellationRequested) {
            execution.cancel();
        }
    }

    synchronized void cancel() {
        cancellationRequested = true;

        JobExecution currentExecution = execution;
        if (currentExecution != null) {
            currentExecution.cancel();
        }

        Thread currentThread = thread;
        if (currentThread != null) {
            currentThread.interrupt();
        }
    }

    synchronized void clearExecution() {
        execution = null;
    }

    boolean isCancellationRequested() {
        return cancellationRequested;
    }

    Thread getThread() {
        return thread;
    }

    JobMetrics getMetrics() {
        JobExecution currentExecution = execution;

        return currentExecution == null
                ? null
                : currentExecution.getMetrics();
    }

    private static String requireText(
            String value,
            String name) {

        if (value == null
                || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }

        return value.trim();
    }
}
