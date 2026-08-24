package com.link.up.server.infrastructure.runtime;

import com.link.up.framework.execution.JobExecution;
import com.link.up.framework.execution.JobExecutionListener;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.server.application.port.JobExecutor;
import com.link.up.server.application.port.JobRuntimeScheduler;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Local Worker scheduler that owns admission, job threads and framework
 * execution bindings.
 *
 * <p>This adapter intentionally preserves the existing single-node admission
 * semantics: one permit is held for each accepted queued/running job.</p>
 */
public final class LocalJobRuntimeScheduler
        implements JobRuntimeScheduler {

    private final int maxActiveJobs;
    private final Semaphore admissionSemaphore;
    private final long shutdownTimeoutMillis;
    private final JobExecutor jobExecutor;
    private final ConcurrentMap<String, ActiveJobExecution> activeJobs =
            new ConcurrentHashMap<String, ActiveJobExecution>();
    private final AtomicInteger threadSequence =
            new AtomicInteger();
    private final AtomicBoolean closed =
            new AtomicBoolean(false);

    public LocalJobRuntimeScheduler(
            int maxActiveJobs,
            long shutdownTimeoutMillis,
            JobExecutor jobExecutor) {

        if (maxActiveJobs <= 0) {
            throw new IllegalArgumentException(
                    "maxActiveJobs must be greater than 0");
        }

        this.maxActiveJobs = maxActiveJobs;
        this.shutdownTimeoutMillis =
                Math.max(0L, shutdownTimeoutMillis);
        this.jobExecutor = Objects.requireNonNull(
                jobExecutor,
                "jobExecutor must not be null");
        this.admissionSemaphore =
                new Semaphore(maxActiveJobs);
    }

    @Override
    public void schedule(
            final String jobId,
            final JobDefinition definition,
            final Listener listener) {

        ensureOpen();
        requireText(jobId, "jobId");
        Objects.requireNonNull(
                definition,
                "definition must not be null");
        Objects.requireNonNull(
                listener,
                "listener must not be null");

        if (!admissionSemaphore.tryAcquire()) {
            throw new RejectedExecutionException(
                    "Job queue is full (maxQueuedJobs="
                            + maxActiveJobs
                            + ")");
        }

        final ActiveJobExecution active =
                new ActiveJobExecution(jobId, listener);

        ActiveJobExecution previous =
                activeJobs.putIfAbsent(jobId, active);

        if (previous != null) {
            admissionSemaphore.release();
            throw new IllegalStateException(
                    "Duplicate active jobId: " + jobId);
        }

        try {
            listener.onQueued();

            Thread thread =
                    new Thread(
                            new Runnable() {
                                @Override
                                public void run() {
                                    runJob(
                                            active,
                                            definition);
                                }
                            },
                            "link-up-job-"
                                    + jobId
                                    + "-"
                                    + threadSequence.incrementAndGet());
            thread.setDaemon(false);
            active.bindThread(thread);
            thread.start();
        } catch (RuntimeException failure) {
            activeJobs.remove(jobId, active);
            admissionSemaphore.release();
            throw failure;
        }
    }

    private void runJob(
            final ActiveJobExecution active,
            JobDefinition definition) {

        try {
            if (!active.listener.onStarting()) {
                active.listener.onCompleted(
                        null,
                        null,
                        true);
                return;
            }

            JobResult result =
                    jobExecutor.execute(
                            definition,
                            new JobExecutionListener() {
                                @Override
                                public void onJobLogCreated(
                                        String runId,
                                        String jobLogFile) {
                                    active.listener.onJobLogCreated(
                                            runId,
                                            jobLogFile);
                                }

                                @Override
                                public void onJobExecutionCreated(
                                        JobExecution execution) {
                                    active.bindExecution(execution);
                                }
                            });

            active.listener.onCompleted(
                    result,
                    null,
                    false);

        } catch (Throwable failure) {
            boolean cancellationLike =
                    active.isCancellationRequested()
                            || failure instanceof CancellationException
                            || failure instanceof InterruptedException;

            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            active.listener.onCompleted(
                    null,
                    failure,
                    cancellationLike);

            if (failure instanceof Error) {
                throw (Error) failure;
            }
        } finally {
            active.clearExecution();
            activeJobs.remove(
                    active.jobId,
                    active);
            admissionSemaphore.release();
        }
    }

    @Override
    public void cancel(String jobId) {
        requireText(jobId, "jobId");
        ActiveJobExecution active =
                activeJobs.get(jobId);
        if (active != null) {
            active.cancel();
        }
    }

    @Override
    public JobMetrics getMetrics(String jobId) {
        ActiveJobExecution active =
                activeJobs.get(jobId);
        return active == null
                ? null
                : active.getMetrics();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        for (ActiveJobExecution active : activeJobs.values()) {
            active.cancel();
        }

        long deadline =
                System.currentTimeMillis()
                        + shutdownTimeoutMillis;

        for (ActiveJobExecution active : activeJobs.values()) {
            Thread thread = active.getThread();
            if (thread == null) {
                continue;
            }

            long remaining =
                    deadline - System.currentTimeMillis();
            if (remaining <= 0L) {
                break;
            }

            try {
                thread.join(remaining);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        for (ActiveJobExecution active : activeJobs.values()) {
            active.listener.onLost();
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Job runtime scheduler is closed");
        }
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

    /** Runtime-only binding for a scheduled job. */
    private static final class ActiveJobExecution {
        private final String jobId;
        private final Listener listener;
        private volatile Thread thread;
        private volatile JobExecution execution;
        private volatile boolean cancellationRequested;

        private ActiveJobExecution(
                String jobId,
                Listener listener) {
            this.jobId = jobId;
            this.listener = listener;
        }

        private synchronized void bindThread(
                Thread thread) {
            this.thread = Objects.requireNonNull(
                    thread,
                    "thread must not be null");
            if (cancellationRequested) {
                thread.interrupt();
            }
        }

        private synchronized void bindExecution(
                JobExecution execution) {
            this.execution = Objects.requireNonNull(
                    execution,
                    "execution must not be null");
            if (cancellationRequested) {
                execution.cancel();
            }
        }

        private synchronized void cancel() {
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

        private synchronized void clearExecution() {
            execution = null;
        }

        private boolean isCancellationRequested() {
            return cancellationRequested;
        }

        private Thread getThread() {
            return thread;
        }

        private JobMetrics getMetrics() {
            JobExecution currentExecution = execution;
            return currentExecution == null
                    ? null
                    : currentExecution.getMetrics();
        }
    }
}
