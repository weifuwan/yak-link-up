package com.link.up.server.infrastructure.runtime;

import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.server.application.port.JobExecutor;
import com.link.up.server.application.port.JobRuntimeScheduler;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Local Worker scheduler that owns admission, job threads and shutdown.
 *
 * <p>The accepted job's Thread/Framework execution binding belongs to
 * {@link ActiveJobExecution}; invoking the Framework belongs to
 * {@link LocalJobRunner}.</p>
 */
public final class LocalJobRuntimeScheduler
        implements JobRuntimeScheduler {

    private final int maxActiveJobs;
    private final Semaphore admissionSemaphore;
    private final long shutdownTimeoutMillis;
    private final LocalJobRunner jobRunner;
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
                Math.max(
                        0L,
                        shutdownTimeoutMillis);
        this.jobRunner =
                new LocalJobRunner(
                        Objects.requireNonNull(
                                jobExecutor,
                                "jobExecutor must not be null"));
        this.admissionSemaphore =
                new Semaphore(maxActiveJobs);
    }

    @Override
    public void schedule(
            final String jobId,
            final JobDefinition definition,
            final Listener listener) {

        ensureOpen();

        String normalizedJobId =
                requireText(
                        jobId,
                        "jobId");

        Objects.requireNonNull(
                definition,
                "definition must not be null");
        Objects.requireNonNull(
                listener,
                "listener must not be null");

        acquireAdmission();

        final ActiveJobExecution active =
                new ActiveJobExecution(
                        normalizedJobId,
                        listener);

        ActiveJobExecution previous =
                activeJobs.putIfAbsent(
                        normalizedJobId,
                        active);

        if (previous != null) {
            admissionSemaphore.release();
            throw new IllegalStateException(
                    "Duplicate active jobId: "
                            + normalizedJobId);
        }

        try {
            listener.onQueued();

            Thread thread =
                    createJobThread(
                            active,
                            definition);

            active.bindThread(thread);
            thread.start();

        } catch (RuntimeException failure) {
            activeJobs.remove(
                    normalizedJobId,
                    active);
            admissionSemaphore.release();
            throw failure;
        }
    }

    @Override
    public void cancel(String jobId) {
        String normalizedJobId =
                requireText(
                        jobId,
                        "jobId");

        ActiveJobExecution active =
                activeJobs.get(normalizedJobId);

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

        cancelActiveJobs();
        awaitActiveJobs();
        notifyLostJobs();
    }

    private void acquireAdmission() {
        if (admissionSemaphore.tryAcquire()) {
            return;
        }

        throw new RejectedExecutionException(
                "Job queue is full (maxQueuedJobs="
                        + maxActiveJobs
                        + ")");
    }

    private Thread createJobThread(
            final ActiveJobExecution active,
            final JobDefinition definition) {

        Thread thread =
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                runScheduledJob(
                                        active,
                                        definition);
                            }
                        },
                        "link-up-job-"
                                + active.getJobId()
                                + "-"
                                + threadSequence.incrementAndGet());

        thread.setDaemon(false);
        return thread;
    }

    private void runScheduledJob(
            ActiveJobExecution active,
            JobDefinition definition) {

        try {
            jobRunner.run(
                    active,
                    definition);
        } finally {
            activeJobs.remove(
                    active.getJobId(),
                    active);
            admissionSemaphore.release();
        }
    }

    private void cancelActiveJobs() {
        for (ActiveJobExecution active : activeJobs.values()) {
            active.cancel();
        }
    }

    private void awaitActiveJobs() {
        long deadline =
                System.currentTimeMillis()
                        + shutdownTimeoutMillis;

        for (ActiveJobExecution active : activeJobs.values()) {
            Thread thread = active.getThread();

            if (thread == null) {
                continue;
            }

            long remaining =
                    deadline
                            - System.currentTimeMillis();

            if (remaining <= 0L) {
                return;
            }

            try {
                thread.join(remaining);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void notifyLostJobs() {
        for (ActiveJobExecution active : activeJobs.values()) {
            active.getListener().onLost();
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

        if (value == null
                || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }

        return value.trim();
    }
}
