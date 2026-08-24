package com.link.up.framework.execution;

import com.link.up.framework.execution.task.ExecutionTask;
import com.link.up.framework.metrics.TaskMetrics;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Owns local worker threads that execute already-planned tasks. */
public final class TaskExecutor
        implements AutoCloseable {

    private static final Logger LOG =
            LogManager.getLogger(TaskExecutor.class);

    private final ExecutorService executorService;
    private final ExecutorCompletionService<TaskResult> completionService;
    private final String jobName;
    private final long runId;

    public TaskExecutor(
            int threadCount,
            String threadPrefix) {

        this(
                threadCount,
                threadPrefix,
                "unnamed",
                System.currentTimeMillis());
    }

    public TaskExecutor(
            int threadCount,
            String threadPrefix,
            String jobName,
            long runId) {

        if (threadCount <= 0) {
            throw new IllegalArgumentException(
                    "threadCount must be greater than 0");
        }
        if (runId < 0L) {
            throw new IllegalArgumentException(
                    "runId must not be negative");
        }

        this.jobName = Objects.requireNonNull(
                jobName,
                "jobName must not be null");
        this.runId = runId;

        this.executorService =
                Executors.newFixedThreadPool(
                        threadCount,
                        threadFactory(
                                Objects.requireNonNull(
                                        threadPrefix,
                                        "threadPrefix must not be null")));

        this.completionService =
                new ExecutorCompletionService<TaskResult>(
                        executorService);
    }

    public Future<TaskResult> submit(
            final ExecutionTask task,
            final TaskContext context) {

        Objects.requireNonNull(
                task,
                "task must not be null");
        Objects.requireNonNull(
                context,
                "context must not be null");

        final String jobId =
                JobLogFileName.createJobId(
                        jobName,
                        runId);
        final String jobLogFile =
                JobLogFileName.create(
                        jobName,
                        runId);

        return completionService.submit(
                new Callable<TaskResult>() {
                    @Override
                    public TaskResult call() {
                        try (CloseableThreadContext.Instance ignored =
                                     CloseableThreadContext
                                             .put("runId", jobId)
                                             .put("jobId", jobId)
                                             .put("jobName", jobName)
                                             .put("jobLogFile", jobLogFile)) {

                            return executeTask(
                                    task,
                                    context);
                        }
                    }
                });
    }

    public Future<TaskResult> takeCompleted()
            throws InterruptedException {

        return completionService.take();
    }

    @Override
    public void close() {
        executorService.shutdownNow();

        try {
            if (!executorService.awaitTermination(
                    5L,
                    TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }

    private TaskResult executeTask(
            ExecutionTask task,
            TaskContext context) {

        if (context.getCancellationToken().isCancelled()) {
            return cancelBeforeExecution(
                    task,
                    context);
        }

        TaskMetrics metrics = context.getMetrics();
        metrics.markStarted();

        LOG.info(
                "Task started: {}",
                task.getTaskId());

        try {
            task.execute(context);

            if (context.getCancellationToken().isCancelled()) {
                return cancelAfterExecution(
                        task,
                        context);
            }

            metrics.markFinished(TaskState.FINISHED);

            LOG.info(
                    "Task finished: {}",
                    task.getTaskId());

            return TaskResult.finished(
                    task.getTaskId());

        } catch (Throwable failure) {
            return handleFailure(
                    task,
                    context,
                    failure);
        }
    }

    private TaskResult cancelBeforeExecution(
            ExecutionTask task,
            TaskContext context) {

        context.getMetrics()
                .markFinished(TaskState.CANCELED);

        LOG.info(
                "Task cancelled before execution: {}",
                task.getTaskId());

        return TaskResult.canceled(
                task.getTaskId(),
                context.getCancellationToken()
                        .getCause());
    }

    private TaskResult cancelAfterExecution(
            ExecutionTask task,
            TaskContext context) {

        context.getMetrics()
                .markFinished(TaskState.CANCELED);

        LOG.info(
                "Task cancelled: {}",
                task.getTaskId());

        return TaskResult.canceled(
                task.getTaskId(),
                context.getCancellationToken()
                        .getCause());
    }

    private TaskResult handleFailure(
            ExecutionTask task,
            TaskContext context,
            Throwable failure) {

        if (context.getCancellationToken().isCancelled()
                && failure instanceof InterruptedException) {

            Thread.currentThread().interrupt();
            context.getMetrics()
                    .markFinished(TaskState.CANCELED);

            LOG.info(
                    "Task cancelled: {}",
                    task.getTaskId());

            return TaskResult.canceled(
                    task.getTaskId(),
                    failure);
        }

        context.getMetrics()
                .markFinished(TaskState.FAILED);

        LOG.error(
                "Task failed: {}",
                task.getTaskId(),
                failure);

        return TaskResult.failed(
                task.getTaskId(),
                failure);
    }

    private static ThreadFactory threadFactory(
            final String threadPrefix) {

        final AtomicInteger sequence =
                new AtomicInteger();

        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread =
                        new Thread(
                                runnable,
                                threadPrefix
                                        + "-"
                                        + sequence.getAndIncrement());

                thread.setDaemon(false);
                return thread;
            }
        };
    }
}
