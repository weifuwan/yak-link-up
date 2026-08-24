package com.link.up.framework.execution;

import com.link.up.framework.execution.task.ExecutionTask;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.metrics.TaskMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Coordinates local task execution for one pipeline.
 *
 * <p>Sink tasks are submitted before source tasks. The first task failure
 * cancels the shared runtime and interrupts outstanding tasks.</p>
 */
public final class ExecutionCoordinator {

    private final TaskExecutor taskExecutor;
    private final CancellationToken cancellationToken;
    private final JobMetrics jobMetrics;
    private final ClassLoader classLoader;
    private final Runnable cancellationHook;
    private final SinkCommitSummaryCollector commitSummaryCollector;

    public ExecutionCoordinator(
            TaskExecutor taskExecutor,
            CancellationToken cancellationToken,
            JobMetrics jobMetrics,
            ClassLoader classLoader,
            Runnable cancellationHook) {

        this.taskExecutor = Objects.requireNonNull(
                taskExecutor,
                "taskExecutor must not be null");
        this.cancellationToken = Objects.requireNonNull(
                cancellationToken,
                "cancellationToken must not be null");
        this.jobMetrics = Objects.requireNonNull(
                jobMetrics,
                "jobMetrics must not be null");
        this.classLoader = Objects.requireNonNull(
                classLoader,
                "classLoader must not be null");
        this.cancellationHook = Objects.requireNonNull(
                cancellationHook,
                "cancellationHook must not be null");
        this.commitSummaryCollector =
                new SinkCommitSummaryCollector(jobMetrics);
    }

    public ExecutionOutcome execute(
            List<ExecutionTask> sinkTasks,
            List<ExecutionTask> sourceTasks) {

        Objects.requireNonNull(
                sinkTasks,
                "sinkTasks must not be null");
        Objects.requireNonNull(
                sourceTasks,
                "sourceTasks must not be null");

        List<ExecutionTask> orderedTasks =
                orderedTasks(sinkTasks, sourceTasks);

        if (orderedTasks.isEmpty()) {
            return new ExecutionOutcome(
                    null,
                    CommitSummary.empty());
        }

        List<Future<TaskResult>> futures =
                new ArrayList<Future<TaskResult>>(
                        orderedTasks.size());

        Throwable firstFailure = null;

        try {
            submit(orderedTasks, futures);
            firstFailure =
                    awaitCompletion(
                            orderedTasks,
                            futures);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            firstFailure = interrupted;
            cancelAll(
                    orderedTasks,
                    futures,
                    interrupted);
        } catch (Throwable failure) {
            firstFailure = failure;
            cancelAll(
                    orderedTasks,
                    futures,
                    failure);
        }

        return new ExecutionOutcome(
                firstFailure,
                commitSummaryCollector.collect(sinkTasks));
    }

    private List<ExecutionTask> orderedTasks(
            List<ExecutionTask> sinkTasks,
            List<ExecutionTask> sourceTasks) {

        List<ExecutionTask> tasks =
                new ArrayList<ExecutionTask>(
                        sinkTasks.size()
                                + sourceTasks.size());

        tasks.addAll(sinkTasks);
        tasks.addAll(sourceTasks);
        return tasks;
    }

    private void submit(
            List<ExecutionTask> tasks,
            List<Future<TaskResult>> futures) {

        for (ExecutionTask task : tasks) {
            TaskMetrics metrics =
                    jobMetrics.registerTask(
                            task.getTaskId());

            TaskContext context =
                    new TaskContext(
                            task.getTaskId(),
                            cancellationToken,
                            metrics,
                            classLoader);

            futures.add(
                    taskExecutor.submit(
                            task,
                            context));
        }
    }

    private Throwable awaitCompletion(
            List<ExecutionTask> tasks,
            List<Future<TaskResult>> futures)
            throws InterruptedException {

        Throwable firstFailure = null;

        for (int index = 0;
             index < tasks.size();
             index++) {

            Future<TaskResult> completed =
                    taskExecutor.takeCompleted();

            try {
                TaskResult result = completed.get();

                if (result.isFailed()
                        && firstFailure == null) {
                    firstFailure = result.getFailure();
                    cancelAll(
                            tasks,
                            futures,
                            firstFailure);
                }
            } catch (CancellationException ignored) {
                // Cancellation was already initiated by another task.
            } catch (ExecutionException executionFailure) {
                if (firstFailure != null) {
                    continue;
                }

                firstFailure =
                        executionFailure.getCause() == null
                                ? executionFailure
                                : executionFailure.getCause();

                cancelAll(
                        tasks,
                        futures,
                        firstFailure);
            }
        }

        return firstFailure;
    }

    private void cancelAll(
            List<ExecutionTask> tasks,
            List<Future<TaskResult>> futures,
            Throwable cause) {

        cancellationToken.cancel(cause);

        try {
            cancellationHook.run();
        } catch (Throwable hookFailure) {
            cause.addSuppressed(hookFailure);
        }

        for (ExecutionTask task : tasks) {
            try {
                task.cancel();
            } catch (Throwable cancelFailure) {
                cause.addSuppressed(cancelFailure);
            }
        }

        for (Future<TaskResult> future : futures) {
            future.cancel(true);
        }
    }

    /** Result of task coordination and local sink commit observations. */
    public static final class ExecutionOutcome {

        private final Throwable failure;
        private final CommitSummary commitSummary;

        private ExecutionOutcome(
                Throwable failure,
                CommitSummary commitSummary) {

            this.failure = failure;
            this.commitSummary = commitSummary;
        }

        public Throwable getFailure() {
            return failure;
        }

        public CommitSummary getCommitSummary() {
            return commitSummary;
        }
    }
}
