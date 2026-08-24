package com.link.up.framework.execution;

import com.link.up.api.source.SourceSplit;
import com.link.up.framework.execution.task.ExecutionTask;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.job.PipelineStatus;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.planner.PipelineGraph;

import java.util.List;
import java.util.Objects;

/**
 * Executes one immutable {@link PipelineGraph}.
 *
 * <p>This class coordinates the pipeline workflow only. Mutable channels and
 * split queues belong to {@link PipelineRuntimeResources}; task materialization
 * belongs to {@link PipelineTaskFactory}.</p>
 */
final class PipelineExecution<SplitT extends SourceSplit> {

    private final PipelineGraph<SplitT> graph;
    private final ExecutionConfig config;
    private final CancellationToken cancellationToken;
    private final JobMetrics metrics;
    private final ClassLoader classLoader;
    private final String jobName;
    private final long runId;

    PipelineExecution(
            PipelineGraph<SplitT> graph,
            ExecutionConfig config,
            CancellationToken cancellationToken,
            JobMetrics metrics,
            ClassLoader classLoader,
            String jobName,
            long runId) {

        this.graph = Objects.requireNonNull(
                graph,
                "graph must not be null");
        this.config = Objects.requireNonNull(
                config,
                "config must not be null");
        this.cancellationToken = Objects.requireNonNull(
                cancellationToken,
                "cancellationToken must not be null");
        this.metrics = Objects.requireNonNull(
                metrics,
                "metrics must not be null");
        this.classLoader = Objects.requireNonNull(
                classLoader,
                "classLoader must not be null");
        this.jobName = Objects.requireNonNull(
                jobName,
                "jobName must not be null");

        if (runId < 0L) {
            throw new IllegalArgumentException(
                    "runId must not be negative");
        }
        this.runId = runId;
    }

    PipelineResult execute() {
        try (PipelineRuntimeResources<SplitT> resources =
                     PipelineRuntimeResources.open(
                             graph,
                             config,
                             metrics)) {

            PipelineTaskFactory<SplitT> taskFactory =
                    new PipelineTaskFactory<SplitT>(
                            graph,
                            resources.getChannels(),
                            resources.getSplitProvider());

            List<ExecutionTask> sinkTasks =
                    taskFactory.createSinkTasks();
            List<ExecutionTask> sourceTasks =
                    taskFactory.createSourceTasks();

            ExecutionCoordinator.ExecutionOutcome outcome =
                    executeTasks(
                            sinkTasks,
                            sourceTasks,
                            resources);

            return PipelineResultFactory.create(
                    graph,
                    statusOf(outcome),
                    outcome.getCommitSummary(),
                    outcome.getFailure());
        }
    }

    private ExecutionCoordinator.ExecutionOutcome executeTasks(
            List<ExecutionTask> sinkTasks,
            List<ExecutionTask> sourceTasks,
            final PipelineRuntimeResources<SplitT> resources) {

        try (TaskExecutor taskExecutor =
                     new TaskExecutor(
                             sinkTasks.size() + sourceTasks.size(),
                             "link-up-" + graph.getPipelineId(),
                             jobName,
                             runId)) {

            ExecutionCoordinator coordinator =
                    new ExecutionCoordinator(
                            taskExecutor,
                            cancellationToken,
                            metrics,
                            classLoader,
                            new Runnable() {
                                @Override
                                public void run() {
                                    resources.cancel(
                                            cancellationToken.getCause());
                                }
                            });

            return coordinator.execute(
                    sinkTasks,
                    sourceTasks);
        }
    }

    private PipelineStatus statusOf(
            ExecutionCoordinator.ExecutionOutcome outcome) {
        if (outcome.getFailure() == null) {
            return PipelineStatus.SUCCEEDED;
        }

        return cancellationToken.isCancelled()
                ? PipelineStatus.CANCELED
                : PipelineStatus.FAILED;
    }
}
