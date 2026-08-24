package com.link.up.framework.execution;

import com.link.up.api.source.SourceSplit;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.PipelineGraph;
import org.apache.logging.log4j.CloseableThreadContext;

import java.util.Objects;

/**
 * Materializes and executes one local {@link PipelineExecution} from an
 * immutable {@link PipelineGraph}.
 *
 * <p>The executor also restores the job log context on the scheduler worker
 * thread. Log4j thread context is thread-local and therefore must be opened at
 * the execution boundary rather than assumed to propagate from the caller.
 */
final class LocalPipelineExecutor
        implements PipelineExecutor {

    private final ExecutionGraph executionGraph;
    private final ClassLoader classLoader;

    LocalPipelineExecutor(
            ExecutionGraph executionGraph,
            ClassLoader classLoader) {

        this.executionGraph =
                Objects.requireNonNull(
                        executionGraph,
                        "executionGraph must not be null");
        this.classLoader =
                Objects.requireNonNull(
                        classLoader,
                        "classLoader must not be null");
    }

    @Override
    public PipelineResult execute(
            PipelineGraph<?> pipelineGraph) {

        Objects.requireNonNull(
                pipelineGraph,
                "pipelineGraph must not be null");

        JobGraph jobGraph = executionGraph.getJobGraph();

        try (CloseableThreadContext.Instance ignored =
                     CloseableThreadContext
                             .put("runId", executionGraph.getRunId())
                             .put("jobId", executionGraph.getRunId())
                             .put("jobName", jobGraph.getJobName())
                             .put("jobLogFile", executionGraph.getJobLogFile())) {

            return executeTyped(pipelineGraph);
        }
    }

    private <SplitT extends SourceSplit> PipelineResult executeTyped(
            PipelineGraph<SplitT> pipelineGraph) {

        JobGraph jobGraph = executionGraph.getJobGraph();

        return new PipelineExecution<SplitT>(
                pipelineGraph,
                jobGraph.getExecutionConfig(),
                executionGraph.getCancellationToken(),
                executionGraph.getMetrics(),
                classLoader,
                jobGraph.getJobName(),
                executionGraph.getStartTimeMillis())
                .execute();
    }
}
