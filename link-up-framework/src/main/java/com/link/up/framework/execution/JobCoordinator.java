package com.link.up.framework.execution;

import com.link.up.api.dirtydata.DirtyDataSummary;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.planner.JobGraph;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Coordinates the lifecycle of one {@link ExecutionGraph}.
 *
 * <p>Pipeline concurrency belongs to {@link PipelineScheduler}; pipeline work
 * belongs to {@link PipelineExecutor}.</p>
 */
final class JobCoordinator {

    private static final Logger LOG =
            LogManager.getLogger(JobCoordinator.class);

    private final ExecutionGraph executionGraph;
    private final PipelineScheduler pipelineScheduler;
    private final PipelineExecutor pipelineExecutor;

    JobCoordinator(
            ExecutionGraph executionGraph,
            PipelineScheduler pipelineScheduler,
            PipelineExecutor pipelineExecutor) {

        this.executionGraph = Objects.requireNonNull(
                executionGraph,
                "executionGraph must not be null");
        this.pipelineScheduler = Objects.requireNonNull(
                pipelineScheduler,
                "pipelineScheduler must not be null");
        this.pipelineExecutor = Objects.requireNonNull(
                pipelineExecutor,
                "pipelineExecutor must not be null");
    }

    JobResult execute() {
        JobGraph jobGraph = executionGraph.getJobGraph();
        executionGraph.markRunning();

        try (CloseableThreadContext.Instance ignored =
                     openJobLogContext(jobGraph)) {

            logStarted(jobGraph);

            PipelineScheduleResult scheduleResult =
                    pipelineScheduler.schedule(
                            jobGraph.getPipelineGraphs(),
                            jobGraph.getExecutionConfig()
                                    .getPipelineParallelism(),
                            executionGraph.getCancellationToken(),
                            pipelineExecutor);

            JobResult result =
                    createJobResult(
                            jobGraph,
                            scheduleResult);

            executionGraph.complete(result);
            logResult(result);
            return result;

        } catch (RuntimeException failure) {
            executionGraph.fail(failure);
            throw failure;
        } catch (Error failure) {
            executionGraph.fail(failure);
            throw failure;
        }
    }

    private JobResult createJobResult(
            JobGraph jobGraph,
            PipelineScheduleResult scheduleResult) {

        Objects.requireNonNull(
                scheduleResult,
                "scheduleResult must not be null");

        Throwable failure = scheduleResult.getFailure();

        return new JobResult(
                jobGraph.getJobName(),
                resolveStatus(failure),
                executionGraph.getStartTimeMillis(),
                System.currentTimeMillis(),
                executionGraph.getMetrics(),
                failure,
                JobCommitSummaryMerger.merge(
                        scheduleResult.getPipelineResults()),
                DirtyDataSummary.empty(),
                scheduleResult.getPipelineResults());
    }

    private JobStatus resolveStatus(Throwable failure) {
        if (failure != null) {
            return JobStatus.FAILED;
        }

        if (executionGraph.isCancellationRequested()) {
            return JobStatus.CANCELED;
        }

        return JobStatus.SUCCEEDED;
    }

    private void logStarted(JobGraph jobGraph) {
        LOG.info(
                "Job started: jobName={}, runId={}, jobLogFile={}",
                jobGraph.getJobName(),
                executionGraph.getRunId(),
                executionGraph.getJobLogFile());
    }

    private void logResult(JobResult result) {
        if (result.getStatus() == JobStatus.SUCCEEDED) {
            LOG.info(
                    "Job finished: status={}, durationMillis={}",
                    result.getStatus(),
                    result.getDurationMillis());
            return;
        }

        if (result.getFailure() != null) {
            LOG.error(
                    "Job finished: status={}, durationMillis={}",
                    result.getStatus(),
                    result.getDurationMillis(),
                    result.getFailure());
            return;
        }

        LOG.warn(
                "Job finished: status={}, durationMillis={}",
                result.getStatus(),
                result.getDurationMillis());
    }

    private CloseableThreadContext.Instance openJobLogContext(
            JobGraph jobGraph) {

        return CloseableThreadContext
                .put("runId", executionGraph.getRunId())
                .put("jobId", executionGraph.getRunId())
                .put("jobName", jobGraph.getJobName())
                .put("jobLogFile", executionGraph.getJobLogFile());
    }
}
