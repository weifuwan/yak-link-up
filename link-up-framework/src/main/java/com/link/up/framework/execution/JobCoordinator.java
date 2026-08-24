package com.link.up.framework.execution;

import com.link.up.api.dirtydata.DirtyDataSummary;
import com.link.up.api.sink.CommitScope;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.planner.JobGraph;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * Coordinates the lifecycle of one {@link ExecutionGraph}.
 *
 * <p>The coordinator owns job-level lifecycle transitions, failure policy and
 * result aggregation. Pipeline concurrency belongs to {@link PipelineScheduler}
 * and pipeline execution belongs to {@link PipelineExecutor}.
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

        this.executionGraph =
                Objects.requireNonNull(
                        executionGraph,
                        "executionGraph must not be null");
        this.pipelineScheduler =
                Objects.requireNonNull(
                        pipelineScheduler,
                        "pipelineScheduler must not be null");
        this.pipelineExecutor =
                Objects.requireNonNull(
                        pipelineExecutor,
                        "pipelineExecutor must not be null");
    }

    JobResult execute() {
        JobGraph jobGraph = executionGraph.getJobGraph();
        String jobName = jobGraph.getJobName();
        String runId = executionGraph.getRunId();
        String jobLogFile = executionGraph.getJobLogFile();

        executionGraph.markRunning();

        try (CloseableThreadContext.Instance ignored =
                     openJobLogContext(
                             runId,
                             jobName,
                             jobLogFile)) {

            LOG.info(
                    "Job started: jobName={}, runId={}, jobLogFile={}",
                    jobName,
                    runId,
                    jobLogFile);

            PipelineScheduleResult scheduleResult =
                    pipelineScheduler.schedule(
                            jobGraph.getPipelineGraphs(),
                            jobGraph
                                    .getExecutionConfig()
                                    .getPipelineParallelism(),
                            executionGraph.getCancellationToken(),
                            pipelineExecutor);

            JobResult result =
                    createJobResult(scheduleResult);

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
            PipelineScheduleResult scheduleResult) {

        Objects.requireNonNull(
                scheduleResult,
                "scheduleResult must not be null");

        JobGraph jobGraph = executionGraph.getJobGraph();
        Throwable failure = scheduleResult.getFailure();
        JobStatus status =
                failure != null
                        ? JobStatus.FAILED
                        : executionGraph.isCancellationRequested()
                        ? JobStatus.CANCELED
                        : JobStatus.SUCCEEDED;

        return new JobResult(
                jobGraph.getJobName(),
                status,
                executionGraph.getStartTimeMillis(),
                System.currentTimeMillis(),
                executionGraph.getMetrics(),
                failure,
                merge(scheduleResult.getPipelineResults()),
                DirtyDataSummary.empty(),
                scheduleResult.getPipelineResults());
    }

    private void logResult(JobResult result) {
        if (result.getStatus() == JobStatus.SUCCEEDED) {
            LOG.info(
                    "Job finished: status={}, durationMillis={}",
                    result.getStatus(),
                    result.getDurationMillis());
        } else if (result.getFailure() != null) {
            LOG.error(
                    "Job finished: status={}, durationMillis={}",
                    result.getStatus(),
                    result.getDurationMillis(),
                    result.getFailure());
        } else {
            LOG.warn(
                    "Job finished: status={}, durationMillis={}",
                    result.getStatus(),
                    result.getDurationMillis());
        }
    }

    private CommitSummary merge(
            List<PipelineResult> results) {

        int total = 0;
        int finished = 0;
        int committed = 0;
        int empty = 0;
        int failed = 0;
        long attempted = 0L;
        long written = 0L;
        long committedRows = 0L;
        long failedRows = 0L;
        long unknown = 0L;

        for (PipelineResult result : results) {
            CommitSummary summary = result.getCommitSummary();

            total += summary.getTotalTaskCount();
            finished += summary.getFinishedTaskCount();
            committed += summary.getCommittedTaskCount();
            empty += summary.getEmptyCommittedTaskCount();
            failed += summary.getFailedOrUncommittedTaskCount();
            attempted += summary.getAttemptedRecordCount();
            written += summary.getSuccessfullyWrittenRecordCount();
            committedRows += summary.getSuccessfullyCommittedRecordCount();
            failedRows += summary.getFailedRecordCount();
            unknown += summary.getUnknownStateRecordCount();
        }

        return new CommitSummary(
                total,
                finished,
                committed,
                empty,
                failed,
                attempted,
                written,
                committedRows,
                failedRows,
                unknown,
                CommitScope.TASK_LOCAL,
                "This sink commits per task; inspect unknown batch states before retrying.");
    }

    private static CloseableThreadContext.Instance openJobLogContext(
            String runId,
            String jobName,
            String jobLogFile) {

        return CloseableThreadContext
                .put("runId", runId)
                .put("jobId", runId)
                .put("jobName", jobName)
                .put("jobLogFile", jobLogFile);
    }
}
