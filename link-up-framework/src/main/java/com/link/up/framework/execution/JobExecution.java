package com.link.up.framework.execution;

import com.link.up.api.dirtydata.DirtyDataSummary;
import com.link.up.api.sink.CommitScope;
import com.link.up.api.source.SourceSplit;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.PipelineGraph;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Coordinates one runtime execution of an immutable JobGraph.
 *
 * <p>Mutable state is owned by {@link ExecutionGraph}; this class only drives
 * pipeline execution and aggregates outcomes.
 */
public final class JobExecution {

    private static final Logger LOG =
            LogManager.getLogger(JobExecution.class);

    private final ExecutionGraph executionGraph;
    private final ClassLoader classLoader;

    public JobExecution(
            JobGraph jobGraph,
            ClassLoader classLoader) {

        this(
                jobGraph,
                classLoader,
                LogIdentity.create(jobGraph));
    }

    private JobExecution(
            JobGraph jobGraph,
            ClassLoader classLoader,
            LogIdentity identity) {

        this(
                new ExecutionGraph(
                        jobGraph,
                        identity.startTimeMillis,
                        identity.runId,
                        identity.jobLogFile),
                classLoader);
    }

    public JobExecution(
            JobGraph jobGraph,
            ClassLoader classLoader,
            long startTimeMillis,
            String runId,
            String jobLogFile) {

        this(
                new ExecutionGraph(
                        jobGraph,
                        startTimeMillis,
                        runId,
                        jobLogFile),
                classLoader);
    }

    JobExecution(
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

    public JobResult execute() {
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

            JobResult result = executeInternal();
            executionGraph.complete(result);

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

            return result;

        } catch (RuntimeException failure) {
            executionGraph.fail(failure);
            throw failure;
        } catch (Error failure) {
            executionGraph.fail(failure);
            throw failure;
        }
    }

    private JobResult executeInternal() {
        final JobGraph jobGraph = executionGraph.getJobGraph();
        final CancellationToken cancellationToken =
                executionGraph.getCancellationToken();
        final JobMetrics jobMetrics = executionGraph.getMetrics();
        final long start = executionGraph.getStartTimeMillis();
        final String currentRunId = executionGraph.getRunId();
        final String currentJobLogFile = executionGraph.getJobLogFile();

        List<PipelineResult> results =
                new ArrayList<PipelineResult>();
        Throwable first = null;

        if (!jobGraph.isEmpty()) {
            int threadCount = Math.min(
                    jobGraph
                            .getExecutionConfig()
                            .getPipelineParallelism(),
                    jobGraph
                            .getPipelineGraphs()
                            .size());

            ExecutorService pool =
                    Executors.newFixedThreadPool(threadCount);
            CompletionService<PipelineResult> completionService =
                    new ExecutorCompletionService<PipelineResult>(pool);
            List<Future<PipelineResult>> submitted =
                    new ArrayList<Future<PipelineResult>>();

            try {
                for (final PipelineGraph<?> pipelineGraph :
                        jobGraph.getPipelineGraphs()) {

                    submitted.add(
                            completionService.submit(
                                    new Callable<PipelineResult>() {
                                        public PipelineResult call() {
                                            try (CloseableThreadContext.Instance ignored =
                                                         openJobLogContext(
                                                                 currentRunId,
                                                                 jobGraph.getJobName(),
                                                                 currentJobLogFile)) {
                                                return executePipeline(
                                                        pipelineGraph,
                                                        jobGraph,
                                                        cancellationToken,
                                                        jobMetrics,
                                                        start);
                                            }
                                        }
                                    }));
                }

                for (int index = 0;
                     index < submitted.size();
                     index++) {
                    try {
                        PipelineResult result =
                                completionService
                                        .take()
                                        .get();
                        results.add(result);

                        if (result.getFailure() != null
                                && first == null) {
                            first = result.getFailure();
                            cancellationToken.cancel(first);
                        }

                    } catch (Exception exception) {
                        Throwable failure =
                                exception instanceof ExecutionException
                                        && exception.getCause() != null
                                        ? exception.getCause()
                                        : exception;

                        if (first == null) {
                            first = failure;
                            cancellationToken.cancel(failure);
                        }
                    }
                }

            } finally {
                pool.shutdownNow();
            }
        }

        CommitSummary summary = merge(results);
        JobStatus status =
                first != null
                        ? JobStatus.FAILED
                        : cancellationToken.isCancelled()
                        ? JobStatus.CANCELED
                        : JobStatus.SUCCEEDED;

        return new JobResult(
                jobGraph.getJobName(),
                status,
                start,
                System.currentTimeMillis(),
                jobMetrics,
                first,
                summary,
                DirtyDataSummary.empty(),
                results);
    }

    private <SplitT extends SourceSplit> PipelineResult executePipeline(
            PipelineGraph<SplitT> pipelineGraph,
            JobGraph jobGraph,
            CancellationToken cancellationToken,
            JobMetrics jobMetrics,
            long start) {

        return new PipelineExecution<SplitT>(
                pipelineGraph,
                jobGraph.getExecutionConfig(),
                cancellationToken,
                jobMetrics,
                classLoader,
                jobGraph.getJobName(),
                start)
                .execute();
    }

    private static CloseableThreadContext.Instance openJobLogContext(
            String currentRunId,
            String jobName,
            String currentJobLogFile) {

        return CloseableThreadContext
                .put("runId", currentRunId)
                .put("jobId", currentRunId)
                .put("jobName", jobName)
                .put("jobLogFile", currentJobLogFile);
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

    public void cancel() {
        executionGraph.requestCancellation(
                new java.util.concurrent.CancellationException(
                        "Job was cancelled by caller"));
    }

    public JobMetrics getMetrics() {
        return executionGraph.getMetrics();
    }

    public String getRunId() {
        return executionGraph.getRunId();
    }

    public String getJobLogFile() {
        return executionGraph.getJobLogFile();
    }

    public boolean isCancellationRequested() {
        return executionGraph.isCancellationRequested();
    }

    public ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    private static final class LogIdentity {
        private final long startTimeMillis;
        private final String runId;
        private final String jobLogFile;

        private LogIdentity(
                long startTimeMillis,
                String runId,
                String jobLogFile) {
            this.startTimeMillis = startTimeMillis;
            this.runId = runId;
            this.jobLogFile = jobLogFile;
        }

        private static LogIdentity create(JobGraph jobGraph) {
            Objects.requireNonNull(
                    jobGraph,
                    "jobGraph must not be null");

            long start = System.currentTimeMillis();
            String jobName = jobGraph.getJobName();

            return new LogIdentity(
                    start,
                    JobLogFileName.createJobId(
                            jobName,
                            start),
                    JobLogFileName.create(
                            jobName,
                            start));
        }
    }
}
