package com.link.up.framework.execution;

import com.link.up.framework.job.JobResult;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.planner.JobGraph;

import java.util.Objects;

/**
 * Public handle for one local execution of an immutable {@link JobGraph}.
 *
 * <p>This class is intentionally a thin facade. Job lifecycle coordination is
 * delegated to {@link JobCoordinator}; pipeline concurrency and pipeline work
 * are owned by dedicated scheduler/executor roles.
 */
public final class JobExecution {

    private final ExecutionGraph executionGraph;
    private final JobCoordinator jobCoordinator;

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

        this(
                executionGraph,
                new LocalPipelineScheduler(),
                new LocalPipelineExecutor(
                        executionGraph,
                        classLoader));
    }

    JobExecution(
            ExecutionGraph executionGraph,
            PipelineScheduler pipelineScheduler,
            PipelineExecutor pipelineExecutor) {

        this.executionGraph =
                Objects.requireNonNull(
                        executionGraph,
                        "executionGraph must not be null");
        this.jobCoordinator =
                new JobCoordinator(
                        executionGraph,
                        Objects.requireNonNull(
                                pipelineScheduler,
                                "pipelineScheduler must not be null"),
                        Objects.requireNonNull(
                                pipelineExecutor,
                                "pipelineExecutor must not be null"));
    }

    public JobResult execute() {
        return jobCoordinator.execute();
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
