package com.link.up.framework.execution;

import com.link.up.api.sink.CommitScope;
import com.link.up.framework.execution.task.ExecutionTask;
import com.link.up.framework.execution.task.SinkTask;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.metrics.TaskMetrics;

import java.util.List;
import java.util.Objects;

/** Collects sink commit observations after task coordination finishes. */
final class SinkCommitSummaryCollector {

    private static final String DEFAULT_RETRY_ADVICE =
            "This sink commits per task; verify already committed targets before retrying.";

    private final JobMetrics jobMetrics;

    SinkCommitSummaryCollector(JobMetrics jobMetrics) {
        this.jobMetrics = Objects.requireNonNull(
                jobMetrics,
                "jobMetrics must not be null");
    }

    CommitSummary collect(List<ExecutionTask> sinkTasks) {
        Objects.requireNonNull(
                sinkTasks,
                "sinkTasks must not be null");

        int finished = 0;
        int committed = 0;
        int emptyCommitted = 0;
        long attempted = 0L;
        long written = 0L;
        long failed = 0L;
        long unknown = 0L;
        CommitScope scope = CommitScope.TASK_LOCAL;
        String retryAdvice = DEFAULT_RETRY_ADVICE;

        for (ExecutionTask task : sinkTasks) {
            if (!(task instanceof SinkTask)) {
                continue;
            }

            SinkTask sinkTask = (SinkTask) task;
            TaskMetrics metrics =
                    jobMetrics.getTaskMetrics()
                            .get(sinkTask.getTaskId());

            long successful =
                    metrics == null
                            ? 0L
                            : metrics.getSinkWriteSuccessRecordCount();

            attempted +=
                    metrics == null
                            ? 0L
                            : metrics.getAttemptedRecordCount();
            written += successful;
            unknown +=
                    metrics == null
                            ? 0L
                            : metrics.getUnknownStateRecordCount();
            failed +=
                    metrics == null
                            ? 0L
                            : metrics.getFailedRecordCount();

            if (metrics != null
                    && metrics.getState() == TaskState.FINISHED) {
                finished++;
            }

            if (sinkTask.isCommitted()) {
                committed++;
                if (successful == 0L) {
                    emptyCommitted++;
                }
            }

            scope = sinkTask.getCommitScope();
            retryAdvice = sinkTask.getRetryAdvice();
        }

        return new CommitSummary(
                sinkTasks.size(),
                finished,
                committed,
                emptyCommitted,
                sinkTasks.size() - committed,
                attempted,
                written,
                committed == 0 ? 0L : written,
                failed,
                unknown,
                scope,
                retryAdvice);
    }
}
