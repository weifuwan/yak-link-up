package com.link.up.server.runtime;

import com.link.up.api.sink.TableDdl;
import com.link.up.framework.execution.TaskId;
import com.link.up.framework.execution.TaskType;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.metrics.ChannelMetrics;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.framework.metrics.TaskMetrics;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Copies control-plane state and framework metrics into REST-safe read models.
 */
public final class JobSnapshotFactory {

    private JobSnapshotFactory() {
    }

    public static JobSnapshot create(
            JobExecutionState state,
            JobMetrics liveMetrics) {

        JobResult result = state.getResult();
        JobMetrics jobMetrics =
                result == null
                        ? liveMetrics
                        : result.getMetrics();

        JobSnapshot.Metrics metrics = metrics(jobMetrics);
        JobSnapshot.Commit commit =
                commit(
                        result == null
                                ? null
                                : result.getCommitSummary());
        List<JobSnapshot.Pipeline> pipelines =
                pipelines(
                        state,
                        result,
                        jobMetrics);

        Throwable failure = state.getFailure();
        if (failure == null
                && state.getStatus() == ServerJobStatus.FAILED
                && result != null) {
            failure = result.getFailure();
        }

        long endTimeMillis = state.getEndTimeMillis();
        long durationMillis =
                duration(
                        state.getStartTimeMillis(),
                        endTimeMillis);

        return new JobSnapshot(
                state.getJobId(),
                state.getJobName(),
                state.getStatus(),
                state.getCreateTimeMillis(),
                state.getStartTimeMillis(),
                endTimeMillis,
                durationMillis,
                metrics,
                commit,
                pipelines,
                failure == null
                        ? null
                        : "FLUX-JOB-FAILED",
                failure == null
                        ? null
                        : safeMessage(failure));
    }

    public static JobExecutionMetadata metadata(
            JobExecutionState state) {

        Map<String, TableDdl> tableDdls =
                new LinkedHashMap<String, TableDdl>();
        JobResult result = state.getResult();

        if (result != null) {
            for (PipelineResult pipelineResult :
                    result.getPipelineResults()) {
                if (pipelineResult.getTableDdl() != null) {
                    tableDdls.put(
                            pipelineResult.getPipelineId(),
                            pipelineResult.getTableDdl());
                }
            }
        }

        JobSubmission submission = state.getSubmission();
        return new JobExecutionMetadata(
                submission.getExternalExecutionId(),
                submission.getIdempotencyKey(),
                submission.getDefinitionVersion(),
                submission.getConfigDigest(),
                state.getSubmittedTimeMillis(),
                state.getQueuedTimeMillis(),
                state.getStateVersion(),
                state.isCancellationRequested(),
                state.getTransitions(),
                tableDdls,
                state.getRunId(),
                state.getJobLogFile());
    }

    private static List<JobSnapshot.Pipeline> pipelines(
            JobExecutionState state,
            JobResult result,
            JobMetrics metrics) {

        Map<String, List<TaskMetrics>> tasksByPipeline =
                groupTasks(metrics);
        List<JobSnapshot.Pipeline> pipelines =
                new ArrayList<JobSnapshot.Pipeline>();

        if (result != null
                && !result.getPipelineResults().isEmpty()) {
            for (PipelineResult pipelineResult :
                    result.getPipelineResults()) {
                List<TaskMetrics> taskMetrics =
                        tasksByPipeline.remove(
                                pipelineResult.getPipelineId());
                pipelines.add(
                        finalPipeline(
                                pipelineResult,
                                taskMetrics,
                                metrics));
            }
        }

        List<String> remainingPipelineIds =
                new ArrayList<String>(
                        tasksByPipeline.keySet());
        Collections.sort(remainingPipelineIds);

        for (String pipelineId : remainingPipelineIds) {
            pipelines.add(
                    livePipeline(
                            state,
                            pipelineId,
                            tasksByPipeline.get(pipelineId),
                            metrics));
        }

        return pipelines;
    }

    private static JobSnapshot.Pipeline finalPipeline(
            PipelineResult result,
            List<TaskMetrics> taskMetrics,
            JobMetrics jobMetrics) {

        List<TaskMetrics> tasks =
                taskMetrics == null
                        ? Collections.<TaskMetrics>emptyList()
                        : taskMetrics;
        CommitSummary summary = result.getCommitSummary();

        return new JobSnapshot.Pipeline(
                result.getPipelineId(),
                result.getDataSetId(),
                result.getStatus().name(),
                source(
                        result.getSourceIdentifier(),
                        result.getSourceTable(),
                        result.getSourceTaskCount(),
                        tasks),
                sink(
                        result.getSinkIdentifier(),
                        result.getSinkTable(),
                        result.getSinkTaskCount(),
                        tasks,
                        summary),
                commit(summary),
                taskViews(tasks),
                channelViews(
                        result.getPipelineId(),
                        jobMetrics),
                result.getFailure() == null
                        ? null
                        : safeMessage(result.getFailure()));
    }

    private static JobSnapshot.Pipeline livePipeline(
            JobExecutionState state,
            String pipelineId,
            List<TaskMetrics> tasks,
            JobMetrics jobMetrics) {

        String sourceTable =
                firstTable(tasks, TaskType.SOURCE);
        String sinkTable =
                firstTable(tasks, TaskType.SINK);

        return new JobSnapshot.Pipeline(
                pipelineId,
                sourceTable,
                state.getStatus().name(),
                source(
                        state.getDefinition()
                                .getSource()
                                .getType(),
                        sourceTable,
                        countTasks(tasks, TaskType.SOURCE),
                        tasks),
                sink(
                        state.getDefinition()
                                .getSink()
                                .getType(),
                        sinkTable,
                        countTasks(tasks, TaskType.SINK),
                        tasks,
                        null),
                commit(null),
                taskViews(tasks),
                channelViews(pipelineId, jobMetrics),
                null);
    }

    private static JobSnapshot.Source source(
            String connector,
            String table,
            int configuredTaskCount,
            List<TaskMetrics> tasks) {

        long batches = 0L;
        long records = 0L;
        long bytes = 0L;
        long totalSplits = 0L;
        long completedSplits = 0L;
        long failedSplits = 0L;

        List<TaskMetrics> sourceTasks =
                filterTasks(tasks, TaskType.SOURCE);
        for (TaskMetrics task : sourceTasks) {
            batches += task.getBatchCount();
            records += task.getSourceReadRecordCount();
            bytes += task.getSourceReadBytes();
            totalSplits += task.getTotalSplitCount();
            completedSplits += task.getCompletedSplitCount();
            failedSplits += task.getFailedSplitCount();
        }

        return new JobSnapshot.Source(
                display(connector),
                display(table),
                configuredTaskCount > 0
                        ? configuredTaskCount
                        : sourceTasks.size(),
                batches,
                records,
                bytes,
                totalSplits,
                completedSplits,
                failedSplits,
                qps(sourceTasks, TaskType.SOURCE));
    }

    private static JobSnapshot.Sink sink(
            String connector,
            String table,
            int configuredTaskCount,
            List<TaskMetrics> tasks,
            CommitSummary commitSummary) {

        long receivedBatches = 0L;
        long attempted = 0L;
        long success = 0L;
        long failed = 0L;
        long unknown = 0L;
        long bytes = 0L;

        List<TaskMetrics> sinkTasks =
                filterTasks(tasks, TaskType.SINK);
        for (TaskMetrics task : sinkTasks) {
            receivedBatches += task.getReceivedBatchCount();
            attempted += task.getAttemptedRecordCount();
            success += task.getSinkWriteSuccessRecordCount();
            failed += task.getFailedRecordCount();
            unknown += task.getUnknownStateRecordCount();
            bytes += task.getSinkWrittenBytes();
        }

        return new JobSnapshot.Sink(
                display(connector),
                display(table),
                configuredTaskCount > 0
                        ? configuredTaskCount
                        : sinkTasks.size(),
                receivedBatches,
                attempted,
                success,
                failed,
                unknown,
                bytes,
                commitSummary == null
                        ? 0L
                        : commitSummary
                        .getSuccessfullyCommittedRecordCount(),
                qps(sinkTasks, TaskType.SINK));
    }

    private static double qps(
            List<TaskMetrics> tasks,
            TaskType type) {

        long records = 0L;
        long minimumStart = Long.MAX_VALUE;
        long maximumEnd = 0L;

        for (TaskMetrics task : tasks) {
            if (task.getStartTimeMillis() <= 0L) {
                continue;
            }
            records +=
                    type == TaskType.SOURCE
                            ? task.getSourceReadRecordCount()
                            : task.getSinkWriteSuccessRecordCount();
            minimumStart = Math.min(
                    minimumStart,
                    task.getStartTimeMillis());
            maximumEnd = Math.max(
                    maximumEnd,
                    task.getEndTimeMillis() > 0L
                            ? task.getEndTimeMillis()
                            : System.currentTimeMillis());
        }

        if (minimumStart == Long.MAX_VALUE) {
            return 0D;
        }
        long duration = Math.max(
                0L,
                maximumEnd - minimumStart);
        return duration == 0L
                ? 0D
                : records * 1000D / duration;
    }

    private static Map<String, List<TaskMetrics>>
    groupTasks(JobMetrics metrics) {

        Map<String, List<TaskMetrics>> grouped =
                new LinkedHashMap<String, List<TaskMetrics>>();
        if (metrics == null) {
            return grouped;
        }

        for (Map.Entry<TaskId, TaskMetrics> entry :
                metrics.getTaskMetrics().entrySet()) {
            String pipelineId =
                    entry.getKey().getPipelineId();
            List<TaskMetrics> tasks = grouped.get(pipelineId);
            if (tasks == null) {
                tasks = new ArrayList<TaskMetrics>();
                grouped.put(pipelineId, tasks);
            }
            tasks.add(entry.getValue());
        }
        return grouped;
    }

    private static List<TaskMetrics> filterTasks(
            List<TaskMetrics> tasks,
            TaskType type) {

        List<TaskMetrics> result =
                new ArrayList<TaskMetrics>();
        if (tasks == null) {
            return result;
        }
        for (TaskMetrics task : tasks) {
            if (task.getTaskId().getTaskType() == type) {
                result.add(task);
            }
        }
        return result;
    }

    private static int countTasks(
            List<TaskMetrics> tasks,
            TaskType type) {
        return filterTasks(tasks, type).size();
    }

    private static String firstTable(
            List<TaskMetrics> tasks,
            TaskType type) {

        if (tasks == null) {
            return "-";
        }
        for (TaskMetrics task : tasks) {
            if (task.getTaskId().getTaskType() == type
                    && task.getCurrentTable() != null
                    && !task.getCurrentTable().trim().isEmpty()) {
                return task.getCurrentTable();
            }
        }
        return "-";
    }

    private static List<JobSnapshot.Task> taskViews(
            List<TaskMetrics> tasks) {

        List<TaskMetrics> ordered =
                new ArrayList<TaskMetrics>(
                        tasks == null
                                ? Collections.<TaskMetrics>emptyList()
                                : tasks);
        Collections.sort(
                ordered,
                new Comparator<TaskMetrics>() {
                    @Override
                    public int compare(
                            TaskMetrics left,
                            TaskMetrics right) {
                        int type =
                                left.getTaskId()
                                        .getTaskType()
                                        .compareTo(
                                                right.getTaskId()
                                                        .getTaskType());
                        if (type != 0) {
                            return type;
                        }
                        return Integer.compare(
                                left.getTaskId().getSubtaskIndex(),
                                right.getTaskId().getSubtaskIndex());
                    }
                });

        List<JobSnapshot.Task> result =
                new ArrayList<JobSnapshot.Task>();
        for (TaskMetrics task : ordered) {
            TaskId taskId = task.getTaskId();
            result.add(
                    new JobSnapshot.Task(
                            taskId.toString(),
                            taskId.getPipelineId(),
                            taskId.getTaskType().name(),
                            task.getState().name(),
                            taskId.getSubtaskIndex(),
                            taskId.getParallelism(),
                            task.getBatchCount(),
                            task.getReceivedBatchCount(),
                            task.getAttemptedRecordCount(),
                            task.getRecordCount(),
                            task.getFailedRecordCount(),
                            task.getUnknownStateRecordCount(),
                            task.getCompletedSplitCount(),
                            task.getAverageQps(),
                            task.getDurationMillis(),
                            task.getCurrentTable(),
                            task.getCurrentSplit()));
        }
        return result;
    }

    private static List<JobSnapshot.Channel> channelViews(
            String pipelineId,
            JobMetrics metrics) {

        List<JobSnapshot.Channel> result =
                new ArrayList<JobSnapshot.Channel>();
        if (metrics == null) {
            return result;
        }

        for (ChannelMetrics channel : metrics.getChannelMetrics()) {
            if (channel.getChannelId() == null
                    || !channel.getChannelId()
                    .startsWith(pipelineId + "-")) {
                continue;
            }
            result.add(
                    new JobSnapshot.Channel(
                            channel.getChannelId(),
                            channel.getEnqueuedCount(),
                            channel.getDequeuedCount(),
                            channel.getCurrentBatches(),
                            channel.getCurrentRecords(),
                            channel.getCurrentBytes(),
                            channel.getMaximumBatches(),
                            channel.getMaximumRecords(),
                            channel.getMaximumBytes(),
                            channel.getOversizedBatches(),
                            channel.getProducerBackpressureRatio(),
                            channel.getConsumerIdleRatio(),
                            channel.getRateLimitedRatio()));
        }
        return result;
    }

    private static JobSnapshot.Metrics metrics(
            JobMetrics metrics) {

        if (metrics == null) {
            return new JobSnapshot.Metrics(
                    0L, 0L, 0L, 0D,
                    0L, 0L, 0L, 0L, 0D,
                    0L, 0L, 0L, 0L,
                    0L, 0L, 0L, 0L, 0L,
                    0L, 0L,
                    0D, 0D, 0D);
        }

        return new JobSnapshot.Metrics(
                metrics.getSourceBatchCount(),
                metrics.getSourceRecordCount(),
                metrics.getSourceReadBytes(),
                metrics.getSourceAverageQps(),
                metrics.getSinkReceivedBatchCount(),
                metrics.getSinkAttemptedRecordCount(),
                metrics.getSinkRecordCount(),
                metrics.getSinkWrittenBytes(),
                metrics.getSinkAverageQps(),
                metrics.getFailedRecordCount(),
                metrics.getSkippedRecordCount(),
                metrics.getUnknownStateRecordCount(),
                metrics.getBatchRetryCount(),
                metrics.getTotalSplitCount(),
                metrics.getCompletedSplitCount(),
                metrics.getPendingSplitCount(),
                metrics.getRunningSplitCount(),
                metrics.getFailedSplitCount(),
                metrics.getDatabaseCommitMillis(),
                metrics.getSqlExecutionMillis(),
                metrics.getProducerBackpressureRatio(),
                metrics.getConsumerIdleRatio(),
                metrics.getRateLimitedRatio());
    }

    private static JobSnapshot.Commit commit(
            CommitSummary summary) {

        if (summary == null) {
            return new JobSnapshot.Commit(
                    0, 0, 0, 0, 0, 0,
                    0L, 0L, 0L, 0L, 0L,
                    false,
                    false,
                    null,
                    null);
        }

        return new JobSnapshot.Commit(
                summary.getTotalTaskCount(),
                summary.getFinishedTaskCount(),
                summary.getCommittedTaskCount(),
                summary.getDataCommittedTaskCount(),
                summary.getEmptyCommittedTaskCount(),
                summary.getFailedOrUncommittedTaskCount(),
                summary.getAttemptedRecordCount(),
                summary.getSuccessfullyWrittenRecordCount(),
                summary.getSuccessfullyCommittedRecordCount(),
                summary.getFailedRecordCount(),
                summary.getUnknownStateRecordCount(),
                summary.isPartialTaskCommit(),
                summary.isPartialDataCommit(),
                summary.getCommitScope().name(),
                summary.getRetryAdvice());
    }

    private static long duration(
            long startTimeMillis,
            long endTimeMillis) {
        if (startTimeMillis <= 0L) {
            return 0L;
        }
        long end =
                endTimeMillis > 0L
                        ? endTimeMillis
                        : System.currentTimeMillis();
        return Math.max(0L, end - startTimeMillis);
    }

    private static String display(String value) {
        return value == null || value.trim().isEmpty()
                ? "-"
                : value;
    }

    private static String safeMessage(
            Throwable failure) {
        String message = failure.getMessage();
        if (message == null || message.trim().isEmpty()) {
            message = failure.getClass().getSimpleName();
        }
        message = message.replace('\r', ' ').replace('\n', ' ');
        return message.length() <= 500
                ? message
                : message.substring(0, 500);
    }
}
