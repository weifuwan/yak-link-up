package com.link.up.server.runtime.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.link.up.server.runtime.JobSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Secret-safe execution facts captured at a durable Job checkpoint.
 *
 * <p>This model intentionally contains only stable identities, statuses and
 * numeric execution facts. Connector table locations, current split details,
 * SQL, log paths, exception messages and arbitrary payload maps are excluded.
 * It can therefore be stored inside the append-only Job event journal.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobExecutionFacts {

    private final Metrics metrics;
    private final Commit commitSummary;
    private final List<Pipeline> pipelines;
    private final List<Task> tasks;

    @JsonCreator
    public JobExecutionFacts(
            @JsonProperty("metrics") Metrics metrics,
            @JsonProperty("commitSummary") Commit commitSummary,
            @JsonProperty("pipelines") List<Pipeline> pipelines,
            @JsonProperty("tasks") List<Task> tasks) {

        this.metrics = Objects.requireNonNull(
                metrics,
                "metrics must not be null");
        this.commitSummary = commitSummary;
        this.pipelines = immutable(
                pipelines,
                "pipelines");
        this.tasks = immutable(
                tasks,
                "tasks");
    }

    public static JobExecutionFacts from(
            JobSnapshot snapshot) {

        JobSnapshot safeSnapshot = Objects.requireNonNull(
                snapshot,
                "snapshot must not be null");
        List<Pipeline> pipelines = new ArrayList<Pipeline>();
        List<Task> tasks = new ArrayList<Task>();

        for (JobSnapshot.Pipeline pipeline :
                safeSnapshot.getPipelines()) {
            pipelines.add(Pipeline.from(pipeline));
            for (JobSnapshot.Task task : pipeline.getTasks()) {
                tasks.add(Task.from(task));
            }
        }

        return new JobExecutionFacts(
                Metrics.from(safeSnapshot.getMetrics()),
                Commit.from(safeSnapshot.getCommitSummary()),
                pipelines,
                tasks);
    }

    public boolean hasExecutionDetails() {
        return !pipelines.isEmpty() || !tasks.isEmpty();
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public Commit getCommitSummary() {
        return commitSummary;
    }

    public List<Pipeline> getPipelines() {
        return pipelines;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    private static <T> List<T> immutable(
            List<T> values,
            String name) {

        if (values == null) {
            return Collections.emptyList();
        }

        List<T> copy = new ArrayList<T>(values.size());
        for (T value : values) {
            copy.add(
                    Objects.requireNonNull(
                            value,
                            name + " must not contain null"));
        }
        return Collections.unmodifiableList(copy);
    }

    /** Aggregated numeric facts useful for History UI scorecards. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Metrics {

        private final long sourceRecordCount;
        private final long sourceReadBytes;
        private final double sourceAverageQps;
        private final long sinkAttemptedRecordCount;
        private final long sinkSuccessRecordCount;
        private final long sinkWrittenBytes;
        private final double sinkAverageQps;
        private final long failedRecordCount;
        private final long skippedRecordCount;
        private final long unknownStateRecordCount;
        private final long totalSplitCount;
        private final long completedSplitCount;
        private final long failedSplitCount;
        private final long databaseCommitMillis;
        private final long sqlExecutionMillis;
        private final double producerBackpressureRatio;
        private final double consumerIdleRatio;
        private final double rateLimitedRatio;

        @JsonCreator
        public Metrics(
                @JsonProperty("sourceRecordCount") long sourceRecordCount,
                @JsonProperty("sourceReadBytes") long sourceReadBytes,
                @JsonProperty("sourceAverageQps") double sourceAverageQps,
                @JsonProperty("sinkAttemptedRecordCount") long sinkAttemptedRecordCount,
                @JsonProperty("sinkSuccessRecordCount") long sinkSuccessRecordCount,
                @JsonProperty("sinkWrittenBytes") long sinkWrittenBytes,
                @JsonProperty("sinkAverageQps") double sinkAverageQps,
                @JsonProperty("failedRecordCount") long failedRecordCount,
                @JsonProperty("skippedRecordCount") long skippedRecordCount,
                @JsonProperty("unknownStateRecordCount") long unknownStateRecordCount,
                @JsonProperty("totalSplitCount") long totalSplitCount,
                @JsonProperty("completedSplitCount") long completedSplitCount,
                @JsonProperty("failedSplitCount") long failedSplitCount,
                @JsonProperty("databaseCommitMillis") long databaseCommitMillis,
                @JsonProperty("sqlExecutionMillis") long sqlExecutionMillis,
                @JsonProperty("producerBackpressureRatio") double producerBackpressureRatio,
                @JsonProperty("consumerIdleRatio") double consumerIdleRatio,
                @JsonProperty("rateLimitedRatio") double rateLimitedRatio) {

            this.sourceRecordCount = sourceRecordCount;
            this.sourceReadBytes = sourceReadBytes;
            this.sourceAverageQps = sourceAverageQps;
            this.sinkAttemptedRecordCount = sinkAttemptedRecordCount;
            this.sinkSuccessRecordCount = sinkSuccessRecordCount;
            this.sinkWrittenBytes = sinkWrittenBytes;
            this.sinkAverageQps = sinkAverageQps;
            this.failedRecordCount = failedRecordCount;
            this.skippedRecordCount = skippedRecordCount;
            this.unknownStateRecordCount = unknownStateRecordCount;
            this.totalSplitCount = totalSplitCount;
            this.completedSplitCount = completedSplitCount;
            this.failedSplitCount = failedSplitCount;
            this.databaseCommitMillis = databaseCommitMillis;
            this.sqlExecutionMillis = sqlExecutionMillis;
            this.producerBackpressureRatio = producerBackpressureRatio;
            this.consumerIdleRatio = consumerIdleRatio;
            this.rateLimitedRatio = rateLimitedRatio;
        }

        private static Metrics from(JobSnapshot.Metrics metrics) {
            JobSnapshot.Metrics safe = Objects.requireNonNull(
                    metrics,
                    "snapshot metrics must not be null");
            return new Metrics(
                    safe.getSourceRecordCount(),
                    safe.getSourceReadBytes(),
                    safe.getSourceAverageQps(),
                    safe.getSinkAttemptedRecordCount(),
                    safe.getSinkSuccessRecordCount(),
                    safe.getSinkWrittenBytes(),
                    safe.getSinkAverageQps(),
                    safe.getFailedRecordCount(),
                    safe.getSkippedRecordCount(),
                    safe.getUnknownStateRecordCount(),
                    safe.getTotalSplitCount(),
                    safe.getCompletedSplitCount(),
                    safe.getFailedSplitCount(),
                    safe.getDatabaseCommitMillis(),
                    safe.getSqlExecutionMillis(),
                    safe.getProducerBackpressureRatio(),
                    safe.getConsumerIdleRatio(),
                    safe.getRateLimitedRatio());
        }

        public long getSourceRecordCount() { return sourceRecordCount; }
        public long getSourceReadBytes() { return sourceReadBytes; }
        public double getSourceAverageQps() { return sourceAverageQps; }
        public long getSinkAttemptedRecordCount() { return sinkAttemptedRecordCount; }
        public long getSinkSuccessRecordCount() { return sinkSuccessRecordCount; }
        public long getSinkWrittenBytes() { return sinkWrittenBytes; }
        public double getSinkAverageQps() { return sinkAverageQps; }
        public long getFailedRecordCount() { return failedRecordCount; }
        public long getSkippedRecordCount() { return skippedRecordCount; }
        public long getUnknownStateRecordCount() { return unknownStateRecordCount; }
        public long getTotalSplitCount() { return totalSplitCount; }
        public long getCompletedSplitCount() { return completedSplitCount; }
        public long getFailedSplitCount() { return failedSplitCount; }
        public long getDatabaseCommitMillis() { return databaseCommitMillis; }
        public long getSqlExecutionMillis() { return sqlExecutionMillis; }
        public double getProducerBackpressureRatio() { return producerBackpressureRatio; }
        public double getConsumerIdleRatio() { return consumerIdleRatio; }
        public double getRateLimitedRatio() { return rateLimitedRatio; }
    }

    /** Pipeline-level facts; physical table locations are intentionally omitted. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Pipeline {

        private final String pipelineId;
        private final String dataSetId;
        private final String status;
        private final String sourceConnector;
        private final int sourceTaskCount;
        private final long sourceRecordCount;
        private final long sourceReadBytes;
        private final double sourceAverageQps;
        private final String sinkConnector;
        private final int sinkTaskCount;
        private final long sinkAttemptedRecordCount;
        private final long sinkSuccessRecordCount;
        private final long sinkFailedRecordCount;
        private final long sinkUnknownStateRecordCount;
        private final long sinkWrittenBytes;
        private final double sinkAverageQps;
        private final Commit commitSummary;

        @JsonCreator
        public Pipeline(
                @JsonProperty("pipelineId") String pipelineId,
                @JsonProperty("dataSetId") String dataSetId,
                @JsonProperty("status") String status,
                @JsonProperty("sourceConnector") String sourceConnector,
                @JsonProperty("sourceTaskCount") int sourceTaskCount,
                @JsonProperty("sourceRecordCount") long sourceRecordCount,
                @JsonProperty("sourceReadBytes") long sourceReadBytes,
                @JsonProperty("sourceAverageQps") double sourceAverageQps,
                @JsonProperty("sinkConnector") String sinkConnector,
                @JsonProperty("sinkTaskCount") int sinkTaskCount,
                @JsonProperty("sinkAttemptedRecordCount") long sinkAttemptedRecordCount,
                @JsonProperty("sinkSuccessRecordCount") long sinkSuccessRecordCount,
                @JsonProperty("sinkFailedRecordCount") long sinkFailedRecordCount,
                @JsonProperty("sinkUnknownStateRecordCount") long sinkUnknownStateRecordCount,
                @JsonProperty("sinkWrittenBytes") long sinkWrittenBytes,
                @JsonProperty("sinkAverageQps") double sinkAverageQps,
                @JsonProperty("commitSummary") Commit commitSummary) {

            this.pipelineId = requireText(pipelineId, "pipelineId");
            this.dataSetId = requireText(dataSetId, "dataSetId");
            this.status = requireText(status, "status");
            this.sourceConnector = requireText(sourceConnector, "sourceConnector");
            this.sourceTaskCount = sourceTaskCount;
            this.sourceRecordCount = sourceRecordCount;
            this.sourceReadBytes = sourceReadBytes;
            this.sourceAverageQps = sourceAverageQps;
            this.sinkConnector = requireText(sinkConnector, "sinkConnector");
            this.sinkTaskCount = sinkTaskCount;
            this.sinkAttemptedRecordCount = sinkAttemptedRecordCount;
            this.sinkSuccessRecordCount = sinkSuccessRecordCount;
            this.sinkFailedRecordCount = sinkFailedRecordCount;
            this.sinkUnknownStateRecordCount = sinkUnknownStateRecordCount;
            this.sinkWrittenBytes = sinkWrittenBytes;
            this.sinkAverageQps = sinkAverageQps;
            this.commitSummary = commitSummary;
        }

        private static Pipeline from(JobSnapshot.Pipeline pipeline) {
            JobSnapshot.Source source = pipeline.getSource();
            JobSnapshot.Sink sink = pipeline.getSink();
            return new Pipeline(
                    pipeline.getPipelineId(),
                    pipeline.getDataSetId(),
                    pipeline.getStatus(),
                    source.getConnector(),
                    source.getTaskCount(),
                    source.getRecordCount(),
                    source.getReadBytes(),
                    source.getAverageQps(),
                    sink.getConnector(),
                    sink.getTaskCount(),
                    sink.getAttemptedRecordCount(),
                    sink.getSuccessRecordCount(),
                    sink.getFailedRecordCount(),
                    sink.getUnknownStateRecordCount(),
                    sink.getWrittenBytes(),
                    sink.getAverageQps(),
                    Commit.from(pipeline.getCommitSummary()));
        }

        public String getPipelineId() { return pipelineId; }
        public String getDataSetId() { return dataSetId; }
        public String getStatus() { return status; }
        public String getSourceConnector() { return sourceConnector; }
        public int getSourceTaskCount() { return sourceTaskCount; }
        public long getSourceRecordCount() { return sourceRecordCount; }
        public long getSourceReadBytes() { return sourceReadBytes; }
        public double getSourceAverageQps() { return sourceAverageQps; }
        public String getSinkConnector() { return sinkConnector; }
        public int getSinkTaskCount() { return sinkTaskCount; }
        public long getSinkAttemptedRecordCount() { return sinkAttemptedRecordCount; }
        public long getSinkSuccessRecordCount() { return sinkSuccessRecordCount; }
        public long getSinkFailedRecordCount() { return sinkFailedRecordCount; }
        public long getSinkUnknownStateRecordCount() { return sinkUnknownStateRecordCount; }
        public long getSinkWrittenBytes() { return sinkWrittenBytes; }
        public double getSinkAverageQps() { return sinkAverageQps; }
        public Commit getCommitSummary() { return commitSummary; }
    }

    /** Task-level facts; current table/split values are intentionally omitted. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Task {

        private final String taskId;
        private final String pipelineId;
        private final String taskType;
        private final String state;
        private final int subtaskIndex;
        private final int parallelism;
        private final long batchCount;
        private final long receivedBatchCount;
        private final long attemptedRecordCount;
        private final long recordCount;
        private final long failedRecordCount;
        private final long unknownStateRecordCount;
        private final long completedSplitCount;
        private final double averageQps;
        private final long durationMillis;

        @JsonCreator
        public Task(
                @JsonProperty("taskId") String taskId,
                @JsonProperty("pipelineId") String pipelineId,
                @JsonProperty("taskType") String taskType,
                @JsonProperty("state") String state,
                @JsonProperty("subtaskIndex") int subtaskIndex,
                @JsonProperty("parallelism") int parallelism,
                @JsonProperty("batchCount") long batchCount,
                @JsonProperty("receivedBatchCount") long receivedBatchCount,
                @JsonProperty("attemptedRecordCount") long attemptedRecordCount,
                @JsonProperty("recordCount") long recordCount,
                @JsonProperty("failedRecordCount") long failedRecordCount,
                @JsonProperty("unknownStateRecordCount") long unknownStateRecordCount,
                @JsonProperty("completedSplitCount") long completedSplitCount,
                @JsonProperty("averageQps") double averageQps,
                @JsonProperty("durationMillis") long durationMillis) {

            this.taskId = requireText(taskId, "taskId");
            this.pipelineId = requireText(pipelineId, "pipelineId");
            this.taskType = requireText(taskType, "taskType");
            this.state = requireText(state, "state");
            this.subtaskIndex = subtaskIndex;
            this.parallelism = parallelism;
            this.batchCount = batchCount;
            this.receivedBatchCount = receivedBatchCount;
            this.attemptedRecordCount = attemptedRecordCount;
            this.recordCount = recordCount;
            this.failedRecordCount = failedRecordCount;
            this.unknownStateRecordCount = unknownStateRecordCount;
            this.completedSplitCount = completedSplitCount;
            this.averageQps = averageQps;
            this.durationMillis = durationMillis;
        }

        private static Task from(JobSnapshot.Task task) {
            return new Task(
                    task.getTaskId(),
                    task.getPipelineId(),
                    task.getTaskType(),
                    task.getState(),
                    task.getSubtaskIndex(),
                    task.getParallelism(),
                    task.getBatchCount(),
                    task.getReceivedBatchCount(),
                    task.getAttemptedRecordCount(),
                    task.getRecordCount(),
                    task.getFailedRecordCount(),
                    task.getUnknownStateRecordCount(),
                    task.getCompletedSplitCount(),
                    task.getAverageQps(),
                    task.getDurationMillis());
        }

        public String getTaskId() { return taskId; }
        public String getPipelineId() { return pipelineId; }
        public String getTaskType() { return taskType; }
        public String getState() { return state; }
        public int getSubtaskIndex() { return subtaskIndex; }
        public int getParallelism() { return parallelism; }
        public long getBatchCount() { return batchCount; }
        public long getReceivedBatchCount() { return receivedBatchCount; }
        public long getAttemptedRecordCount() { return attemptedRecordCount; }
        public long getRecordCount() { return recordCount; }
        public long getFailedRecordCount() { return failedRecordCount; }
        public long getUnknownStateRecordCount() { return unknownStateRecordCount; }
        public long getCompletedSplitCount() { return completedSplitCount; }
        public double getAverageQps() { return averageQps; }
        public long getDurationMillis() { return durationMillis; }
    }

    /** Commit evidence without free-form retry advice. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Commit {

        private final int totalTaskCount;
        private final int finishedTaskCount;
        private final int committedTaskCount;
        private final int dataCommittedTaskCount;
        private final int emptyCommittedTaskCount;
        private final int failedOrUncommittedTaskCount;
        private final long attemptedRecordCount;
        private final long successfullyWrittenRecordCount;
        private final long successfullyCommittedRecordCount;
        private final long failedRecordCount;
        private final long unknownStateRecordCount;
        private final boolean partialTaskCommit;
        private final boolean partialDataCommit;
        private final String commitScope;

        @JsonCreator
        public Commit(
                @JsonProperty("totalTaskCount") int totalTaskCount,
                @JsonProperty("finishedTaskCount") int finishedTaskCount,
                @JsonProperty("committedTaskCount") int committedTaskCount,
                @JsonProperty("dataCommittedTaskCount") int dataCommittedTaskCount,
                @JsonProperty("emptyCommittedTaskCount") int emptyCommittedTaskCount,
                @JsonProperty("failedOrUncommittedTaskCount") int failedOrUncommittedTaskCount,
                @JsonProperty("attemptedRecordCount") long attemptedRecordCount,
                @JsonProperty("successfullyWrittenRecordCount") long successfullyWrittenRecordCount,
                @JsonProperty("successfullyCommittedRecordCount") long successfullyCommittedRecordCount,
                @JsonProperty("failedRecordCount") long failedRecordCount,
                @JsonProperty("unknownStateRecordCount") long unknownStateRecordCount,
                @JsonProperty("partialTaskCommit") boolean partialTaskCommit,
                @JsonProperty("partialDataCommit") boolean partialDataCommit,
                @JsonProperty("commitScope") String commitScope) {

            this.totalTaskCount = totalTaskCount;
            this.finishedTaskCount = finishedTaskCount;
            this.committedTaskCount = committedTaskCount;
            this.dataCommittedTaskCount = dataCommittedTaskCount;
            this.emptyCommittedTaskCount = emptyCommittedTaskCount;
            this.failedOrUncommittedTaskCount = failedOrUncommittedTaskCount;
            this.attemptedRecordCount = attemptedRecordCount;
            this.successfullyWrittenRecordCount = successfullyWrittenRecordCount;
            this.successfullyCommittedRecordCount = successfullyCommittedRecordCount;
            this.failedRecordCount = failedRecordCount;
            this.unknownStateRecordCount = unknownStateRecordCount;
            this.partialTaskCommit = partialTaskCommit;
            this.partialDataCommit = partialDataCommit;
            this.commitScope = commitScope;
        }

        private static Commit from(JobSnapshot.Commit commit) {
            if (commit == null) {
                return null;
            }
            return new Commit(
                    commit.getTotalTaskCount(),
                    commit.getFinishedTaskCount(),
                    commit.getCommittedTaskCount(),
                    commit.getDataCommittedTaskCount(),
                    commit.getEmptyCommittedTaskCount(),
                    commit.getFailedOrUncommittedTaskCount(),
                    commit.getAttemptedRecordCount(),
                    commit.getSuccessfullyWrittenRecordCount(),
                    commit.getSuccessfullyCommittedRecordCount(),
                    commit.getFailedRecordCount(),
                    commit.getUnknownStateRecordCount(),
                    commit.isPartialTaskCommit(),
                    commit.isPartialDataCommit(),
                    commit.getCommitScope());
        }

        public int getTotalTaskCount() { return totalTaskCount; }
        public int getFinishedTaskCount() { return finishedTaskCount; }
        public int getCommittedTaskCount() { return committedTaskCount; }
        public int getDataCommittedTaskCount() { return dataCommittedTaskCount; }
        public int getEmptyCommittedTaskCount() { return emptyCommittedTaskCount; }
        public int getFailedOrUncommittedTaskCount() { return failedOrUncommittedTaskCount; }
        public long getAttemptedRecordCount() { return attemptedRecordCount; }
        public long getSuccessfullyWrittenRecordCount() { return successfullyWrittenRecordCount; }
        public long getSuccessfullyCommittedRecordCount() { return successfullyCommittedRecordCount; }
        public long getFailedRecordCount() { return failedRecordCount; }
        public long getUnknownStateRecordCount() { return unknownStateRecordCount; }
        public boolean isPartialTaskCommit() { return partialTaskCommit; }
        public boolean isPartialDataCommit() { return partialDataCommit; }
        public String getCommitScope() { return commitScope; }
    }

    private static String requireText(
            String value,
            String name) {

        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return value.trim();
    }
}
