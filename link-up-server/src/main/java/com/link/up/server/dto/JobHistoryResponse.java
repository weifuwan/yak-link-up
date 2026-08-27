package com.link.up.server.dto;

import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Secret-safe execution history projection for one Worker Job.
 *
 * <p>The projection deliberately joins durable lifecycle events with the
 * latest persisted Attempt/Pipeline/Task read models instead of creating a
 * second event-sourced state machine. Raw exception messages, log paths,
 * connector table locations and current split details stay outside this
 * protocol.</p>
 */
public final class JobHistoryResponse {

    public static final String CURRENT_API_VERSION =
            "link-up-job-history/v1";

    private final String apiVersion;
    private final String jobId;
    private final String jobName;
    private final ServerJobStatus status;
    private final boolean completed;
    private final boolean cancellationRequested;
    private final long createTimeMillis;
    private final long startTimeMillis;
    private final long endTimeMillis;
    private final long durationMillis;
    private final long checkpointVersion;
    private final JobSnapshot.Metrics metrics;
    private final Commit commitSummary;
    private final List<JobEventEnvelope> events;
    private final long nextSequence;
    private final boolean hasMore;
    private final List<Attempt> attempts;
    private final List<Pipeline> pipelines;

    public JobHistoryResponse(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata,
            JobEventPage eventPage) {

        JobSnapshot safeSnapshot = Objects.requireNonNull(
                snapshot,
                "snapshot must not be null");
        JobEventPage safeEventPage = Objects.requireNonNull(
                eventPage,
                "eventPage must not be null");

        if (!safeSnapshot.getJobId().equals(
                safeEventPage.getJobId())) {
            throw new IllegalArgumentException(
                    "Job history identity mismatch");
        }

        this.apiVersion = CURRENT_API_VERSION;
        this.jobId = safeSnapshot.getJobId();
        this.jobName = safeSnapshot.getJobName();
        this.status = safeSnapshot.getStatus();
        this.completed = safeSnapshot.getStatus().isTerminal();
        this.cancellationRequested = metadata != null
                && metadata.isCancellationRequested();
        this.createTimeMillis = safeSnapshot.getCreateTimeMillis();
        this.startTimeMillis = safeSnapshot.getStartTimeMillis();
        this.endTimeMillis = safeSnapshot.getEndTimeMillis();
        this.durationMillis = safeSnapshot.getDurationMillis();
        this.checkpointVersion = metadata == null
                ? 0L
                : metadata.getCheckpointVersion();
        this.metrics = safeSnapshot.getMetrics();
        this.commitSummary = Commit.from(
                safeSnapshot.getCommitSummary());
        this.events = Collections.unmodifiableList(
                new ArrayList<JobEventEnvelope>(
                        safeEventPage.getItems()));
        this.nextSequence = safeEventPage.getNextSequence();
        this.hasMore = safeEventPage.isHasMore();
        this.attempts = attempts(metadata);
        this.pipelines = pipelines(
                safeSnapshot.getPipelines());
    }

    private static List<Attempt> attempts(
            JobExecutionMetadata metadata) {

        if (metadata == null) {
            return Collections.emptyList();
        }

        List<Attempt> result = new ArrayList<Attempt>();
        for (JobAttemptMetadata attempt : metadata.getAttempts()) {
            result.add(Attempt.from(attempt));
        }
        return Collections.unmodifiableList(result);
    }

    private static List<Pipeline> pipelines(
            List<JobSnapshot.Pipeline> pipelines) {

        List<Pipeline> result = new ArrayList<Pipeline>();
        for (JobSnapshot.Pipeline pipeline : pipelines) {
            result.add(Pipeline.from(pipeline));
        }
        return Collections.unmodifiableList(result);
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public ServerJobStatus getStatus() {
        return status;
    }

    public boolean isCompleted() {
        return completed;
    }

    public boolean isCancellationRequested() {
        return cancellationRequested;
    }

    public long getCreateTimeMillis() {
        return createTimeMillis;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public long getEndTimeMillis() {
        return endTimeMillis;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public long getCheckpointVersion() {
        return checkpointVersion;
    }

    public JobSnapshot.Metrics getMetrics() {
        return metrics;
    }

    public Commit getCommitSummary() {
        return commitSummary;
    }

    public List<JobEventEnvelope> getEvents() {
        return events;
    }

    public long getNextSequence() {
        return nextSequence;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public List<Attempt> getAttempts() {
        return attempts;
    }

    public List<Pipeline> getPipelines() {
        return pipelines;
    }

    /** Stable Attempt history without raw failure text or log locations. */
    public static final class Attempt {

        private final int attemptNumber;
        private final String attemptId;
        private final String status;
        private final long createTimeMillis;
        private final long queuedTimeMillis;
        private final long startTimeMillis;
        private final long endTimeMillis;
        private final String runId;
        private final String failureType;
        private final String errorCode;
        private final String errorCategory;
        private final String errorPhase;
        private final boolean failureRetryable;
        private final String failureRetryScope;
        private final boolean commitEvidenceAvailable;
        private final int dataCommittedTaskCount;
        private final long successfullyCommittedRecordCount;
        private final long unknownStateRecordCount;
        private final boolean partialDataCommit;
        private final String commitScope;

        private Attempt(
                JobAttemptMetadata attempt) {

            this.attemptNumber = attempt.getAttemptNumber();
            this.attemptId = attempt.getAttemptId();
            this.status = attempt.getStatus().name();
            this.createTimeMillis = attempt.getCreateTimeMillis();
            this.queuedTimeMillis = attempt.getQueuedTimeMillis();
            this.startTimeMillis = attempt.getStartTimeMillis();
            this.endTimeMillis = attempt.getEndTimeMillis();
            this.runId = attempt.getRunId();
            this.failureType = attempt.getFailureType();
            this.errorCode = attempt.getErrorCode();
            this.errorCategory = attempt.getErrorCategory();
            this.errorPhase = attempt.getErrorPhase();
            this.failureRetryable = attempt.isFailureRetryable();
            this.failureRetryScope = attempt.getFailureRetryScope();
            this.commitEvidenceAvailable =
                    attempt.isCommitEvidenceAvailable();
            this.dataCommittedTaskCount =
                    attempt.getDataCommittedTaskCount();
            this.successfullyCommittedRecordCount =
                    attempt.getSuccessfullyCommittedRecordCount();
            this.unknownStateRecordCount =
                    attempt.getUnknownStateRecordCount();
            this.partialDataCommit = attempt.isPartialDataCommit();
            this.commitScope = attempt.getCommitScope();
        }

        private static Attempt from(
                JobAttemptMetadata attempt) {
            return new Attempt(
                    Objects.requireNonNull(
                            attempt,
                            "attempt must not be null"));
        }

        public int getAttemptNumber() {
            return attemptNumber;
        }

        public String getAttemptId() {
            return attemptId;
        }

        public String getStatus() {
            return status;
        }

        public long getCreateTimeMillis() {
            return createTimeMillis;
        }

        public long getQueuedTimeMillis() {
            return queuedTimeMillis;
        }

        public long getStartTimeMillis() {
            return startTimeMillis;
        }

        public long getEndTimeMillis() {
            return endTimeMillis;
        }

        public String getRunId() {
            return runId;
        }

        public String getFailureType() {
            return failureType;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public String getErrorCategory() {
            return errorCategory;
        }

        public String getErrorPhase() {
            return errorPhase;
        }

        public boolean isFailureRetryable() {
            return failureRetryable;
        }

        public String getFailureRetryScope() {
            return failureRetryScope;
        }

        public boolean isCommitEvidenceAvailable() {
            return commitEvidenceAvailable;
        }

        public int getDataCommittedTaskCount() {
            return dataCommittedTaskCount;
        }

        public long getSuccessfullyCommittedRecordCount() {
            return successfullyCommittedRecordCount;
        }

        public long getUnknownStateRecordCount() {
            return unknownStateRecordCount;
        }

        public boolean isPartialDataCommit() {
            return partialDataCommit;
        }

        public String getCommitScope() {
            return commitScope;
        }
    }

    /** Safe Pipeline projection with numeric execution facts only. */
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
        private final List<Task> tasks;
        private final List<Channel> channels;

        private Pipeline(JobSnapshot.Pipeline pipeline) {
            JobSnapshot.Source source = pipeline.getSource();
            JobSnapshot.Sink sink = pipeline.getSink();

            this.pipelineId = pipeline.getPipelineId();
            this.dataSetId = pipeline.getDataSetId();
            this.status = pipeline.getStatus();
            this.sourceConnector = source.getConnector();
            this.sourceTaskCount = source.getTaskCount();
            this.sourceRecordCount = source.getRecordCount();
            this.sourceReadBytes = source.getReadBytes();
            this.sourceAverageQps = source.getAverageQps();
            this.sinkConnector = sink.getConnector();
            this.sinkTaskCount = sink.getTaskCount();
            this.sinkAttemptedRecordCount =
                    sink.getAttemptedRecordCount();
            this.sinkSuccessRecordCount = sink.getSuccessRecordCount();
            this.sinkFailedRecordCount = sink.getFailedRecordCount();
            this.sinkUnknownStateRecordCount =
                    sink.getUnknownStateRecordCount();
            this.sinkWrittenBytes = sink.getWrittenBytes();
            this.sinkAverageQps = sink.getAverageQps();
            this.commitSummary = Commit.from(
                    pipeline.getCommitSummary());
            this.tasks = tasks(pipeline.getTasks());
            this.channels = channels(pipeline.getChannels());
        }

        private static Pipeline from(JobSnapshot.Pipeline pipeline) {
            return new Pipeline(
                    Objects.requireNonNull(
                            pipeline,
                            "pipeline must not be null"));
        }

        private static List<Task> tasks(
                List<JobSnapshot.Task> tasks) {
            List<Task> result = new ArrayList<Task>();
            for (JobSnapshot.Task task : tasks) {
                result.add(Task.from(task));
            }
            return Collections.unmodifiableList(result);
        }

        private static List<Channel> channels(
                List<JobSnapshot.Channel> channels) {
            List<Channel> result = new ArrayList<Channel>();
            for (JobSnapshot.Channel channel : channels) {
                result.add(Channel.from(channel));
            }
            return Collections.unmodifiableList(result);
        }

        public String getPipelineId() {
            return pipelineId;
        }

        public String getDataSetId() {
            return dataSetId;
        }

        public String getStatus() {
            return status;
        }

        public String getSourceConnector() {
            return sourceConnector;
        }

        public int getSourceTaskCount() {
            return sourceTaskCount;
        }

        public long getSourceRecordCount() {
            return sourceRecordCount;
        }

        public long getSourceReadBytes() {
            return sourceReadBytes;
        }

        public double getSourceAverageQps() {
            return sourceAverageQps;
        }

        public String getSinkConnector() {
            return sinkConnector;
        }

        public int getSinkTaskCount() {
            return sinkTaskCount;
        }

        public long getSinkAttemptedRecordCount() {
            return sinkAttemptedRecordCount;
        }

        public long getSinkSuccessRecordCount() {
            return sinkSuccessRecordCount;
        }

        public long getSinkFailedRecordCount() {
            return sinkFailedRecordCount;
        }

        public long getSinkUnknownStateRecordCount() {
            return sinkUnknownStateRecordCount;
        }

        public long getSinkWrittenBytes() {
            return sinkWrittenBytes;
        }

        public double getSinkAverageQps() {
            return sinkAverageQps;
        }

        public Commit getCommitSummary() {
            return commitSummary;
        }

        public List<Task> getTasks() {
            return tasks;
        }

        public List<Channel> getChannels() {
            return channels;
        }
    }

    /** Task projection intentionally excludes currentTable/currentSplit. */
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

        private Task(JobSnapshot.Task task) {
            this.taskId = task.getTaskId();
            this.pipelineId = task.getPipelineId();
            this.taskType = task.getTaskType();
            this.state = task.getState();
            this.subtaskIndex = task.getSubtaskIndex();
            this.parallelism = task.getParallelism();
            this.batchCount = task.getBatchCount();
            this.receivedBatchCount = task.getReceivedBatchCount();
            this.attemptedRecordCount = task.getAttemptedRecordCount();
            this.recordCount = task.getRecordCount();
            this.failedRecordCount = task.getFailedRecordCount();
            this.unknownStateRecordCount = task.getUnknownStateRecordCount();
            this.completedSplitCount = task.getCompletedSplitCount();
            this.averageQps = task.getAverageQps();
            this.durationMillis = task.getDurationMillis();
        }

        private static Task from(JobSnapshot.Task task) {
            return new Task(
                    Objects.requireNonNull(
                            task,
                            "task must not be null"));
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

    /** Channel pressure projection with no connector payload. */
    public static final class Channel {

        private final String channelId;
        private final long enqueuedCount;
        private final long dequeuedCount;
        private final long currentBatches;
        private final long currentRecords;
        private final long currentBytes;
        private final long maximumBatches;
        private final long maximumRecords;
        private final long maximumBytes;
        private final long oversizedBatches;
        private final double producerBackpressureRatio;
        private final double consumerIdleRatio;
        private final double rateLimitedRatio;

        private Channel(JobSnapshot.Channel channel) {
            this.channelId = channel.getChannelId();
            this.enqueuedCount = channel.getEnqueuedCount();
            this.dequeuedCount = channel.getDequeuedCount();
            this.currentBatches = channel.getCurrentBatches();
            this.currentRecords = channel.getCurrentRecords();
            this.currentBytes = channel.getCurrentBytes();
            this.maximumBatches = channel.getMaximumBatches();
            this.maximumRecords = channel.getMaximumRecords();
            this.maximumBytes = channel.getMaximumBytes();
            this.oversizedBatches = channel.getOversizedBatches();
            this.producerBackpressureRatio =
                    channel.getProducerBackpressureRatio();
            this.consumerIdleRatio = channel.getConsumerIdleRatio();
            this.rateLimitedRatio = channel.getRateLimitedRatio();
        }

        private static Channel from(JobSnapshot.Channel channel) {
            return new Channel(
                    Objects.requireNonNull(
                            channel,
                            "channel must not be null"));
        }

        public String getChannelId() { return channelId; }
        public long getEnqueuedCount() { return enqueuedCount; }
        public long getDequeuedCount() { return dequeuedCount; }
        public long getCurrentBatches() { return currentBatches; }
        public long getCurrentRecords() { return currentRecords; }
        public long getCurrentBytes() { return currentBytes; }
        public long getMaximumBatches() { return maximumBatches; }
        public long getMaximumRecords() { return maximumRecords; }
        public long getMaximumBytes() { return maximumBytes; }
        public long getOversizedBatches() { return oversizedBatches; }
        public double getProducerBackpressureRatio() { return producerBackpressureRatio; }
        public double getConsumerIdleRatio() { return consumerIdleRatio; }
        public double getRateLimitedRatio() { return rateLimitedRatio; }
    }

    /** Commit evidence projection intentionally excludes retryAdvice text. */
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

        private Commit(JobSnapshot.Commit commit) {
            this.totalTaskCount = commit.getTotalTaskCount();
            this.finishedTaskCount = commit.getFinishedTaskCount();
            this.committedTaskCount = commit.getCommittedTaskCount();
            this.dataCommittedTaskCount = commit.getDataCommittedTaskCount();
            this.emptyCommittedTaskCount = commit.getEmptyCommittedTaskCount();
            this.failedOrUncommittedTaskCount =
                    commit.getFailedOrUncommittedTaskCount();
            this.attemptedRecordCount = commit.getAttemptedRecordCount();
            this.successfullyWrittenRecordCount =
                    commit.getSuccessfullyWrittenRecordCount();
            this.successfullyCommittedRecordCount =
                    commit.getSuccessfullyCommittedRecordCount();
            this.failedRecordCount = commit.getFailedRecordCount();
            this.unknownStateRecordCount = commit.getUnknownStateRecordCount();
            this.partialTaskCommit = commit.isPartialTaskCommit();
            this.partialDataCommit = commit.isPartialDataCommit();
            this.commitScope = commit.getCommitScope();
        }

        private static Commit from(JobSnapshot.Commit commit) {
            return commit == null ? null : new Commit(commit);
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
}
