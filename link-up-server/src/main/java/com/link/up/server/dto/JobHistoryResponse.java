package com.link.up.server.dto;

import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;
import com.link.up.server.runtime.event.JobExecutionFacts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Yak-Ops-friendly read projection joining lifecycle, Attempt and execution
 * history without turning the event journal into the Job state source of truth.
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
    private final long stateRevision;
    private final List<JobEventEnvelope> events;
    private final long nextSequence;
    private final boolean hasMore;
    private final List<Attempt> attempts;
    private final JobExecutionFacts execution;

    public JobHistoryResponse(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata,
            JobEventPage eventPage) {
        this(
                snapshot,
                metadata,
                eventPage,
                JobExecutionFacts.from(snapshot));
    }

    public JobHistoryResponse(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata,
            JobEventPage eventPage,
            JobExecutionFacts execution) {

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
        this.stateRevision = metadata == null
                ? 0L
                : metadata.getStateRevision();
        this.events = Collections.unmodifiableList(
                new ArrayList<JobEventEnvelope>(
                        safeEventPage.getItems()));
        this.nextSequence = safeEventPage.getNextSequence();
        this.hasMore = safeEventPage.isHasMore();
        this.attempts = attempts(metadata);
        this.execution = Objects.requireNonNull(
                execution,
                "execution must not be null");
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

    public String getApiVersion() { return apiVersion; }
    public String getJobId() { return jobId; }
    public String getJobName() { return jobName; }
    public ServerJobStatus getStatus() { return status; }
    public boolean isCompleted() { return completed; }
    public boolean isCancellationRequested() { return cancellationRequested; }
    public long getCreateTimeMillis() { return createTimeMillis; }
    public long getStartTimeMillis() { return startTimeMillis; }
    public long getEndTimeMillis() { return endTimeMillis; }
    public long getDurationMillis() { return durationMillis; }
    public long getStateRevision() { return stateRevision; }

    /**
     * @deprecated REST compatibility alias. New clients should use
     * {@code stateRevision} because this value is not a data checkpoint.
     */
    @Deprecated
    public long getCheckpointVersion() { return stateRevision; }

    public List<JobEventEnvelope> getEvents() { return events; }
    public long getNextSequence() { return nextSequence; }
    public boolean isHasMore() { return hasMore; }
    public List<Attempt> getAttempts() { return attempts; }
    public JobExecutionFacts getExecution() { return execution; }

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

        private Attempt(JobAttemptMetadata attempt) {
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
            this.commitEvidenceAvailable = attempt.isCommitEvidenceAvailable();
            this.dataCommittedTaskCount = attempt.getDataCommittedTaskCount();
            this.successfullyCommittedRecordCount =
                    attempt.getSuccessfullyCommittedRecordCount();
            this.unknownStateRecordCount = attempt.getUnknownStateRecordCount();
            this.partialDataCommit = attempt.isPartialDataCommit();
            this.commitScope = attempt.getCommitScope();
        }

        private static Attempt from(JobAttemptMetadata attempt) {
            return new Attempt(
                    Objects.requireNonNull(
                            attempt,
                            "attempt must not be null"));
        }

        public int getAttemptNumber() { return attemptNumber; }
        public String getAttemptId() { return attemptId; }
        public String getStatus() { return status; }
        public long getCreateTimeMillis() { return createTimeMillis; }
        public long getQueuedTimeMillis() { return queuedTimeMillis; }
        public long getStartTimeMillis() { return startTimeMillis; }
        public long getEndTimeMillis() { return endTimeMillis; }
        public String getRunId() { return runId; }
        public String getFailureType() { return failureType; }
        public String getErrorCode() { return errorCode; }
        public String getErrorCategory() { return errorCategory; }
        public String getErrorPhase() { return errorPhase; }
        public boolean isFailureRetryable() { return failureRetryable; }
        public String getFailureRetryScope() { return failureRetryScope; }
        public boolean isCommitEvidenceAvailable() { return commitEvidenceAvailable; }
        public int getDataCommittedTaskCount() { return dataCommittedTaskCount; }
        public long getSuccessfullyCommittedRecordCount() { return successfullyCommittedRecordCount; }
        public long getUnknownStateRecordCount() { return unknownStateRecordCount; }
        public boolean isPartialDataCommit() { return partialDataCommit; }
        public String getCommitScope() { return commitScope; }
    }
}
