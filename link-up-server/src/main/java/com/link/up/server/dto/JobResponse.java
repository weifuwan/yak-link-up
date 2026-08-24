package com.link.up.server.dto;

import com.link.up.api.sink.TableDdl;
import com.link.up.server.application.JobRetryDecision;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobStateTransition;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.WorkerIdentity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Stable offline-job protocol view returned to the control plane. */
public final class JobResponse {

    private final JobSnapshot snapshot;
    private final JobExecutionMetadata metadata;
    private final WorkerIdentity worker;
    private final Retry retry;

    public JobResponse(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata,
            WorkerIdentity worker) {
        this(snapshot, metadata, worker, null);
    }

    public JobResponse(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata,
            WorkerIdentity worker,
            JobRetryDecision retryDecision) {
        this.snapshot = snapshot;
        this.metadata = metadata;
        this.worker = worker;
        this.retry = retryDecision == null
                ? null
                : Retry.from(retryDecision);
    }

    public JobSnapshot.Summary toSummary() { return snapshot.toSummary(); }
    public String getJobId() { return snapshot.getJobId(); }
    public String getExternalExecutionId() { return metadata == null ? null : metadata.getExternalExecutionId(); }
    public String getIdempotencyKey() { return metadata == null ? null : metadata.getIdempotencyKey(); }
    public String getJobName() { return snapshot.getJobName(); }
    public int getDefinitionVersion() { return metadata == null ? 1 : metadata.getDefinitionVersion(); }
    public String getWorkerNodeId() { return worker.getNodeId(); }
    public String getWorkerInstanceId() { return worker.getInstanceId(); }
    public ServerJobStatus getStatus() { return snapshot.getStatus(); }
    public long getStateVersion() { return metadata == null ? 0L : metadata.getStateVersion(); }
    public boolean isCancellationRequested() { return metadata != null && metadata.isCancellationRequested(); }
    public long getCreateTimeMillis() { return snapshot.getCreateTimeMillis(); }
    public long getSubmittedTimeMillis() { return metadata == null ? 0L : metadata.getSubmittedTimeMillis(); }
    public long getQueuedTimeMillis() { return metadata == null ? 0L : metadata.getQueuedTimeMillis(); }
    public long getStartTimeMillis() { return snapshot.getStartTimeMillis(); }
    public long getEndTimeMillis() { return snapshot.getEndTimeMillis(); }
    public long getDurationMillis() { return snapshot.getDurationMillis(); }
    public JobSnapshot.Metrics getMetrics() { return snapshot.getMetrics(); }
    public JobSnapshot.Commit getCommitSummary() { return snapshot.getCommitSummary(); }
    public Retry getRetry() { return retry; }

    public List<JobAttemptMetadata> getAttempts() {
        return metadata == null
                ? Collections.<JobAttemptMetadata>emptyList()
                : metadata.getAttempts();
    }

    public int getAttemptCount() {
        return metadata == null ? 0 : metadata.getAttemptCount();
    }

    public List<PipelineResponse> getPipelines() {
        List<PipelineResponse> result = new ArrayList<PipelineResponse>();
        for (JobSnapshot.Pipeline pipeline : snapshot.getPipelines()) {
            TableDdl tableDdl = metadata == null
                    ? null
                    : metadata.getTableDdl(pipeline.getPipelineId());
            result.add(new PipelineResponse(pipeline, tableDdl));
        }
        return Collections.unmodifiableList(result);
    }

    public List<JobStateTransition> getTransitions() {
        return metadata == null
                ? Collections.<JobStateTransition>emptyList()
                : metadata.getTransitions();
    }

    public String getErrorCode() { return snapshot.getErrorCode(); }
    public String getErrorMessage() { return snapshot.getErrorMessage(); }

    /** Additive Phase-8 retry eligibility view. */
    public static final class Retry {
        private final boolean eligible;
        private final String code;
        private final String message;
        private final int nextAttemptNumber;

        private Retry(
                boolean eligible,
                String code,
                String message,
                int nextAttemptNumber) {
            this.eligible = eligible;
            this.code = code;
            this.message = message;
            this.nextAttemptNumber = nextAttemptNumber;
        }

        static Retry from(JobRetryDecision decision) {
            return new Retry(
                    decision.isEligible(),
                    decision.getCode(),
                    decision.getMessage(),
                    decision.getNextAttemptNumber());
        }

        public boolean isEligible() { return eligible; }
        public String getCode() { return code; }
        public String getMessage() { return message; }
        public int getNextAttemptNumber() { return nextAttemptNumber; }
    }
}
