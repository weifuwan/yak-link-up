package com.link.up.server.runtime;

import com.link.up.api.sink.TableDdl;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.PipelineResult;
import com.link.up.server.domain.JobExecutionAttempt;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Worker protocol metadata, lifecycle audit and execution-attempt history. */
public final class JobExecutionMetadata {

    private final String externalExecutionId;
    private final String idempotencyKey;
    private final int definitionVersion;
    private final String configDigest;
    private final long submittedTimeMillis;
    private final long queuedTimeMillis;
    private final long stateVersion;
    private final long checkpointVersion;
    private final boolean cancellationRequested;
    private final List<JobStateTransition> transitions;
    private final Map<String, TableDdl> tableDdlsByPipelineId;
    private final String runId;
    private final String jobLogFile;
    private final List<JobAttemptMetadata> attempts;

    public JobExecutionMetadata(
            String externalExecutionId,
            String idempotencyKey,
            int definitionVersion,
            String configDigest,
            long submittedTimeMillis,
            long queuedTimeMillis,
            long stateVersion,
            boolean cancellationRequested,
            List<JobStateTransition> transitions) {
        this(
                externalExecutionId,
                idempotencyKey,
                definitionVersion,
                configDigest,
                submittedTimeMillis,
                queuedTimeMillis,
                stateVersion,
                stateVersion,
                cancellationRequested,
                transitions,
                Collections.<String, TableDdl>emptyMap(),
                null,
                null,
                Collections.<JobAttemptMetadata>emptyList());
    }

    public JobExecutionMetadata(
            String externalExecutionId,
            String idempotencyKey,
            int definitionVersion,
            String configDigest,
            long submittedTimeMillis,
            long queuedTimeMillis,
            long stateVersion,
            boolean cancellationRequested,
            List<JobStateTransition> transitions,
            Map<String, TableDdl> tableDdlsByPipelineId) {
        this(
                externalExecutionId,
                idempotencyKey,
                definitionVersion,
                configDigest,
                submittedTimeMillis,
                queuedTimeMillis,
                stateVersion,
                stateVersion,
                cancellationRequested,
                transitions,
                tableDdlsByPipelineId,
                null,
                null,
                Collections.<JobAttemptMetadata>emptyList());
    }

    public JobExecutionMetadata(
            String externalExecutionId,
            String idempotencyKey,
            int definitionVersion,
            String configDigest,
            long submittedTimeMillis,
            long queuedTimeMillis,
            long stateVersion,
            boolean cancellationRequested,
            List<JobStateTransition> transitions,
            Map<String, TableDdl> tableDdlsByPipelineId,
            String runId,
            String jobLogFile) {
        this(
                externalExecutionId,
                idempotencyKey,
                definitionVersion,
                configDigest,
                submittedTimeMillis,
                queuedTimeMillis,
                stateVersion,
                stateVersion,
                cancellationRequested,
                transitions,
                tableDdlsByPipelineId,
                runId,
                jobLogFile,
                Collections.<JobAttemptMetadata>emptyList());
    }

    public JobExecutionMetadata(
            String externalExecutionId,
            String idempotencyKey,
            int definitionVersion,
            String configDigest,
            long submittedTimeMillis,
            long queuedTimeMillis,
            long stateVersion,
            long checkpointVersion,
            boolean cancellationRequested,
            List<JobStateTransition> transitions,
            Map<String, TableDdl> tableDdlsByPipelineId,
            String runId,
            String jobLogFile,
            List<JobAttemptMetadata> attempts) {

        this.externalExecutionId = externalExecutionId;
        this.idempotencyKey = idempotencyKey;
        this.definitionVersion = definitionVersion;
        this.configDigest = configDigest;
        this.submittedTimeMillis = submittedTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.stateVersion = stateVersion;
        this.checkpointVersion = checkpointVersion;
        this.cancellationRequested = cancellationRequested;
        this.transitions = Collections.unmodifiableList(
                new ArrayList<JobStateTransition>(transitions));
        this.tableDdlsByPipelineId = Collections.unmodifiableMap(
                new LinkedHashMap<String, TableDdl>(tableDdlsByPipelineId));
        this.runId = runId;
        this.jobLogFile = jobLogFile;
        this.attempts = Collections.unmodifiableList(
                new ArrayList<JobAttemptMetadata>(attempts));
    }

    public static JobExecutionMetadata fromState(
            JobExecutionState state) {

        Map<String, TableDdl> tableDdls =
                new LinkedHashMap<String, TableDdl>();
        JobResult result = state.getResult();
        if (result != null) {
            for (PipelineResult pipelineResult : result.getPipelineResults()) {
                if (pipelineResult.getTableDdl() != null) {
                    tableDdls.put(
                            pipelineResult.getPipelineId(),
                            pipelineResult.getTableDdl());
                }
            }
        }

        List<JobAttemptMetadata> attempts =
                new ArrayList<JobAttemptMetadata>();
        for (JobExecutionAttempt attempt : state.getAttempts()) {
            attempts.add(JobAttemptMetadata.from(attempt));
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
                state.getCheckpointVersion(),
                state.isCancellationRequested(),
                state.getTransitions(),
                tableDdls,
                state.getRunId(),
                state.getJobLogFile(),
                attempts);
    }

    public JobExecutionMetadata recoverLost(
            ServerJobStatus previousStatus,
            long recoveryTimeMillis,
            String reason) {

        if (previousStatus == null || previousStatus.isTerminal()) {
            return this;
        }

        List<JobStateTransition> recoveredTransitions =
                new ArrayList<JobStateTransition>(transitions);
        recoveredTransitions.add(
                new JobStateTransition(
                        stateVersion + 1L,
                        previousStatus,
                        ServerJobStatus.LOST,
                        recoveryTimeMillis,
                        "worker-restart-recovery"));

        List<JobAttemptMetadata> recoveredAttempts =
                new ArrayList<JobAttemptMetadata>(attempts.size());
        for (JobAttemptMetadata attempt : attempts) {
            recoveredAttempts.add(
                    attempt.recoverLost(recoveryTimeMillis, reason));
        }

        return new JobExecutionMetadata(
                externalExecutionId,
                idempotencyKey,
                definitionVersion,
                configDigest,
                submittedTimeMillis,
                queuedTimeMillis,
                stateVersion + 1L,
                checkpointVersion + 1L,
                cancellationRequested,
                recoveredTransitions,
                tableDdlsByPipelineId,
                runId,
                jobLogFile,
                recoveredAttempts);
    }

    public String getExternalExecutionId() { return externalExecutionId; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public int getDefinitionVersion() { return definitionVersion; }
    public String getConfigDigest() { return configDigest; }
    public long getSubmittedTimeMillis() { return submittedTimeMillis; }
    public long getQueuedTimeMillis() { return queuedTimeMillis; }
    public long getStateVersion() { return stateVersion; }
    public long getCheckpointVersion() { return checkpointVersion; }
    public boolean isCancellationRequested() { return cancellationRequested; }
    public List<JobStateTransition> getTransitions() { return transitions; }
    public Map<String, TableDdl> getTableDdlsByPipelineId() { return tableDdlsByPipelineId; }
    public TableDdl getTableDdl(String pipelineId) { return tableDdlsByPipelineId.get(pipelineId); }
    public String getRunId() { return runId; }
    public String getJobLogFile() { return jobLogFile; }
    public List<JobAttemptMetadata> getAttempts() { return attempts; }
    public int getAttemptCount() { return attempts.size(); }
}
