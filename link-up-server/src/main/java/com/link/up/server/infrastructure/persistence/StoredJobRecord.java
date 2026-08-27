package com.link.up.server.infrastructure.persistence;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.link.up.api.sink.TableDdl;
import com.link.up.server.application.port.JobRepositoryEntry;
import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobRecoverySnapshotFactory;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobStateTransition;
import com.link.up.server.runtime.ServerJobStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Versioned JSON persistence model for one Worker Job state record. */
final class StoredJobRecord {

    static final int CURRENT_FORMAT_VERSION = 4;
    static final int MIN_SUPPORTED_FORMAT_VERSION = 1;

    public int formatVersion;
    public String jobId;
    public String jobName;
    public String status;
    public long createTimeMillis;
    public long startTimeMillis;
    public long endTimeMillis;
    public String errorCode;
    public String errorMessage;
    public StoredMetadata metadata;

    public StoredJobRecord() {
    }

    static StoredJobRecord from(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata) {

        StoredJobRecord record = new StoredJobRecord();
        record.formatVersion = CURRENT_FORMAT_VERSION;
        record.jobId = snapshot.getJobId();
        record.jobName = snapshot.getJobName();
        record.status = snapshot.getStatus().name();
        record.createTimeMillis =
                snapshot.getCreateTimeMillis();
        record.startTimeMillis =
                snapshot.getStartTimeMillis();
        record.endTimeMillis =
                snapshot.getEndTimeMillis();
        record.errorCode = snapshot.getErrorCode();
        record.errorMessage = snapshot.getErrorMessage();
        record.metadata = StoredMetadata.from(metadata);
        return record;
    }

    void validateFormat(String source)
            throws IOException {

        if (formatVersion < MIN_SUPPORTED_FORMAT_VERSION
                || formatVersion > CURRENT_FORMAT_VERSION) {
            throw new IOException(
                    "Unsupported Worker state formatVersion="
                            + formatVersion
                            + " in "
                            + source);
        }
    }

    JobRepositoryEntry toEntry() {
        JobSnapshot restoredSnapshot =
                JobRecoverySnapshotFactory.restoreBasic(
                        jobId,
                        jobName,
                        ServerJobStatus.valueOf(status),
                        createTimeMillis,
                        startTimeMillis,
                        endTimeMillis,
                        errorCode,
                        errorMessage);

        return new JobRepositoryEntry(
                restoredSnapshot,
                metadata == null
                        ? null
                        : metadata.toMetadata());
    }

    public static final class StoredMetadata {
        public String externalExecutionId;
        public String idempotencyKey;
        public int definitionVersion;
        public String configDigest;
        public long submittedTimeMillis;
        public long queuedTimeMillis;
        public long stateVersion;

        /** Reads the legacy v1-v3 checkpointVersion field without writing it. */
        @JsonAlias("checkpointVersion")
        public long stateRevision;

        public boolean cancellationRequested;
        public String runId;
        public String jobLogFile;
        public List<StoredTransition> transitions =
                new ArrayList<StoredTransition>();
        public List<StoredAttempt> attempts =
                new ArrayList<StoredAttempt>();

        public StoredMetadata() {
        }

        static StoredMetadata from(
                JobExecutionMetadata metadata) {

            if (metadata == null) {
                return null;
            }

            StoredMetadata stored = new StoredMetadata();
            stored.externalExecutionId =
                    metadata.getExternalExecutionId();
            stored.idempotencyKey =
                    metadata.getIdempotencyKey();
            stored.definitionVersion =
                    metadata.getDefinitionVersion();
            stored.configDigest =
                    metadata.getConfigDigest();
            stored.submittedTimeMillis =
                    metadata.getSubmittedTimeMillis();
            stored.queuedTimeMillis =
                    metadata.getQueuedTimeMillis();
            stored.stateVersion =
                    metadata.getStateVersion();
            stored.stateRevision =
                    metadata.getStateRevision();
            stored.cancellationRequested =
                    metadata.isCancellationRequested();
            stored.runId = metadata.getRunId();
            stored.jobLogFile = metadata.getJobLogFile();

            for (JobStateTransition transition :
                    metadata.getTransitions()) {
                stored.transitions.add(
                        StoredTransition.from(transition));
            }

            for (JobAttemptMetadata attempt :
                    metadata.getAttempts()) {
                stored.attempts.add(
                        StoredAttempt.from(attempt));
            }

            return stored;
        }

        JobExecutionMetadata toMetadata() {
            List<JobStateTransition> restoredTransitions =
                    new ArrayList<JobStateTransition>();

            for (StoredTransition transition : transitions) {
                restoredTransitions.add(
                        transition.toTransition());
            }

            List<JobAttemptMetadata> restoredAttempts =
                    new ArrayList<JobAttemptMetadata>();

            for (StoredAttempt attempt : attempts) {
                restoredAttempts.add(
                        attempt.toAttempt());
            }

            return new JobExecutionMetadata(
                    externalExecutionId,
                    idempotencyKey,
                    definitionVersion,
                    configDigest,
                    submittedTimeMillis,
                    queuedTimeMillis,
                    stateVersion,
                    stateRevision,
                    cancellationRequested,
                    restoredTransitions,
                    Collections.<String, TableDdl>emptyMap(),
                    runId,
                    jobLogFile,
                    restoredAttempts);
        }
    }

    public static final class StoredTransition {
        public long version;
        public String fromStatus;
        public String toStatus;
        public long transitionTimeMillis;
        public String reason;

        public StoredTransition() {
        }

        static StoredTransition from(
                JobStateTransition transition) {

            StoredTransition stored =
                    new StoredTransition();
            stored.version = transition.getVersion();
            stored.fromStatus =
                    transition.getFromStatus() == null
                            ? null
                            : transition.getFromStatus().name();
            stored.toStatus =
                    transition.getToStatus().name();
            stored.transitionTimeMillis =
                    transition.getTransitionTimeMillis();
            stored.reason = transition.getReason();
            return stored;
        }

        JobStateTransition toTransition() {
            return new JobStateTransition(
                    version,
                    fromStatus == null
                            ? null
                            : ServerJobStatus.valueOf(
                                    fromStatus),
                    ServerJobStatus.valueOf(toStatus),
                    transitionTimeMillis,
                    reason);
        }
    }

    public static final class StoredAttempt {
        public int attemptNumber;
        public String attemptId;
        public String status;
        public long createTimeMillis;
        public long queuedTimeMillis;
        public long startTimeMillis;
        public long endTimeMillis;
        public String runId;
        public String jobLogFile;
        public String failureType;
        public String failureMessage;
        public String retryAdvice;
        public String structuredErrorCode;
        public String structuredErrorCategory;
        public String structuredErrorPhase;
        public boolean failureRetryable;
        public String failureRetryScope;
        public boolean commitEvidenceAvailable;
        public int dataCommittedTaskCount;
        public long successfullyCommittedRecordCount;
        public long unknownStateRecordCount;
        public boolean partialDataCommit;
        public String commitScope;

        public StoredAttempt() {
        }

        static StoredAttempt from(
                JobAttemptMetadata attempt) {

            StoredAttempt stored = new StoredAttempt();
            stored.attemptNumber =
                    attempt.getAttemptNumber();
            stored.attemptId = attempt.getAttemptId();
            stored.status = attempt.getStatus().name();
            stored.createTimeMillis =
                    attempt.getCreateTimeMillis();
            stored.queuedTimeMillis =
                    attempt.getQueuedTimeMillis();
            stored.startTimeMillis =
                    attempt.getStartTimeMillis();
            stored.endTimeMillis =
                    attempt.getEndTimeMillis();
            stored.runId = attempt.getRunId();
            stored.jobLogFile = attempt.getJobLogFile();
            stored.failureType = attempt.getFailureType();
            stored.failureMessage =
                    attempt.getFailureMessage();
            stored.retryAdvice = attempt.getRetryAdvice();
            stored.structuredErrorCode =
                    attempt.getErrorCode();
            stored.structuredErrorCategory =
                    attempt.getErrorCategory();
            stored.structuredErrorPhase =
                    attempt.getErrorPhase();
            stored.failureRetryable =
                    attempt.isFailureRetryable();
            stored.failureRetryScope =
                    attempt.getFailureRetryScope();
            stored.commitEvidenceAvailable =
                    attempt.isCommitEvidenceAvailable();
            stored.dataCommittedTaskCount =
                    attempt.getDataCommittedTaskCount();
            stored.successfullyCommittedRecordCount =
                    attempt.getSuccessfullyCommittedRecordCount();
            stored.unknownStateRecordCount =
                    attempt.getUnknownStateRecordCount();
            stored.partialDataCommit =
                    attempt.isPartialDataCommit();
            stored.commitScope = attempt.getCommitScope();
            return stored;
        }

        JobAttemptMetadata toAttempt() {
            return new JobAttemptMetadata(
                    attemptNumber,
                    attemptId,
                    JobAttemptStatus.valueOf(status),
                    createTimeMillis,
                    queuedTimeMillis,
                    startTimeMillis,
                    endTimeMillis,
                    runId,
                    jobLogFile,
                    failureType,
                    failureMessage,
                    retryAdvice,
                    structuredErrorCode,
                    structuredErrorCategory,
                    structuredErrorPhase,
                    failureRetryable,
                    failureRetryScope,
                    commitEvidenceAvailable,
                    dataCommittedTaskCount,
                    successfullyCommittedRecordCount,
                    unknownStateRecordCount,
                    partialDataCommit,
                    commitScope);
        }
    }
}
