package com.link.up.server.application;

import com.link.up.framework.job.JobDefinition;
import com.link.up.server.application.port.JobIdGenerator;
import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRuntimeScheduler;
import com.link.up.server.domain.JobExecutionAttempt;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobStateConflictException;
import com.link.up.server.runtime.ServerJobStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-node Worker application facade.
 *
 * <p>The service owns use-case sequencing only. Active state indexing,
 * runtime-event lifecycle handling and startup recovery are delegated to
 * focused application collaborators.</p>
 */
public final class JobApplicationService
        implements JobApplication {

    private final JobRuntimeScheduler runtimeScheduler;
    private final JobRepository repository;
    private final JobIdGenerator jobIdGenerator;
    private final JobSubmissionRegistry submissionRegistry;
    private final JobRetryPolicy retryPolicy;
    private final ActiveJobRegistry activeJobs;
    private final JobRuntimeLifecycle lifecycle;
    private final AtomicBoolean closed =
            new AtomicBoolean(false);

    public JobApplicationService(
            JobRuntimeScheduler runtimeScheduler,
            JobRepository repository,
            JobIdGenerator jobIdGenerator) {

        this.runtimeScheduler = Objects.requireNonNull(
                runtimeScheduler,
                "runtimeScheduler must not be null");
        this.repository = Objects.requireNonNull(
                repository,
                "repository must not be null");
        this.jobIdGenerator = Objects.requireNonNull(
                jobIdGenerator,
                "jobIdGenerator must not be null");

        this.submissionRegistry = new JobSubmissionRegistry();
        this.retryPolicy = new JobRetryPolicy();
        this.activeJobs = new ActiveJobRegistry();
        this.lifecycle =
                new JobRuntimeLifecycle(
                        runtimeScheduler,
                        repository,
                        activeJobs);

        new JobRecoveryService(
                repository,
                submissionRegistry)
                .recover();
    }

    @Override
    public JobSnapshot submit(JobDefinition definition) {
        return submit(JobSubmission.legacy(definition));
    }

    @Override
    public synchronized JobSnapshot submit(
            final JobSubmission submission) {

        ensureOpen();
        Objects.requireNonNull(
                submission,
                "submission must not be null");

        JobSnapshot existing =
                findExistingSubmission(submission);

        if (existing != null) {
            return existing;
        }

        final JobExecutionState state =
                new JobExecutionState(
                        jobIdGenerator.nextId(),
                        submission);

        JobExecutionState previous =
                activeJobs.putIfAbsent(state);

        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate jobId: "
                            + state.getJobId());
        }

        try {
            submissionRegistry.register(
                    state.getJobId(),
                    submission);
            state.markSubmitted();
            lifecycle.persistState(state);
            schedule(state, submission);
        } catch (RuntimeException failure) {
            rollbackSubmission(
                    state,
                    submission);
            throw failure;
        }

        return lifecycle.snapshot(state);
    }

    @Override
    public synchronized JobSnapshot retry(
            String jobId,
            JobSubmission submission) {

        ensureOpen();

        String normalizedJobId =
                requireText(
                        jobId,
                        "jobId");

        Objects.requireNonNull(
                submission,
                "submission must not be null");

        if (activeJobs.contains(normalizedJobId)) {
            throw new JobRetryNotAllowedException(
                    retryDecision(normalizedJobId));
        }

        JobSnapshot previousSnapshot =
                repository.get(normalizedJobId);

        if (previousSnapshot == null) {
            throw new JobNotFoundException(
                    normalizedJobId);
        }

        JobExecutionMetadata previousMetadata =
                repository.getMetadata(normalizedJobId);

        JobRetryDecision decision =
                retryPolicy.evaluate(
                        previousSnapshot,
                        previousMetadata);

        if (!decision.isEligible()) {
            throw new JobRetryNotAllowedException(decision);
        }

        validateRetrySubmission(
                previousMetadata,
                submission);

        final JobExecutionState state =
                retryState(
                        normalizedJobId,
                        submission,
                        previousSnapshot,
                        previousMetadata);

        JobExecutionState active =
                activeJobs.putIfAbsent(state);

        if (active != null) {
            throw new JobRetryNotAllowedException(
                    retryPolicy.evaluate(
                            lifecycle.snapshot(active),
                            lifecycle.metadata(active)));
        }

        lifecycle.persistState(state);

        try {
            schedule(state, submission);
        } catch (RuntimeException failure) {
            lifecycle.failBeforeExecutionAndArchive(
                    state,
                    failure);
            throw failure;
        }

        return lifecycle.snapshot(state);
    }

    @Override
    public JobRetryDecision retryDecision(String jobId) {
        String normalizedJobId =
                requireText(
                        jobId,
                        "jobId");

        JobExecutionState active =
                activeJobs.get(normalizedJobId);

        if (active != null) {
            return retryPolicy.evaluate(
                    lifecycle.snapshot(active),
                    lifecycle.metadata(active));
        }

        JobSnapshot snapshot =
                repository.get(normalizedJobId);

        if (snapshot == null) {
            throw new JobNotFoundException(
                    normalizedJobId);
        }

        return retryPolicy.evaluate(
                snapshot,
                repository.getMetadata(normalizedJobId));
    }

    @Override
    public JobSnapshot getJob(String jobId) {
        String normalizedJobId =
                requireText(
                        jobId,
                        "jobId");

        JobExecutionState active =
                activeJobs.get(normalizedJobId);

        if (active != null) {
            return lifecycle.snapshot(active);
        }

        JobSnapshot finished =
                repository.get(normalizedJobId);

        if (finished == null) {
            throw new JobNotFoundException(
                    normalizedJobId);
        }

        return finished;
    }

    @Override
    public JobSnapshot getJobByExternalExecutionId(
            String externalExecutionId) {

        String externalId =
                requireText(
                        externalExecutionId,
                        "externalExecutionId");

        String jobId =
                submissionRegistry.findByExternalExecutionId(
                        externalId);

        if (jobId == null) {
            throw new JobNotFoundException(
                    externalId);
        }

        return getJob(jobId);
    }

    @Override
    public JobExecutionMetadata getMetadata(String jobId) {
        String normalizedJobId =
                requireText(
                        jobId,
                        "jobId");

        JobExecutionState active =
                activeJobs.get(normalizedJobId);

        return active != null
                ? lifecycle.metadata(active)
                : repository.getMetadata(normalizedJobId);
    }

    @Override
    public List<JobSnapshot> listJobs() {
        Map<String, JobSnapshot> snapshots =
                new LinkedHashMap<String, JobSnapshot>();

        for (JobSnapshot snapshot : repository.list()) {
            snapshots.put(
                    snapshot.getJobId(),
                    snapshot);
        }

        for (JobExecutionState state : activeJobs.snapshot()) {
            JobSnapshot snapshot =
                    lifecycle.snapshot(state);
            snapshots.put(
                    snapshot.getJobId(),
                    snapshot);
        }

        List<JobSnapshot> result =
                new ArrayList<JobSnapshot>(
                        snapshots.values());

        Collections.sort(
                result,
                new Comparator<JobSnapshot>() {
                    @Override
                    public int compare(
                            JobSnapshot left,
                            JobSnapshot right) {

                        return Long.compare(
                                right.getCreateTimeMillis(),
                                left.getCreateTimeMillis());
                    }
                });

        return result;
    }

    @Override
    public JobSnapshot cancel(String jobId) {
        String normalizedJobId =
                requireText(
                        jobId,
                        "jobId");

        JobExecutionState state =
                activeJobs.get(normalizedJobId);

        if (state == null) {
            JobSnapshot finished =
                    repository.get(normalizedJobId);

            if (finished != null) {
                throw new JobStateConflictException(
                        normalizedJobId,
                        finished.getStatus());
            }

            throw new JobNotFoundException(
                    normalizedJobId);
        }

        state.requestCancellation();
        lifecycle.persistState(state);
        runtimeScheduler.cancel(normalizedJobId);
        return getJob(normalizedJobId);
    }

    @Override
    public int getRunningJobCount() {
        return activeJobs.count(
                ServerJobStatus.RUNNING);
    }

    @Override
    public int getQueuedJobCount() {
        return activeJobs.countQueued();
    }

    @Override
    public int getActiveJobCount() {
        return activeJobs.size();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        runtimeScheduler.close();

        for (JobExecutionState state : activeJobs.snapshot()) {
            lifecycle.archiveAsLost(state);
        }
    }

    private void schedule(
            JobExecutionState state,
            JobSubmission submission) {

        runtimeScheduler.schedule(
                state.getJobId(),
                submission.getDefinition(),
                lifecycle.listener(state));
    }

    private void rollbackSubmission(
            JobExecutionState state,
            JobSubmission submission) {

        activeJobs.remove(
                state.getJobId(),
                state);
        submissionRegistry.unregister(
                state.getJobId(),
                submission);
        repository.delete(
                state.getJobId());
    }

    private JobExecutionState retryState(
            String jobId,
            JobSubmission submission,
            JobSnapshot previousSnapshot,
            JobExecutionMetadata previousMetadata) {

        List<JobExecutionAttempt> previousAttempts =
                new ArrayList<JobExecutionAttempt>();

        for (JobAttemptMetadata attempt :
                previousMetadata.getAttempts()) {
            previousAttempts.add(
                    attempt.toDomain());
        }

        return JobExecutionState.retryFrom(
                jobId,
                submission,
                previousSnapshot.getCreateTimeMillis(),
                previousMetadata.getSubmittedTimeMillis(),
                previousMetadata.getStateVersion(),
                previousMetadata.getStateRevision(),
                previousSnapshot.getStatus(),
                previousMetadata.getTransitions(),
                previousAttempts);
    }

    private void validateRetrySubmission(
            JobExecutionMetadata metadata,
            JobSubmission submission) {

        if (metadata == null
                || !submission.getExternalExecutionId()
                .equals(metadata.getExternalExecutionId())
                || !submission.getIdempotencyKey()
                .equals(metadata.getIdempotencyKey())
                || submission.getDefinitionVersion()
                != metadata.getDefinitionVersion()
                || !submission.getConfigDigest()
                .equals(metadata.getConfigDigest())) {

            throw new JobSubmissionConflictException(
                    "Retry request must use the same externalExecutionId, "
                            + "idempotencyKey, definitionVersion and job content");
        }
    }

    private JobSnapshot findExistingSubmission(
            JobSubmission submission) {

        String jobId =
                submissionRegistry.lookup(submission);

        if (jobId == null) {
            return null;
        }

        JobSnapshot snapshot;

        try {
            snapshot = getJob(jobId);
        } catch (JobNotFoundException stale) {
            submissionRegistry.unregister(
                    jobId,
                    submission);
            return null;
        }

        JobExecutionMetadata metadata =
                getMetadata(jobId);

        if (!sameSubmission(metadata, submission)) {
            throw new JobSubmissionConflictException(
                    "The idempotency key or external execution ID was reused with different content");
        }

        return snapshot;
    }

    private boolean sameSubmission(
            JobExecutionMetadata metadata,
            JobSubmission submission) {

        return metadata != null
                && submission.getExternalExecutionId()
                .equals(metadata.getExternalExecutionId())
                && submission.getIdempotencyKey()
                .equals(metadata.getIdempotencyKey())
                && submission.getDefinitionVersion()
                == metadata.getDefinitionVersion()
                && submission.getConfigDigest()
                .equals(metadata.getConfigDigest());
    }

    private void ensureOpen() {
        if (closed.get()
                || runtimeScheduler.isClosed()) {
            throw new IllegalStateException(
                    "Job application is closed");
        }
    }

    private static String requireText(
            String value,
            String name) {

        if (value == null
                || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }

        return value.trim();
    }
}
