package com.link.up.server.application;

import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.server.application.port.JobIdGenerator;
import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRuntimeScheduler;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobSnapshotFactory;
import com.link.up.server.runtime.JobStateConflictException;
import com.link.up.server.runtime.ServerJobStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-node Worker application service.
 *
 * <p>Owns use-case orchestration and idempotency semantics. Local admission,
 * threads and framework execution bindings are delegated to
 * {@link JobRuntimeScheduler}.</p>
 */
public final class JobApplicationService
        implements JobApplication {

    private final JobRuntimeScheduler runtimeScheduler;
    private final JobRepository repository;
    private final JobIdGenerator jobIdGenerator;
    private final JobSubmissionRegistry submissionRegistry;
    private final ConcurrentMap<String, JobExecutionState> activeJobs =
            new ConcurrentHashMap<String, JobExecutionState>();
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

        JobSnapshot existing = findExistingSubmission(submission);
        if (existing != null) {
            return existing;
        }

        final String jobId = jobIdGenerator.nextId();
        final JobExecutionState state =
                new JobExecutionState(jobId, submission);

        JobExecutionState previous =
                activeJobs.putIfAbsent(jobId, state);
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate jobId: " + jobId);
        }

        try {
            submissionRegistry.register(jobId, submission);
            state.markSubmitted();
            runtimeScheduler.schedule(
                    jobId,
                    submission.getDefinition(),
                    listener(state));
        } catch (RuntimeException failure) {
            activeJobs.remove(jobId, state);
            submissionRegistry.unregister(jobId, submission);
            throw failure;
        }

        return snapshot(state);
    }

    private JobRuntimeScheduler.Listener listener(
            final JobExecutionState state) {

        return new JobRuntimeScheduler.Listener() {
            @Override
            public void onQueued() {
                state.markQueued();
            }

            @Override
            public boolean onStarting() {
                return state.markRunning();
            }

            @Override
            public void onJobLogCreated(
                    String runId,
                    String jobLogFile) {
                state.bindLogIdentity(runId, jobLogFile);
            }

            @Override
            public void onCompleted(
                    JobResult result,
                    Throwable failure,
                    boolean cancellationLike) {

                completeAndArchive(
                        state,
                        terminalStatus(
                                state,
                                result,
                                failure,
                                cancellationLike),
                        result,
                        terminalFailure(
                                state,
                                result,
                                failure,
                                cancellationLike));
            }

            @Override
            public void onLost() {
                completeAndArchive(
                        state,
                        ServerJobStatus.LOST,
                        null,
                        null);
            }
        };
    }

    private JobSnapshot findExistingSubmission(
            JobSubmission submission) {

        String jobId = submissionRegistry.lookup(submission);
        if (jobId == null) {
            return null;
        }

        JobSnapshot snapshot;
        try {
            snapshot = getJob(jobId);
        } catch (JobNotFoundException stale) {
            submissionRegistry.unregister(jobId, submission);
            return null;
        }

        JobExecutionMetadata metadata = getMetadata(jobId);
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
                    "The idempotency key or external execution ID was reused with different content");
        }
        return snapshot;
    }

    private ServerJobStatus terminalStatus(
            JobExecutionState state,
            JobResult result,
            Throwable failure,
            boolean cancellationLike) {

        if (state.isCancellationRequested()
                || cancellationLike
                || result != null
                && result.getStatus() == JobStatus.CANCELED) {
            return ServerJobStatus.CANCELED;
        }
        if (result != null
                && result.getStatus() == JobStatus.SUCCEEDED
                && failure == null) {
            return ServerJobStatus.SUCCEEDED;
        }
        return ServerJobStatus.FAILED;
    }

    private Throwable terminalFailure(
            JobExecutionState state,
            JobResult result,
            Throwable failure,
            boolean cancellationLike) {

        if (state.isCancellationRequested()
                || cancellationLike) {
            return null;
        }
        if (failure != null) {
            return failure;
        }
        return result == null
                ? null
                : result.getFailure();
    }

    private void completeAndArchive(
            JobExecutionState state,
            ServerJobStatus status,
            JobResult result,
            Throwable failure) {

        if (!state.complete(status, result, failure)) {
            return;
        }

        JobSnapshot snapshot = snapshot(state);
        JobExecutionMetadata metadata =
                JobSnapshotFactory.metadata(state);
        repository.save(snapshot, metadata);
        activeJobs.remove(state.getJobId(), state);
    }

    private JobSnapshot snapshot(
            JobExecutionState state) {
        return JobSnapshotFactory.create(
                state,
                runtimeScheduler.getMetrics(
                        state.getJobId()));
    }

    @Override
    public JobSnapshot getJob(String jobId) {
        requireText(jobId, "jobId");
        JobExecutionState active = activeJobs.get(jobId);
        if (active != null) {
            return snapshot(active);
        }
        JobSnapshot finished = repository.get(jobId);
        if (finished == null) {
            throw new JobNotFoundException(jobId);
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
            throw new JobNotFoundException(externalId);
        }
        return getJob(jobId);
    }

    @Override
    public JobExecutionMetadata getMetadata(String jobId) {
        requireText(jobId, "jobId");
        JobExecutionState active = activeJobs.get(jobId);
        if (active != null) {
            return JobSnapshotFactory.metadata(active);
        }
        return repository.getMetadata(jobId);
    }

    @Override
    public List<JobSnapshot> listJobs() {
        Map<String, JobSnapshot> snapshots =
                new LinkedHashMap<String, JobSnapshot>();
        for (JobSnapshot snapshot : repository.list()) {
            snapshots.put(snapshot.getJobId(), snapshot);
        }
        for (JobExecutionState state : activeJobs.values()) {
            JobSnapshot snapshot = snapshot(state);
            snapshots.put(snapshot.getJobId(), snapshot);
        }
        List<JobSnapshot> result =
                new ArrayList<JobSnapshot>(snapshots.values());
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
        requireText(jobId, "jobId");
        JobExecutionState state = activeJobs.get(jobId);
        if (state == null) {
            JobSnapshot finished = repository.get(jobId);
            if (finished != null) {
                throw new JobStateConflictException(
                        jobId,
                        finished.getStatus());
            }
            throw new JobNotFoundException(jobId);
        }
        state.requestCancellation();
        runtimeScheduler.cancel(jobId);
        return getJob(jobId);
    }

    @Override
    public int getRunningJobCount() {
        return count(ServerJobStatus.RUNNING);
    }

    @Override
    public int getQueuedJobCount() {
        int count = 0;
        for (JobExecutionState state : activeJobs.values()) {
            ServerJobStatus status = state.getStatus();
            if (status == ServerJobStatus.CREATED
                    || status == ServerJobStatus.SUBMITTED
                    || status == ServerJobStatus.QUEUED) {
                count++;
            }
        }
        return count;
    }

    private int count(ServerJobStatus status) {
        int count = 0;
        for (JobExecutionState state : activeJobs.values()) {
            if (state.getStatus() == status) {
                count++;
            }
        }
        return count;
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
        for (JobExecutionState state : activeJobs.values()) {
            completeAndArchive(
                    state,
                    ServerJobStatus.LOST,
                    null,
                    null);
        }
    }

    private void ensureOpen() {
        if (closed.get() || runtimeScheduler.isClosed()) {
            throw new IllegalStateException(
                    "Job application is closed");
        }
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
