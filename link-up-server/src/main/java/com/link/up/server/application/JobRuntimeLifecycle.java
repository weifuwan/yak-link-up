package com.link.up.server.application;

import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRuntimeScheduler;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobSnapshotFactory;
import com.link.up.server.runtime.ServerJobStatus;

import java.util.Objects;

/**
 * Applies runtime callbacks to domain state and persists Worker control-plane
 * state. This persistence is not a data checkpoint or resume point.
 */
final class JobRuntimeLifecycle {

    private final JobRuntimeScheduler runtimeScheduler;
    private final JobRepository repository;
    private final ActiveJobRegistry activeJobs;

    JobRuntimeLifecycle(
            JobRuntimeScheduler runtimeScheduler,
            JobRepository repository,
            ActiveJobRegistry activeJobs) {

        this.runtimeScheduler = Objects.requireNonNull(
                runtimeScheduler,
                "runtimeScheduler must not be null");
        this.repository = Objects.requireNonNull(
                repository,
                "repository must not be null");
        this.activeJobs = Objects.requireNonNull(
                activeJobs,
                "activeJobs must not be null");
    }

    JobSnapshot snapshot(JobExecutionState state) {
        return JobSnapshotFactory.create(
                state,
                runtimeScheduler.getMetrics(state.getJobId()));
    }

    JobExecutionMetadata metadata(JobExecutionState state) {
        return JobExecutionMetadata.fromState(state);
    }

    void persistState(JobExecutionState state) {
        repository.save(
                snapshot(state),
                metadata(state));
    }

    JobRuntimeScheduler.Listener listener(
            final JobExecutionState state) {

        return new JobRuntimeScheduler.Listener() {
            @Override
            public void onQueued() {
                state.markQueued();
                persistState(state);
            }

            @Override
            public boolean onStarting() {
                boolean started = state.markRunning();
                if (started) {
                    persistState(state);
                }
                return started;
            }

            @Override
            public void onJobLogCreated(
                    String runId,
                    String jobLogFile) {

                state.bindLogIdentity(
                        runId,
                        jobLogFile);
                persistState(state);
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

    void failBeforeExecutionAndArchive(
            JobExecutionState state,
            RuntimeException failure) {

        if (activeJobs.get(state.getJobId()) != state) {
            return;
        }

        state.failBeforeExecution(failure);
        persistState(state);
        activeJobs.remove(
                state.getJobId(),
                state);
    }

    void archiveAsLost(JobExecutionState state) {
        completeAndArchive(
                state,
                ServerJobStatus.LOST,
                null,
                null);
    }

    private void completeAndArchive(
            JobExecutionState state,
            ServerJobStatus status,
            JobResult result,
            Throwable failure) {

        if (!state.complete(status, result, failure)) {
            return;
        }

        persistState(state);
        activeJobs.remove(
                state.getJobId(),
                state);
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
}
