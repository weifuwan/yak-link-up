package com.link.up.server.infrastructure.event;

import com.link.up.server.application.port.JobEventListener;
import com.link.up.server.application.port.JobEventRetention;
import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRepositoryEntry;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobStateTransition;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobRuntimeEvent;
import com.link.up.server.runtime.event.JobRuntimeEventType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Repository decorator that publishes one lifecycle fact after durable Worker
 * state persistence succeeds.
 *
 * <p>The wrapped repository remains the state source of truth. Event delivery
 * is best-effort and listener failures are isolated at this boundary. The
 * journal intentionally contains Job/Attempt lifecycle facts only.</p>
 */
public final class EventPublishingJobRepository
        implements JobRepository {

    private static final Logger LOG =
            LogManager.getLogger(
                    EventPublishingJobRepository.class);

    private final JobRepository delegate;
    private final JobEventListener eventListener;
    private final JobEventRetention retention;
    private final Object[] jobLocks = createLocks();

    public EventPublishingJobRepository(
            JobRepository delegate,
            JobEventListener eventListener) {
        this(
                delegate,
                eventListener,
                JobEventRetention.EMPTY);
    }

    public EventPublishingJobRepository(
            JobRepository delegate,
            JobEventListener eventListener,
            JobEventRetention retention) {

        this.delegate = Objects.requireNonNull(
                delegate,
                "delegate must not be null");
        this.eventListener = Objects.requireNonNull(
                eventListener,
                "eventListener must not be null");
        this.retention = Objects.requireNonNull(
                retention,
                "retention must not be null");

        retainCurrentJobs();
    }

    @Override
    public void save(JobSnapshot snapshot) {
        delegate.save(snapshot);
    }

    @Override
    public void save(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata) {

        JobSnapshot safeSnapshot = Objects.requireNonNull(
                snapshot,
                "snapshot must not be null");
        String jobId = safeSnapshot.getJobId();

        synchronized (lockFor(jobId)) {
            JobSnapshot previousSnapshot =
                    delegate.get(jobId);
            JobExecutionMetadata previousMetadata =
                    delegate.getMetadata(jobId);

            delegate.save(
                    safeSnapshot,
                    metadata);

            JobEventEnvelope event = event(
                    previousSnapshot,
                    previousMetadata,
                    safeSnapshot,
                    metadata);

            if (event != null) {
                publish(event);
            }

            if (safeSnapshot.getStatus().isTerminal()) {
                retainCurrentJobs();
            }
        }
    }

    @Override
    public JobSnapshot get(String jobId) {
        return delegate.get(jobId);
    }

    @Override
    public JobExecutionMetadata getMetadata(String jobId) {
        return delegate.getMetadata(jobId);
    }

    @Override
    public List<JobSnapshot> list() {
        return delegate.list();
    }

    @Override
    public List<JobRepositoryEntry> listEntries() {
        return delegate.listEntries();
    }

    @Override
    public void delete(String jobId) {
        delegate.delete(jobId);

        try {
            retention.delete(jobId);
        } catch (RuntimeException failure) {
            LOG.warn(
                    "Could not delete Job event history, jobId={}",
                    jobId,
                    failure);
        }
    }

    private void publish(JobEventEnvelope event) {
        try {
            eventListener.onEvent(event);
        } catch (RuntimeException failure) {
            LOG.error(
                    "Could not publish Job event, jobId={}, sequence={}, eventType={}",
                    event.getJobId(),
                    event.getSequence(),
                    event.getEvent().getType(),
                    failure);
        }
    }

    private void retainCurrentJobs() {
        try {
            List<String> jobIds =
                    new ArrayList<String>();

            for (JobSnapshot snapshot : delegate.list()) {
                jobIds.add(snapshot.getJobId());
            }

            retention.retain(jobIds);
        } catch (RuntimeException failure) {
            LOG.warn(
                    "Could not apply Job event history retention",
                    failure);
        }
    }

    private JobEventEnvelope event(
            JobSnapshot previousSnapshot,
            JobExecutionMetadata previousMetadata,
            JobSnapshot snapshot,
            JobExecutionMetadata metadata) {

        if (metadata == null
                || metadata.getStateRevision() <= 0L
                || !hasAttempt(metadata)) {
            return null;
        }

        if (previousMetadata != null
                && metadata.getStateRevision()
                <= previousMetadata.getStateRevision()) {
            return null;
        }

        JobAttemptMetadata attempt = currentAttempt(metadata);
        JobRuntimeEvent runtimeEvent = runtimeEvent(
                previousSnapshot,
                previousMetadata,
                snapshot,
                metadata,
                attempt);

        if (runtimeEvent == null) {
            return null;
        }

        return JobEventEnvelope.create(
                snapshot.getJobId(),
                attempt.getAttemptId(),
                attempt.getAttemptNumber(),
                metadata.getStateRevision(),
                occurredAtMillis(
                        runtimeEvent,
                        metadata),
                runtimeEvent);
    }

    private JobRuntimeEvent runtimeEvent(
            JobSnapshot previousSnapshot,
            JobExecutionMetadata previousMetadata,
            JobSnapshot snapshot,
            JobExecutionMetadata metadata,
            JobAttemptMetadata attempt) {

        if (previousSnapshot == null) {
            if (snapshot.getStatus() == ServerJobStatus.SUBMITTED) {
                return transitionEvent(
                        JobRuntimeEventType.JOB_SUBMITTED,
                        metadata,
                        snapshot.getStatus());
            }

            return statusEvent(
                    snapshot,
                    metadata,
                    attempt);
        }

        if (attemptCount(metadata)
                > attemptCount(previousMetadata)) {
            return transitionEvent(
                    JobRuntimeEventType.JOB_RETRY_CREATED,
                    metadata,
                    snapshot.getStatus());
        }

        if (previousSnapshot.getStatus()
                != snapshot.getStatus()) {
            return statusEvent(
                    snapshot,
                    metadata,
                    attempt);
        }

        if (!same(
                runId(previousMetadata),
                metadata.getRunId())) {
            return JobRuntimeEvent.logCreated(
                    snapshot.getStatus(),
                    metadata.getRunId());
        }

        if (!cancellationRequested(previousMetadata)
                && metadata.isCancellationRequested()) {
            return JobRuntimeEvent.cancellationRequested(
                    snapshot.getStatus());
        }

        return null;
    }

    private JobRuntimeEvent statusEvent(
            JobSnapshot snapshot,
            JobExecutionMetadata metadata,
            JobAttemptMetadata attempt) {

        ServerJobStatus status = snapshot.getStatus();
        if (status == ServerJobStatus.QUEUED) {
            return transitionEvent(
                    JobRuntimeEventType.JOB_QUEUED,
                    metadata,
                    status);
        }
        if (status == ServerJobStatus.RUNNING) {
            return transitionEvent(
                    JobRuntimeEventType.JOB_STARTED,
                    metadata,
                    status);
        }
        if (status == ServerJobStatus.SUCCEEDED) {
            return terminalEvent(
                    JobRuntimeEventType.JOB_SUCCEEDED,
                    metadata,
                    status,
                    null);
        }
        if (status == ServerJobStatus.CANCELED) {
            return terminalEvent(
                    JobRuntimeEventType.JOB_CANCELED,
                    metadata,
                    status,
                    null);
        }
        if (status == ServerJobStatus.LOST) {
            return terminalEvent(
                    JobRuntimeEventType.JOB_LOST,
                    metadata,
                    status,
                    attempt.getFailureType());
        }
        if (status == ServerJobStatus.FAILED) {
            return terminalEvent(
                    JobRuntimeEventType.JOB_FAILED,
                    metadata,
                    status,
                    attempt.getFailureType());
        }

        return null;
    }

    private JobRuntimeEvent transitionEvent(
            JobRuntimeEventType type,
            JobExecutionMetadata metadata,
            ServerJobStatus fallbackStatus) {

        JobStateTransition transition =
                latestTransition(metadata);

        return JobRuntimeEvent.transition(
                type,
                transition == null
                        ? null
                        : transition.getFromStatus(),
                transition == null
                        ? fallbackStatus
                        : transition.getToStatus(),
                transition == null
                        ? null
                        : transition.getReason());
    }

    private JobRuntimeEvent terminalEvent(
            JobRuntimeEventType type,
            JobExecutionMetadata metadata,
            ServerJobStatus fallbackStatus,
            String failureType) {

        JobStateTransition transition =
                latestTransition(metadata);

        return JobRuntimeEvent.terminal(
                type,
                transition == null
                        ? null
                        : transition.getFromStatus(),
                transition == null
                        ? fallbackStatus
                        : transition.getToStatus(),
                transition == null
                        ? null
                        : transition.getReason(),
                failureType);
    }

    private long occurredAtMillis(
            JobRuntimeEvent event,
            JobExecutionMetadata metadata) {

        JobRuntimeEventType type = event.getType();
        if (type == JobRuntimeEventType.JOB_LOG_CREATED
                || type == JobRuntimeEventType.JOB_CANCEL_REQUESTED) {
            return System.currentTimeMillis();
        }

        JobStateTransition transition =
                latestTransition(metadata);

        return transition == null
                ? System.currentTimeMillis()
                : transition.getTransitionTimeMillis();
    }

    private Object lockFor(String jobId) {
        int index = (jobId.hashCode() & Integer.MAX_VALUE)
                % jobLocks.length;
        return jobLocks[index];
    }

    private static Object[] createLocks() {
        Object[] locks = new Object[64];
        for (int index = 0; index < locks.length; index++) {
            locks[index] = new Object();
        }
        return locks;
    }

    private static JobStateTransition latestTransition(
            JobExecutionMetadata metadata) {

        List<JobStateTransition> transitions =
                metadata.getTransitions();

        return transitions.isEmpty()
                ? null
                : transitions.get(transitions.size() - 1);
    }

    private static JobAttemptMetadata currentAttempt(
            JobExecutionMetadata metadata) {

        List<JobAttemptMetadata> attempts =
                metadata.getAttempts();
        return attempts.get(attempts.size() - 1);
    }

    private static boolean hasAttempt(
            JobExecutionMetadata metadata) {
        return !metadata.getAttempts().isEmpty();
    }

    private static int attemptCount(
            JobExecutionMetadata metadata) {
        return metadata == null
                ? 0
                : metadata.getAttemptCount();
    }

    private static String runId(
            JobExecutionMetadata metadata) {
        return metadata == null
                ? null
                : metadata.getRunId();
    }

    private static boolean cancellationRequested(
            JobExecutionMetadata metadata) {
        return metadata != null
                && metadata.isCancellationRequested();
    }

    private static boolean same(
            Object left,
            Object right) {
        return Objects.equals(left, right);
    }
}
