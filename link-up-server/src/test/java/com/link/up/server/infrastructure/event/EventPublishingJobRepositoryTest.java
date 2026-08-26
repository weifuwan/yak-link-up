package com.link.up.server.infrastructure.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import com.link.up.server.application.JobEventBus;
import com.link.up.server.application.port.JobEventListener;
import com.link.up.server.application.port.JobRepository;
import com.link.up.server.domain.JobExecutionAttempt;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobSnapshotFactory;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobRuntimeEventType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EventPublishingJobRepositoryTest {

    @Test
    public void shouldPublishCheckpointLifecycleInSequence()
            throws Exception {

        RecordingListener listener = new RecordingListener();
        EventPublishingJobRepository repository = repository(listener);
        JobExecutionState state = state("job-1");

        state.markSubmitted();
        save(repository, state);
        state.markQueued();
        save(repository, state);
        state.markRunning();
        save(repository, state);
        state.bindLogIdentity("run-1", "logs/job-1.log");
        save(repository, state);
        state.requestCancellation();
        save(repository, state);
        state.complete(ServerJobStatus.CANCELED, null, null);
        save(repository, state);

        assertEquals(6, listener.events.size());
        assertTypes(
                listener.events,
                JobRuntimeEventType.JOB_SUBMITTED,
                JobRuntimeEventType.JOB_QUEUED,
                JobRuntimeEventType.JOB_STARTED,
                JobRuntimeEventType.JOB_LOG_CREATED,
                JobRuntimeEventType.JOB_CANCEL_REQUESTED,
                JobRuntimeEventType.JOB_CANCELED);

        for (int index = 0; index < listener.events.size(); index++) {
            assertEquals(
                    index + 1L,
                    listener.events.get(index).getSequence());
        }

        String json = new ObjectMapper().writeValueAsString(
                listener.events);
        assertFalse(json.contains("TEST_ONLY_PASSWORD"));
        assertFalse(json.contains("password"));
    }

    @Test
    public void shouldPublishRetryAttemptWithoutResettingSequence() {
        RecordingListener listener = new RecordingListener();
        EventPublishingJobRepository repository = repository(listener);
        JobExecutionState failed = state("job-retry");

        failed.markSubmitted();
        save(repository, failed);
        failed.failBeforeExecution(
                new IllegalStateException("TEST_ONLY_PASSWORD"));
        save(repository, failed);

        JobExecutionMetadata previous =
                JobExecutionMetadata.fromState(failed);
        List<JobExecutionAttempt> previousAttempts =
                new ArrayList<JobExecutionAttempt>(
                        failed.getAttempts());

        JobExecutionState retry = JobExecutionState.retryFrom(
                failed.getJobId(),
                failed.getSubmission(),
                failed.getCreateTimeMillis(),
                failed.getSubmittedTimeMillis(),
                previous.getStateVersion(),
                previous.getCheckpointVersion(),
                failed.getStatus(),
                previous.getTransitions(),
                previousAttempts);

        save(repository, retry);

        JobEventEnvelope last =
                listener.events.get(
                        listener.events.size() - 1);
        assertEquals(
                JobRuntimeEventType.JOB_RETRY_CREATED,
                last.getEvent().getType());
        assertEquals(2, last.getAttemptNumber());
        assertEquals(3L, last.getSequence());
    }

    private EventPublishingJobRepository repository(
            JobEventListener listener) {
        return new EventPublishingJobRepository(
                new MemoryRepository(),
                new JobEventBus(
                        Collections.singletonList(listener)));
    }

    private JobExecutionState state(String jobId) {
        Map<String, Object> sourceOptions =
                new LinkedHashMap<String, Object>();
        sourceOptions.put("password", "TEST_ONLY_PASSWORD");

        JobDefinition definition = new JobDefinition(
                "event-test",
                new SourceDefinition(
                        "test-source",
                        ReadonlyConfig.fromMap(sourceOptions)),
                new SinkDefinition(
                        "test-sink",
                        ReadonlyConfig.fromMap(
                                Collections.<String, Object>emptyMap())),
                new ExecutionConfig(100, 1, 1, 32));

        return new JobExecutionState(
                jobId,
                JobSubmission.legacy(definition));
    }

    private void save(
            EventPublishingJobRepository repository,
            JobExecutionState state) {
        repository.save(
                JobSnapshotFactory.create(state, null),
                JobExecutionMetadata.fromState(state));
    }

    private void assertTypes(
            List<JobEventEnvelope> events,
            JobRuntimeEventType... types) {

        assertEquals(types.length, events.size());
        for (int index = 0; index < types.length; index++) {
            assertEquals(
                    types[index],
                    events.get(index).getEvent().getType());
        }
    }

    private static final class RecordingListener
            implements JobEventListener {

        private final List<JobEventEnvelope> events =
                new ArrayList<JobEventEnvelope>();

        @Override
        public void onEvent(JobEventEnvelope event) {
            events.add(event);
        }
    }

    private static final class MemoryRepository
            implements JobRepository {

        private final Map<String, JobSnapshot> snapshots =
                new LinkedHashMap<String, JobSnapshot>();
        private final Map<String, JobExecutionMetadata> metadata =
                new LinkedHashMap<String, JobExecutionMetadata>();

        @Override
        public void save(JobSnapshot snapshot) {
            snapshots.put(snapshot.getJobId(), snapshot);
        }

        @Override
        public void save(
                JobSnapshot snapshot,
                JobExecutionMetadata executionMetadata) {
            snapshots.put(snapshot.getJobId(), snapshot);
            metadata.put(snapshot.getJobId(), executionMetadata);
        }

        @Override
        public JobSnapshot get(String jobId) {
            return snapshots.get(jobId);
        }

        @Override
        public JobExecutionMetadata getMetadata(String jobId) {
            return metadata.get(jobId);
        }

        @Override
        public List<JobSnapshot> list() {
            return new ArrayList<JobSnapshot>(
                    snapshots.values());
        }
    }
}
