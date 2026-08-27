package com.link.up.server.service;

import com.link.up.framework.job.JobDefinition;
import com.link.up.server.application.JobApplication;
import com.link.up.server.application.port.JobEventReader;
import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.dto.JobHistoryResponse;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobRecoverySnapshotFactory;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobStateTransition;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;
import com.link.up.server.runtime.event.JobExecutionFacts;
import com.link.up.server.runtime.event.JobRuntimeEvent;
import com.link.up.server.runtime.event.JobRuntimeEventType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobHistoryRestServiceTest {

    @Test
    public void shouldRecoverCurrentAttemptExecutionFactsFromJournalAfterRestart() {
        JobSnapshot restoredSnapshot = restoredSnapshot();
        JobExecutionMetadata metadata = metadata(
                "job-history-restart-attempt-1");
        JobEventEnvelope terminal = terminalEvent(
                "job-history-restart-attempt-1");

        JobHistoryRestService service =
                new JobHistoryRestService(
                        new StubJobApplication(
                                restoredSnapshot,
                                metadata),
                        new StubEventReader(terminal));

        JobHistoryResponse response = service.history(
                "job-history-restart",
                5L,
                20);

        assertTrue(response.isCompleted());
        assertEquals(0, response.getEvents().size());
        assertEquals(1,
                response.getExecution().getPipelines().size());
        assertEquals("pipeline-demo.orders",
                response.getExecution()
                        .getPipelines()
                        .get(0)
                        .getPipelineId());
    }

    @Test
    public void shouldNotReuseExecutionFactsFromPreviousAttempt() {
        JobSnapshot restoredSnapshot = restoredSnapshot();
        JobExecutionMetadata metadata = metadata(
                "job-history-restart-attempt-2");
        JobEventEnvelope staleTerminal = terminalEvent(
                "job-history-restart-attempt-1");

        JobHistoryRestService service =
                new JobHistoryRestService(
                        new StubJobApplication(
                                restoredSnapshot,
                                metadata),
                        new StubEventReader(staleTerminal));

        JobHistoryResponse response = service.history(
                "job-history-restart",
                0L,
                20);

        assertTrue(response.isCompleted());
        assertEquals(1, response.getEvents().size());
        assertFalse(response.getExecution().hasExecutionDetails());
    }

    private static JobSnapshot restoredSnapshot() {
        return JobRecoverySnapshotFactory.restoreBasic(
                "job-history-restart",
                "history-restart",
                ServerJobStatus.SUCCEEDED,
                1L,
                2L,
                3L,
                null,
                null);
    }

    private static JobExecutionMetadata metadata(
            String attemptId) {

        JobAttemptMetadata attempt =
                new JobAttemptMetadata(
                        attemptId.endsWith("-2") ? 2 : 1,
                        attemptId,
                        JobAttemptStatus.SUCCEEDED,
                        1L,
                        1L,
                        2L,
                        3L,
                        null,
                        null,
                        null,
                        null,
                        null);

        return new JobExecutionMetadata(
                "external-history-restart",
                "key-history-restart",
                1,
                "digest-history-restart",
                1L,
                1L,
                5L,
                5L,
                false,
                Collections.<JobStateTransition>emptyList(),
                Collections.emptyMap(),
                null,
                null,
                Collections.singletonList(attempt));
    }

    private static JobEventEnvelope terminalEvent(
            String attemptId) {

        return JobEventEnvelope.create(
                "job-history-restart",
                attemptId,
                1,
                5L,
                5L,
                JobRuntimeEvent.terminal(
                        JobRuntimeEventType.JOB_SUCCEEDED,
                        ServerJobStatus.RUNNING,
                        ServerJobStatus.SUCCEEDED,
                        "job-succeeded",
                        null,
                        retainedExecutionFacts()));
    }

    private static JobExecutionFacts retainedExecutionFacts() {
        JobExecutionFacts.Metrics metrics =
                new JobExecutionFacts.Metrics(
                        100L,
                        1024L,
                        20D,
                        100L,
                        100L,
                        900L,
                        19D,
                        0L,
                        0L,
                        0L,
                        2L,
                        2L,
                        0L,
                        8L,
                        10L,
                        0.1D,
                        0.2D,
                        0D);
        JobExecutionFacts.Pipeline pipeline =
                new JobExecutionFacts.Pipeline(
                        "pipeline-demo.orders",
                        "demo.orders",
                        "SUCCEEDED",
                        "jdbc",
                        1,
                        100L,
                        1024L,
                        20D,
                        "doris",
                        1,
                        100L,
                        100L,
                        0L,
                        0L,
                        900L,
                        19D,
                        null);

        return new JobExecutionFacts(
                metrics,
                null,
                Collections.singletonList(pipeline),
                Collections.emptyList());
    }

    private static final class StubEventReader
            implements JobEventReader {

        private final JobEventEnvelope terminal;

        private StubEventReader(JobEventEnvelope terminal) {
            this.terminal = terminal;
        }

        @Override
        public JobEventPage read(
                String jobId,
                long afterSequence,
                int limit) {

            if (afterSequence >= terminal.getSequence()) {
                return JobEventPage.empty(
                        jobId,
                        afterSequence);
            }

            return new JobEventPage(
                    jobId,
                    Collections.singletonList(terminal),
                    terminal.getSequence(),
                    false);
        }
    }

    private static final class StubJobApplication
            implements JobApplication {

        private final JobSnapshot snapshot;
        private final JobExecutionMetadata metadata;

        private StubJobApplication(
                JobSnapshot snapshot,
                JobExecutionMetadata metadata) {
            this.snapshot = snapshot;
            this.metadata = metadata;
        }

        @Override
        public JobSnapshot submit(JobDefinition definition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobSnapshot submit(JobSubmission submission) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobSnapshot getJob(String jobId) {
            return snapshot;
        }

        @Override
        public JobSnapshot getJobByExternalExecutionId(
                String externalExecutionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobExecutionMetadata getMetadata(String jobId) {
            return metadata;
        }

        @Override
        public List<JobSnapshot> listJobs() {
            return Collections.emptyList();
        }

        @Override
        public JobSnapshot cancel(String jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getRunningJobCount() {
            return 0;
        }

        @Override
        public int getQueuedJobCount() {
            return 0;
        }

        @Override
        public int getActiveJobCount() {
            return 0;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
        }
    }
}
