package com.link.up.server.application;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.sink.CommitScope;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.server.application.port.JobIdGenerator;
import com.link.up.server.application.port.JobRuntimeScheduler;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.infrastructure.persistence.InMemoryJobRepository;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobApplicationRetryTest {

    @Test
    public void shouldAppendSecondAttemptWhenFailureHasNoCommittedData() {
        SequencedScheduler scheduler = new SequencedScheduler(false);
        JobApplicationService application = application(scheduler);
        JobSubmission submission = submission();

        try {
            JobSnapshot failed = application.submit(submission);
            assertEquals(JobStatus.FAILED.name(), failed.getStatus().name());

            JobRetryDecision decision =
                    application.retryDecision(failed.getJobId());
            assertTrue(decision.isEligible());
            assertEquals(
                    JobRetryDecision.SAFE_NO_DATA_COMMITTED,
                    decision.getCode());
            assertEquals(2, decision.getNextAttemptNumber());

            JobSnapshot retried = application.retry(
                    failed.getJobId(),
                    submission);

            assertEquals(failed.getJobId(), retried.getJobId());
            assertEquals("SUCCEEDED", retried.getStatus().name());
            assertEquals(2, scheduler.getScheduleCount());

            JobExecutionMetadata metadata =
                    application.getMetadata(retried.getJobId());
            assertEquals(2, metadata.getAttemptCount());
            assertEquals("FAILED", metadata.getAttempts().get(0).getStatus().name());
            assertEquals("SUCCEEDED", metadata.getAttempts().get(1).getStatus().name());
            assertFalse(application.retryDecision(retried.getJobId()).isEligible());
        } finally {
            application.close();
        }
    }

    @Test
    public void shouldRejectRetryWhenPreviousAttemptCommittedData() {
        SequencedScheduler scheduler = new SequencedScheduler(true);
        JobApplicationService application = application(scheduler);
        JobSubmission submission = submission();

        try {
            JobSnapshot failed = application.submit(submission);
            JobRetryDecision decision =
                    application.retryDecision(failed.getJobId());

            assertFalse(decision.isEligible());
            assertEquals(
                    JobRetryDecision.DATA_ALREADY_COMMITTED,
                    decision.getCode());

            try {
                application.retry(failed.getJobId(), submission);
                fail("Expected retry to be rejected");
            } catch (JobRetryNotAllowedException expected) {
                assertEquals(
                        JobRetryDecision.DATA_ALREADY_COMMITTED,
                        expected.getDecision().getCode());
            }
            assertEquals(1, scheduler.getScheduleCount());
        } finally {
            application.close();
        }
    }

    private static JobApplicationService application(
            JobRuntimeScheduler scheduler) {
        JobIdGenerator ids = new JobIdGenerator() {
            @Override
            public String nextId() {
                return "retry-job-1";
            }
        };
        return new JobApplicationService(
                scheduler,
                new InMemoryJobRepository(20),
                ids);
    }

    private static JobSubmission submission() {
        ReadonlyConfig options = ReadonlyConfig.fromMap(
                Collections.<String, Object>emptyMap());
        JobDefinition definition = new JobDefinition(
                "retry-test",
                new SourceDefinition("test-source", options),
                new SinkDefinition("test-sink", options),
                new ExecutionConfig(100, 1, 1, 1));
        return new JobSubmission(
                "retry-external-1",
                "retry-key-1",
                1,
                "retry-digest-1",
                definition);
    }

    private static final class SequencedScheduler
            implements JobRuntimeScheduler {

        private final AtomicInteger schedules = new AtomicInteger();
        private final boolean commitDataOnFirstFailure;
        private boolean closed;

        private SequencedScheduler(boolean commitDataOnFirstFailure) {
            this.commitDataOnFirstFailure = commitDataOnFirstFailure;
        }

        @Override
        public void schedule(
                String jobId,
                JobDefinition definition,
                Listener listener) {

            int attempt = schedules.incrementAndGet();
            listener.onQueued();
            if (!listener.onStarting()) {
                listener.onCompleted(null, null, true);
                return;
            }

            long now = System.currentTimeMillis();
            if (attempt == 1) {
                RuntimeException failure = new RuntimeException("first-attempt-failed");
                CommitSummary summary = commitDataOnFirstFailure
                        ? new CommitSummary(
                                1, 1, 1, 0, 0,
                                10L, 10L, 10L, 0L, 0L,
                                CommitScope.TASK_LOCAL,
                                "Committed data already exists.")
                        : new CommitSummary(
                                1, 1, 0, 0, 1,
                                10L, 0L, 0L, 10L, 0L,
                                CommitScope.TASK_LOCAL,
                                "No data was committed.");
                listener.onCompleted(
                        new JobResult(
                                definition.getName(),
                                JobStatus.FAILED,
                                now,
                                now,
                                new JobMetrics(),
                                failure,
                                summary),
                        null,
                        false);
                return;
            }

            listener.onCompleted(
                    new JobResult(
                            definition.getName(),
                            JobStatus.SUCCEEDED,
                            now,
                            now,
                            new JobMetrics(),
                            null),
                    null,
                    false);
        }

        @Override
        public void cancel(String jobId) {
        }

        @Override
        public JobMetrics getMetrics(String jobId) {
            return null;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }

        int getScheduleCount() {
            return schedules.get();
        }
    }
}
