package com.link.up.server.application;

import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.metrics.JobMetrics;
import com.link.up.server.application.port.JobIdGenerator;
import com.link.up.server.application.port.JobRuntimeScheduler;
import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.infrastructure.persistence.InMemoryJobRepository;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobSnapshotFactory;
import com.link.up.server.runtime.ServerJobStatus;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class JobApplicationRecoveryTest {

    @Test
    public void shouldRecoverInterruptedCheckpointAsLostAndRestoreIdempotency() {
        InMemoryJobRepository repository =
                new InMemoryJobRepository(20);
        JobSubmission submission =
                JobApplicationServiceProtocolTest.submission(
                        "external-recovery",
                        "key-recovery",
                        "digest-recovery");

        JobExecutionState interrupted =
                new JobExecutionState(
                        "job-recovery",
                        submission);
        interrupted.markSubmitted();
        interrupted.markQueued();
        interrupted.markRunning();

        repository.save(
                JobSnapshotFactory.create(interrupted, null),
                JobExecutionMetadata.fromState(interrupted));

        RecordingScheduler scheduler =
                new RecordingScheduler();
        JobApplicationService application =
                new JobApplicationService(
                        scheduler,
                        repository,
                        new JobIdGenerator() {
                            @Override
                            public String nextId() {
                                return "unexpected-new-job";
                            }
                        });

        try {
            JobSnapshot recovered =
                    application.getJob("job-recovery");
            assertEquals(
                    ServerJobStatus.LOST,
                    recovered.getStatus());
            assertEquals(
                    "FLUX-JOB-LOST",
                    recovered.getErrorCode());

            JobExecutionMetadata metadata =
                    application.getMetadata("job-recovery");
            assertEquals(1, metadata.getAttemptCount());
            assertEquals(
                    JobAttemptStatus.LOST,
                    metadata.getAttempts().get(0).getStatus());
            assertEquals(
                    ServerJobStatus.LOST,
                    metadata.getTransitions()
                            .get(metadata.getTransitions().size() - 1)
                            .getToStatus());

            assertEquals(
                    "job-recovery",
                    application.getJobByExternalExecutionId(
                            "external-recovery")
                            .getJobId());
            assertEquals(
                    "job-recovery",
                    application.submit(submission).getJobId());
            assertEquals(0, scheduler.scheduleCount.get());
        } finally {
            application.close();
        }
    }

    private static final class RecordingScheduler
            implements JobRuntimeScheduler {

        private final AtomicInteger scheduleCount =
                new AtomicInteger();
        private boolean closed;

        @Override
        public void schedule(
                String jobId,
                JobDefinition definition,
                Listener listener) {
            scheduleCount.incrementAndGet();
        }

        @Override public void cancel(String jobId) { }
        @Override public JobMetrics getMetrics(String jobId) { return null; }
        @Override public boolean isClosed() { return closed; }
        @Override public void close() { closed = true; }
    }
}
