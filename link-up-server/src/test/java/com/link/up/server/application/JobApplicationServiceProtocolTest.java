package com.link.up.server.application;

import com.link.up.api.configuration.ReadonlyConfig;
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
import com.link.up.server.runtime.JobSnapshot;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class JobApplicationServiceProtocolTest {

    @Test
    public void shouldReturnSameJobForSameIdempotentSubmission() {
        JobApplicationService application = application();
        try {
            JobSubmission submission =
                    submission(
                            "external-100",
                            "key-100",
                            "digest-100");

            JobSnapshot first = application.submit(submission);
            JobSnapshot second = application.submit(submission);

            assertEquals(first.getJobId(), second.getJobId());
            assertEquals(
                    first.getJobId(),
                    application.getJobByExternalExecutionId(
                            "external-100")
                            .getJobId());
        } finally {
            application.close();
        }
    }

    @Test(expected = JobSubmissionConflictException.class)
    public void shouldRejectReusedIdempotencyKeyWithDifferentContent() {
        JobApplicationService application = application();
        try {
            application.submit(
                    submission(
                            "external-200",
                            "key-200",
                            "digest-a"));

            application.submit(
                    submission(
                            "external-200",
                            "key-200",
                            "digest-b"));
        } finally {
            application.close();
        }
    }

    private static JobApplicationService application() {
        final AtomicInteger ids = new AtomicInteger();
        JobIdGenerator idGenerator =
                new JobIdGenerator() {
                    @Override
                    public String nextId() {
                        return "job-" + ids.incrementAndGet();
                    }
                };

        return new JobApplicationService(
                new ImmediateScheduler(),
                new InMemoryJobRepository(20),
                idGenerator);
    }

    private static JobSubmission submission(
            String externalExecutionId,
            String idempotencyKey,
            String digest) {

        ReadonlyConfig options =
                ReadonlyConfig.fromMap(
                        Collections.<String, Object>emptyMap());
        JobDefinition definition =
                new JobDefinition(
                        "protocol-test",
                        new SourceDefinition("test-source", options),
                        new SinkDefinition("test-sink", options),
                        new ExecutionConfig(100, 1, 1, 1));

        return new JobSubmission(
                externalExecutionId,
                idempotencyKey,
                1,
                digest,
                definition);
    }

    private static final class ImmediateScheduler
            implements JobRuntimeScheduler {

        private boolean closed;

        @Override
        public void schedule(
                String jobId,
                JobDefinition definition,
                Listener listener) {

            listener.onQueued();
            if (!listener.onStarting()) {
                listener.onCompleted(null, null, true);
                return;
            }

            long now = System.currentTimeMillis();
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
    }
}
