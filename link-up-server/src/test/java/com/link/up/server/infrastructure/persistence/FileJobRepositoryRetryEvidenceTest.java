package com.link.up.server.infrastructure.persistence;

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
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshotFactory;
import com.link.up.server.runtime.ServerJobStatus;
import org.junit.Test;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileJobRepositoryRetryEvidenceTest {

    @Test
    public void shouldPersistRetrySafetyEvidenceAcrossReopen() throws Exception {
        Path directory = Files.createTempDirectory("link-up-retry-evidence-");
        try {
            FileJobRepository repository = new FileJobRepository(directory, 20);
            JobExecutionState state = new JobExecutionState(
                    "evidence-job",
                    submission());
            state.markSubmitted();
            state.markQueued();
            state.markRunning();

            RuntimeException failure = new RuntimeException("boom");
            CommitSummary summary = new CommitSummary(
                    1, 1, 0, 0, 1,
                    20L, 0L, 0L, 20L, 0L,
                    CommitScope.TASK_LOCAL,
                    "No committed data.");
            long now = System.currentTimeMillis();
            state.complete(
                    ServerJobStatus.FAILED,
                    new JobResult(
                            state.getJobName(),
                            JobStatus.FAILED,
                            now,
                            now,
                            new JobMetrics(),
                            failure,
                            summary),
                    failure);

            repository.save(
                    JobSnapshotFactory.create(state, null),
                    JobExecutionMetadata.fromState(state));

            FileJobRepository reopened = new FileJobRepository(directory, 20);
            JobExecutionMetadata metadata = reopened.getMetadata("evidence-job");

            assertEquals(1, metadata.getAttemptCount());
            assertTrue(metadata.getAttempts().get(0).isCommitEvidenceAvailable());
            assertEquals(0, metadata.getAttempts().get(0).getDataCommittedTaskCount());
            assertEquals(0L, metadata.getAttempts().get(0).getSuccessfullyCommittedRecordCount());
            assertEquals(0L, metadata.getAttempts().get(0).getUnknownStateRecordCount());
        } finally {
            deleteDirectory(directory);
        }
    }

    private static JobSubmission submission() {
        ReadonlyConfig options = ReadonlyConfig.fromMap(
                Collections.<String, Object>emptyMap());
        JobDefinition definition = new JobDefinition(
                "retry-evidence",
                new SourceDefinition("test-source", options),
                new SinkDefinition("test-sink", options),
                new ExecutionConfig(100, 1, 1, 1));
        return new JobSubmission(
                "retry-evidence-external",
                "retry-evidence-key",
                1,
                "retry-evidence-digest",
                definition);
    }

    private static void deleteDirectory(Path directory) throws Exception {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path path : stream) {
                Files.deleteIfExists(path);
            }
        }
        Files.deleteIfExists(directory);
    }
}
