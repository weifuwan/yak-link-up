package com.link.up.server.infrastructure.persistence;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.JobSnapshotFactory;
import com.link.up.server.runtime.ServerJobStatus;
import org.junit.Test;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FileJobRepositoryTest {

    @Test
    public void shouldReloadCheckpointAndRejectOlderRevision() throws Exception {
        Path directory = Files.createTempDirectory("link-up-state-");
        try {
            FileJobRepository first = new FileJobRepository(directory, 20);
            JobExecutionState state =
                    new JobExecutionState("job-file-1", submission());
            state.markSubmitted();
            state.markQueued();
            state.markRunning();

            first.save(
                    JobSnapshotFactory.create(state, null),
                    JobExecutionMetadata.fromState(state));

            JobExecutionState stale =
                    new JobExecutionState("job-file-1", submission());
            stale.markSubmitted();
            first.save(
                    JobSnapshotFactory.create(stale, null),
                    JobExecutionMetadata.fromState(stale));

            assertEquals(
                    ServerJobStatus.RUNNING,
                    first.get("job-file-1").getStatus());

            FileJobRepository reopened =
                    new FileJobRepository(directory, 20);
            JobSnapshot snapshot = reopened.get("job-file-1");
            assertNotNull(snapshot);
            assertEquals(ServerJobStatus.RUNNING, snapshot.getStatus());

            JobExecutionMetadata metadata =
                    reopened.getMetadata("job-file-1");
            assertEquals("external-file", metadata.getExternalExecutionId());
            assertEquals("key-file", metadata.getIdempotencyKey());
            assertEquals(1, metadata.getAttemptCount());
            assertEquals(
                    JobAttemptStatus.RUNNING,
                    metadata.getAttempts().get(0).getStatus());
            assertEquals(3L, metadata.getCheckpointVersion());

            reopened.delete("job-file-1");
            assertNull(reopened.get("job-file-1"));

            FileJobRepository afterDelete =
                    new FileJobRepository(directory, 20);
            assertNull(afterDelete.get("job-file-1"));
        } finally {
            deleteDirectory(directory);
        }
    }

    private static JobSubmission submission() {
        ReadonlyConfig options = ReadonlyConfig.fromMap(
                Collections.<String, Object>emptyMap());
        JobDefinition definition =
                new JobDefinition(
                        "file-repository-test",
                        new SourceDefinition("test-source", options),
                        new SinkDefinition("test-sink", options),
                        new ExecutionConfig(100, 1, 1, 1));
        return new JobSubmission(
                "external-file",
                "key-file",
                1,
                "digest-file",
                definition);
    }

    private static void deleteDirectory(Path directory) throws Exception {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(directory)) {
            for (Path path : stream) {
                Files.deleteIfExists(path);
            }
        }
        Files.deleteIfExists(directory);
    }
}
