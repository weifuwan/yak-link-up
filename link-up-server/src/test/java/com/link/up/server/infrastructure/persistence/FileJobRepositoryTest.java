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

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FileJobRepositoryTest {

    @Test
    public void shouldReloadWorkerStateAndRejectOlderRevision()
            throws Exception {
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

            String persisted = readStateFile(directory, "job-file-1");
            assertTrue(persisted.contains("\"formatVersion\" : 4"));
            assertTrue(persisted.contains("\"stateRevision\""));
            assertFalse(persisted.contains("\"checkpointVersion\""));

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
            assertEquals(3L, metadata.getStateRevision());

            reopened.delete("job-file-1");
            assertNull(reopened.get("job-file-1"));

            FileJobRepository afterDelete =
                    new FileJobRepository(directory, 20);
            assertNull(afterDelete.get("job-file-1"));
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void shouldReadLegacyCheckpointVersionAsStateRevision()
            throws Exception {
        Path directory = Files.createTempDirectory("link-up-legacy-state-");
        try {
            String jobId = "job-legacy-state-1";
            String legacyJson =
                    "{\n"
                            + "  \"formatVersion\": 3,\n"
                            + "  \"jobId\": \"" + jobId + "\",\n"
                            + "  \"jobName\": \"legacy-state\",\n"
                            + "  \"status\": \"FAILED\",\n"
                            + "  \"createTimeMillis\": 1,\n"
                            + "  \"startTimeMillis\": 2,\n"
                            + "  \"endTimeMillis\": 3,\n"
                            + "  \"metadata\": {\n"
                            + "    \"externalExecutionId\": \"legacy-external\",\n"
                            + "    \"idempotencyKey\": \"legacy-key\",\n"
                            + "    \"definitionVersion\": 1,\n"
                            + "    \"configDigest\": \"legacy-digest\",\n"
                            + "    \"submittedTimeMillis\": 1,\n"
                            + "    \"queuedTimeMillis\": 1,\n"
                            + "    \"stateVersion\": 5,\n"
                            + "    \"checkpointVersion\": 7,\n"
                            + "    \"cancellationRequested\": false,\n"
                            + "    \"transitions\": [],\n"
                            + "    \"attempts\": []\n"
                            + "  }\n"
                            + "}";

            Files.write(
                    stateFile(directory, jobId),
                    legacyJson.getBytes(StandardCharsets.UTF_8));

            FileJobRepository repository =
                    new FileJobRepository(directory, 20);

            assertNotNull(repository.get(jobId));
            assertEquals(
                    7L,
                    repository.getMetadata(jobId).getStateRevision());
        } finally {
            deleteDirectory(directory);
        }
    }

    private static String readStateFile(
            Path directory,
            String jobId)
            throws Exception {
        return new String(
                Files.readAllBytes(stateFile(directory, jobId)),
                StandardCharsets.UTF_8);
    }

    private static Path stateFile(
            Path directory,
            String jobId) {
        String encoded = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                        jobId.getBytes(StandardCharsets.UTF_8));
        return directory.resolve(encoded + ".job.json");
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
