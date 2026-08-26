package com.link.up.server.infrastructure.persistence;

import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.runtime.JobAttemptMetadata;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobRecoverySnapshotFactory;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.ServerJobStatus;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileJobRepositoryStructuredErrorTest {

    @Test
    public void shouldPersistStructuredFailureMetadataAcrossReopen()
            throws Exception {

        Path directory = Files.createTempDirectory(
                "link-up-structured-error-");

        try {
            FileJobRepository repository =
                    new FileJobRepository(directory, 20);

            JobAttemptMetadata attempt =
                    new JobAttemptMetadata(
                            1,
                            "structured-error-job-attempt-1",
                            JobAttemptStatus.FAILED,
                            1L,
                            1L,
                            2L,
                            3L,
                            null,
                            null,
                            "PlanningException",
                            "safe message",
                            "Do not retry until the definition changes.",
                            "PLAN-005",
                            "CAPABILITY",
                            "CAPABILITY_NEGOTIATION",
                            false,
                            "NONE",
                            true,
                            0,
                            0L,
                            0L,
                            false,
                            null);

            JobExecutionMetadata metadata =
                    new JobExecutionMetadata(
                            "external",
                            "key",
                            1,
                            "digest",
                            1L,
                            1L,
                            4L,
                            4L,
                            false,
                            Collections.emptyList(),
                            Collections.emptyMap(),
                            null,
                            null,
                            Collections.singletonList(attempt));
            JobSnapshot snapshot =
                    JobRecoverySnapshotFactory.restoreBasic(
                            "structured-error-job",
                            "structured-error",
                            ServerJobStatus.FAILED,
                            1L,
                            2L,
                            3L,
                            "PLAN-005",
                            "A required Connector capability is missing");

            repository.save(snapshot, metadata);

            FileJobRepository reopened =
                    new FileJobRepository(directory, 20);
            JobAttemptMetadata restored =
                    reopened.getMetadata(
                                    "structured-error-job")
                            .getAttempts()
                            .get(0);

            assertEquals("PLAN-005", restored.getErrorCode());
            assertEquals(
                    "CAPABILITY",
                    restored.getErrorCategory());
            assertEquals(
                    "CAPABILITY_NEGOTIATION",
                    restored.getErrorPhase());
            assertFalse(restored.isFailureRetryable());
            assertEquals(
                    "NONE",
                    restored.getFailureRetryScope());

            String persisted = new String(
                    Files.readAllBytes(
                            firstRegularFile(directory)),
                    StandardCharsets.UTF_8);
            assertTrue(
                    persisted.contains(
                            "\"formatVersion\":3"));
            assertTrue(persisted.contains("PLAN-005"));
        } finally {
            deleteRecursively(directory);
        }
    }

    private Path firstRegularFile(Path directory)
            throws Exception {

        try (java.util.stream.Stream<Path> paths =
                     Files.walk(directory)) {
            return paths.filter(Files::isRegularFile)
                    .findFirst()
                    .orElseThrow(
                            () -> new AssertionError(
                                    "Expected persisted checkpoint file"));
        }
    }

    private void deleteRecursively(Path directory)
            throws Exception {

        if (directory == null || !Files.exists(directory)) {
            return;
        }

        try (java.util.stream.Stream<Path> paths =
                     Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(
                            path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (Exception failure) {
                                    throw new RuntimeException(failure);
                                }
                            });
        }
    }
}
