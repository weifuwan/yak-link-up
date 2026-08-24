package com.link.up.server.infrastructure.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.server.application.port.JobRepositoryEntry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Owns Worker checkpoint file naming, JSON IO and atomic replacement. */
final class JobStateFileStore {

    private static final String FILE_SUFFIX = ".job.json";

    private final Path stateDirectory;
    private final ObjectMapper mapper = new ObjectMapper();

    JobStateFileStore(Path stateDirectory) {
        if (stateDirectory == null) {
            throw new IllegalArgumentException(
                    "stateDirectory must not be null");
        }

        this.stateDirectory =
                stateDirectory
                        .toAbsolutePath()
                        .normalize();
    }

    void initialize() throws IOException {
        Files.createDirectories(stateDirectory);
    }

    List<JobRepositoryEntry> loadEntries()
            throws IOException {

        List<JobRepositoryEntry> loaded =
                new ArrayList<JobRepositoryEntry>();

        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(
                             stateDirectory,
                             "*" + FILE_SUFFIX)) {

            for (Path file : stream) {
                StoredJobRecord record = read(file);
                record.validateFormat(file.toString());
                loaded.add(record.toEntry());
            }
        }

        return loaded;
    }

    void write(StoredJobRecord record)
            throws IOException {

        Path target = fileFor(record.jobId);
        Path temporary = null;

        try {
            byte[] bytes =
                    mapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsBytes(record);

            temporary =
                    Files.createTempFile(
                            stateDirectory,
                            ".job-state-",
                            ".tmp");

            syncWrite(
                    temporary,
                    bytes);
            replace(
                    temporary,
                    target);

        } finally {
            deleteTemporary(temporary);
        }
    }

    void delete(String jobId)
            throws IOException {
        Files.deleteIfExists(fileFor(jobId));
    }

    Path getStateDirectory() {
        return stateDirectory;
    }

    private StoredJobRecord read(Path file)
            throws IOException {

        try {
            return mapper.readValue(
                    file.toFile(),
                    StoredJobRecord.class);
        } catch (Exception failure) {
            throw new IOException(
                    "Invalid Worker state file: "
                            + file,
                    failure);
        }
    }

    private void syncWrite(
            Path target,
            byte[] bytes)
            throws IOException {

        try (FileOutputStream output =
                     new FileOutputStream(
                             target.toFile())) {

            output.write(bytes);
            output.flush();
            output.getFD().sync();
        }
    }

    private void replace(
            Path source,
            Path target)
            throws IOException {

        try {
            Files.move(
                    source,
                    target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(
                    source,
                    target,
                    StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private Path fileFor(String jobId) {
        String encoded =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(
                                jobId.getBytes(
                                        StandardCharsets.UTF_8));

        return stateDirectory.resolve(
                encoded + FILE_SUFFIX);
    }

    private void deleteTemporary(Path temporary) {
        if (temporary == null) {
            return;
        }

        try {
            Files.deleteIfExists(temporary);
        } catch (IOException ignored) {
            // Best-effort cleanup after a failed or completed atomic write.
        }
    }
}
