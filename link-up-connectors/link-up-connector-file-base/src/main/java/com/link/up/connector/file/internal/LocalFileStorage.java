package com.link.up.connector.file.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/** Local filesystem storage backed by java.nio, zero extra dependencies. */
public final class LocalFileStorage implements FileStorage {

    @Override
    public List<FileEntry> listFiles(
            String basePath,
            boolean recursive) {

        Path base = Paths.get(Objects.requireNonNull(basePath, "basePath must not be null"));

        if (Files.isRegularFile(base)) {
            return Collections.singletonList(entryOf(base));
        }
        if (!Files.isDirectory(base)) {
            throw new IllegalArgumentException("Local path does not exist: " + basePath);
        }

        List<FileEntry> files = new ArrayList<FileEntry>();
        try (Stream<Path> paths = recursive
                ? Files.walk(base, FileVisitOption.FOLLOW_LINKS)
                : Files.list(base)) {

            for (Path path : paths.sorted().collect(java.util.stream.Collectors.toList())) {
                if (Files.isRegularFile(path)) {
                    files.add(entryOf(path));
                }
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Could not list local files under: " + basePath,
                    failure);
        }
        files.sort((left, right) -> left.getFileKey().compareTo(right.getFileKey()));
        return files;
    }

    @Override
    public InputStream openRange(
            String fileKey,
            long start,
            long length) {

        Path path = Paths.get(Objects.requireNonNull(fileKey, "fileKey must not be null"));
        try {
            SeekableByteChannel channel =
                    Files.newByteChannel(path, EnumSet.of(StandardOpenOption.READ));
            boolean opened = false;
            try {
                channel.position(start);
                InputStream bounded =
                        new BoundedRangeInputStream(channel, length, fileKey);
                opened = true;
                return bounded;
            } finally {
                if (!opened) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        // The open failure is the primary signal.
                    }
                }
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Could not open local file range: " + fileKey + " [" + start + ", " + length + ")",
                    failure);
        }
    }

    @Override
    public boolean exists(String fileKey) {
        return Files.exists(Paths.get(fileKey));
    }

    @Override
    public void close() {
        // Nothing to release.
    }

    private static FileEntry entryOf(Path path) {
        try {
            return new FileEntry(path.toAbsolutePath().toString(), Files.size(path));
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "Could not stat local file: " + path,
                    failure);
        }
    }

    /**
     * Reads at most {@code remaining} bytes from the positioned channel; EOF
     * after that keeps every split inside its planned byte range.
     */
    private static final class BoundedRangeInputStream extends InputStream {

        private final SeekableByteChannel channel;
        private final ByteBuffer buffer = ByteBuffer.allocate(8192);
        private final String fileKey;
        private long remaining;
        private boolean closed;

        private BoundedRangeInputStream(
                SeekableByteChannel channel,
                long length,
                String fileKey) {

            this.channel = channel;
            this.remaining = length;
            this.fileKey = fileKey;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int read = read(one, 0, 1);
            return read < 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] target, int offset, int count) throws IOException {
            if (closed) {
                throw new java.io.IOException("Range stream already closed: " + fileKey);
            }
            if (remaining <= 0) {
                return -1;
            }

            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), Math.min(count, remaining)));
            int read = channel.read(buffer);
            if (read < 0) {
                remaining = 0;
                return -1;
            }
            remaining -= read;
            buffer.flip();
            buffer.get(target, offset, read);
            return read;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            channel.close();
        }
    }
}
