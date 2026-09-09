package com.link.up.connector.file.internal;

import java.util.Objects;

/** One regular file discovered by a {@link FileStorage}. */
public final class FileEntry {

    private final String fileKey;
    private final long size;

    public FileEntry(
            String fileKey,
            long size) {

        this.fileKey = Objects.requireNonNull(fileKey, "fileKey must not be null");
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative");
        }
        this.size = size;
    }

    public String getFileKey() {
        return fileKey;
    }

    public long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "FileEntry{fileKey='" + fileKey + "', size=" + size + '}';
    }
}
