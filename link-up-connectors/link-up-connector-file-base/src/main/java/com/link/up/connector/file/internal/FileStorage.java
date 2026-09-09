package com.link.up.connector.file.internal;

import java.io.InputStream;
import java.util.List;

/**
 * Storage seam behind the File Source: list files, read a byte range, check
 * existence. Concrete implementations (local filesystem, S3) live in this
 * package so vendor SDK types never leak into the source/config packages.
 *
 * <p>Each lifecycle (enumerator, reader) owns its own instance and closes it.
 * A fileKey is opaque here: an absolute path for local storage, an object key
 * for S3 (the bucket is fixed per instance).
 */
public interface FileStorage
        extends AutoCloseable {

    /**
     * Lists regular files under the base path, sorted by file key.
     * Directories/prefixes themselves are never returned.
     */
    List<FileEntry> listFiles(String basePath, boolean recursive);

    /**
     * Opens exactly {@code length} bytes starting at {@code start}; reading
     * past the range yields EOF. The range must be within the file.
     */
    InputStream openRange(String fileKey, long start, long length);

    boolean exists(String fileKey);

    /**
     * True when the transport cannot seek (ranges would discard bytes), so
     * every file is planned as one whole split, e.g. SFTP.
     */
    default boolean wholeFileOnly() {
        return false;
    }

    @Override
    void close();
}
