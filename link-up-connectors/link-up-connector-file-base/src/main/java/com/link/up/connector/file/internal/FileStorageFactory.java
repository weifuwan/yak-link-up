package com.link.up.connector.file.internal;

import com.link.up.connector.file.config.FileSourceBaseConfig;

/**
 * The seam between the shared file engine and the storage leaf modules: each
 * leaf connector supplies the storage implementation for its transport.
 */
@FunctionalInterface
public interface FileStorageFactory {

    FileStorage create();
}
