package com.link.up.api.source;

import java.util.List;

/**
 * Discovers bounded splits for one Source planning attempt.
 *
 * <p>An enumerator is created by {@link Source#createEnumerator} and may own
 * temporary split-planning resources. The framework closes it after
 * enumeration, including when enumeration fails. It must not own SourceReader
 * instances or task-runtime state.</p>
 */
public interface SourceSplitEnumerator<SplitT extends SourceSplit>
        extends AutoCloseable {

    /**
     * Returns all splits represented by this bounded Source.
     */
    List<SplitT> enumerateSplits() throws Exception;

    @Override
    default void close() throws Exception {
        // Default enumerators have no resources to release.
    }
}
