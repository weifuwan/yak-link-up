package com.link.up.api.source;

import java.io.Serializable;

/**
 * Framework-provided context for one Source split-enumeration attempt.
 *
 * <p>The context is intentionally connector-neutral and may grow with stable
 * planning inputs over time. Connector implementations should not depend on
 * framework runtime classes.</p>
 */
public final class SourceEnumeratorContext implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Configured Source reader parallelism for this job.
     */
    private final int parallelism;

    public SourceEnumeratorContext(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException(
                    "parallelism must be greater than 0");
        }

        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }
}
