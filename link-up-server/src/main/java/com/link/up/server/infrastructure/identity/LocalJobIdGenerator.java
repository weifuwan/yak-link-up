package com.link.up.server.infrastructure.identity;

import com.link.up.server.application.port.JobIdGenerator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-local job ID generator adapter.
 */
public final class LocalJobIdGenerator
        implements JobIdGenerator {

    private final AtomicLong sequence =
            new AtomicLong();

    @Override
    public String nextId() {
        return "flux-"
                + System.currentTimeMillis()
                + "-"
                + sequence.incrementAndGet();
    }
}
