package com.link.up.server.domain;

/** Lifecycle of one concrete execution attempt of a Worker job. */
public enum JobAttemptStatus {
    CREATED(false),
    QUEUED(false),
    RUNNING(false),
    SUCCEEDED(true),
    FAILED(true),
    CANCELED(true),
    LOST(true);

    private final boolean terminal;

    JobAttemptStatus(boolean terminal) {
        this.terminal = terminal;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
