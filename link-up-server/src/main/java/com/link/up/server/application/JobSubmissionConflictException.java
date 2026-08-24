package com.link.up.server.application;

/**
 * The same idempotency key or external execution ID was reused with
 * different submission content.
 */
public final class JobSubmissionConflictException
        extends RuntimeException {

    public JobSubmissionConflictException(
            String message) {
        super(message);
    }
}
