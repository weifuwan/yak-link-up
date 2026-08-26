package com.link.up.server.application.port;

/** Raised when persisted Job event history cannot be read or appended. */
public final class JobEventHistoryException
        extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JobEventHistoryException(
            String message,
            Throwable cause) {
        super(message, cause);
    }
}
