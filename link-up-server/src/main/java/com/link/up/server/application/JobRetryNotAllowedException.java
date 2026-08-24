package com.link.up.server.application;

/** Raised when a manual retry is requested without sufficient safety evidence. */
public final class JobRetryNotAllowedException extends RuntimeException {

    private final JobRetryDecision decision;

    public JobRetryNotAllowedException(JobRetryDecision decision) {
        super(decision == null
                ? "Job retry is not allowed"
                : decision.getMessage());
        this.decision = decision;
    }

    public JobRetryDecision getDecision() {
        return decision;
    }
}
