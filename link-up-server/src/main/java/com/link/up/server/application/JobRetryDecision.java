package com.link.up.server.application;

/** Immutable result of evaluating whether a terminal Job can start another attempt. */
public final class JobRetryDecision {

    public static final String SAFE_NO_DATA_COMMITTED =
            "SAFE_NO_DATA_COMMITTED";
    public static final String JOB_ACTIVE =
            "JOB_ACTIVE";
    public static final String ALREADY_SUCCEEDED =
            "ALREADY_SUCCEEDED";
    public static final String CANCELED_OUTCOME =
            "CANCELED_OUTCOME";
    public static final String LOST_OUTCOME_UNKNOWN =
            "LOST_OUTCOME_UNKNOWN";
    public static final String EVIDENCE_UNAVAILABLE =
            "EVIDENCE_UNAVAILABLE";
    public static final String UNKNOWN_COMMIT_STATE =
            "UNKNOWN_COMMIT_STATE";
    public static final String DATA_ALREADY_COMMITTED =
            "DATA_ALREADY_COMMITTED";
    public static final String NON_RETRYABLE_FAILURE =
            "NON_RETRYABLE_FAILURE";

    private final boolean eligible;
    private final String code;
    private final String message;
    private final int nextAttemptNumber;

    private JobRetryDecision(
            boolean eligible,
            String code,
            String message,
            int nextAttemptNumber) {

        this.eligible = eligible;
        this.code = code;
        this.message = message;
        this.nextAttemptNumber = nextAttemptNumber;
    }

    public static JobRetryDecision allow(
            String code,
            String message,
            int nextAttemptNumber) {
        return new JobRetryDecision(
                true,
                code,
                message,
                nextAttemptNumber);
    }

    public static JobRetryDecision deny(
            String code,
            String message,
            int nextAttemptNumber) {
        return new JobRetryDecision(
                false,
                code,
                message,
                nextAttemptNumber);
    }

    public boolean isEligible() { return eligible; }
    public String getCode() { return code; }
    public String getMessage() { return message; }
    public int getNextAttemptNumber() { return nextAttemptNumber; }
}
