package com.link.up.framework.planning;

import java.util.Objects;

/** Stable, secret-safe diagnostic emitted by the planning boundary. */
public final class PlanningDiagnostic {

    public enum Severity {
        INFO,
        WARNING,
        ERROR
    }

    private final String code;
    private final Severity severity;
    private final String phase;
    private final String message;

    public PlanningDiagnostic(
            String code,
            Severity severity,
            String phase,
            String message) {

        this.code = requireText(code, "code");
        this.severity = Objects.requireNonNull(
                severity,
                "severity must not be null");
        this.phase = requireText(phase, "phase");
        this.message = requireText(message, "message");
    }

    public String getCode() {
        return code;
    }

    public Severity getSeverity() {
        return severity;
    }

    public String getPhase() {
        return phase;
    }

    public String getMessage() {
        return message;
    }

    private static String requireText(
            String value,
            String name) {

        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
        return value.trim();
    }
}
