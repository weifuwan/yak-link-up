package com.link.up.server.domain;

import com.link.up.server.runtime.ServerJobStatus;

import java.util.EnumSet;

/** Pure control-plane lifecycle rules for the current attempt of one stable Job. */
public final class JobStateMachine {

    private JobStateMachine() {
    }

    public static boolean canTransition(
            ServerJobStatus from,
            ServerJobStatus to) {

        if (from == null || to == null || from == to) {
            return false;
        }
        if (from.isTerminal()) {
            return false;
        }

        switch (from) {
            case CREATED:
                return EnumSet.of(
                                ServerJobStatus.SUBMITTED,
                                ServerJobStatus.CANCELED,
                                ServerJobStatus.FAILED,
                                ServerJobStatus.LOST)
                        .contains(to);
            case SUBMITTED:
                return EnumSet.of(
                                ServerJobStatus.QUEUED,
                                ServerJobStatus.CANCELED,
                                ServerJobStatus.FAILED,
                                ServerJobStatus.LOST)
                        .contains(to);
            case QUEUED:
                return EnumSet.of(
                                ServerJobStatus.RUNNING,
                                ServerJobStatus.CANCELED,
                                ServerJobStatus.FAILED,
                                ServerJobStatus.LOST)
                        .contains(to);
            case RUNNING:
                return EnumSet.of(
                                ServerJobStatus.SUCCEEDED,
                                ServerJobStatus.FAILED,
                                ServerJobStatus.CANCELED,
                                ServerJobStatus.LOST)
                        .contains(to);
            default:
                return false;
        }
    }

    /** Retry is a deliberate new-attempt transition, not a normal lifecycle transition. */
    public static boolean canRetryTransition(
            ServerJobStatus from,
            ServerJobStatus to) {
        return from == ServerJobStatus.FAILED
                && to == ServerJobStatus.SUBMITTED;
    }

    public static void requireTransition(
            ServerJobStatus from,
            ServerJobStatus to) {
        if (!canTransition(from, to)) {
            throw new IllegalStateException(
                    "Illegal job state transition: "
                            + from
                            + " -> "
                            + to);
        }
    }

    public static void requireRetryTransition(
            ServerJobStatus from,
            ServerJobStatus to) {
        if (!canRetryTransition(from, to)) {
            throw new IllegalStateException(
                    "Illegal retry state transition: "
                            + from
                            + " -> "
                            + to);
        }
    }
}
