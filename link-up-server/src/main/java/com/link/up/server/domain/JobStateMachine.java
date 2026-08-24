package com.link.up.server.domain;

import com.link.up.server.runtime.ServerJobStatus;

import java.util.EnumSet;

/**
 * Pure control-plane lifecycle rules for one offline job execution.
 */
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
}
