package com.link.up.api.exception;

/** Stable high-level classification for machine-readable Link-Up failures. */
public enum FluxErrorCategory {
    UNKNOWN,
    VALIDATION,
    DISCOVERY,
    CAPABILITY,
    PREPARATION,
    PLANNING,
    EXECUTION,
    PERSISTENCE,
    INTERNAL
}
