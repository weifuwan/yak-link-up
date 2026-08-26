package com.link.up.api.exception;

/** Stable lifecycle phase in which a Link-Up failure occurred. */
public enum FluxErrorPhase {
    UNKNOWN,
    PROTOCOL,
    COMPILE,
    CONNECTOR_RESOLUTION,
    OPTION_VALIDATION,
    CAPABILITY_NEGOTIATION,
    SOURCE_DISCOVERY,
    SPLIT_DISCOVERY,
    SINK_PREPARATION,
    PHYSICAL_PLANNING,
    EXECUTION,
    PERSISTENCE
}
