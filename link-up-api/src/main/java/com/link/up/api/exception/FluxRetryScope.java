package com.link.up.api.exception;

/** Smallest safe scope at which a structured failure may be retried. */
public enum FluxRetryScope {
    NONE,
    TASK,
    PIPELINE,
    JOB
}
