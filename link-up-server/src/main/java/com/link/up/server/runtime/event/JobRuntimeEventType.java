package com.link.up.server.runtime.event;

/** Stable control-plane event names emitted for one Job lifecycle. */
public enum JobRuntimeEventType {
    JOB_SUBMITTED,
    JOB_QUEUED,
    JOB_STARTED,
    JOB_LOG_CREATED,
    JOB_CANCEL_REQUESTED,
    JOB_RETRY_CREATED,
    JOB_SUCCEEDED,
    JOB_FAILED,
    JOB_CANCELED,
    JOB_LOST
}
