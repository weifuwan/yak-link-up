package com.link.up.server.application.port;

import com.link.up.server.runtime.event.JobEventEnvelope;

/** Observer port for one ordered Job lifecycle event. */
public interface JobEventListener {

    void onEvent(JobEventEnvelope event);
}
