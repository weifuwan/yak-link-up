package com.link.up.server.application;

import com.link.up.server.application.port.JobEventListener;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobRuntimeEvent;
import com.link.up.server.runtime.event.JobRuntimeEventType;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertSame;

public class JobEventBusTest {

    @Test
    public void listenerFailureMustNotBlockLaterListeners() {
        final JobEventEnvelope[] received =
                new JobEventEnvelope[1];

        JobEventListener failing =
                new JobEventListener() {
                    @Override
                    public void onEvent(JobEventEnvelope event) {
                        throw new IllegalStateException("observer failed");
                    }
                };

        JobEventListener succeeding =
                new JobEventListener() {
                    @Override
                    public void onEvent(JobEventEnvelope event) {
                        received[0] = event;
                    }
                };

        JobEventEnvelope event = JobEventEnvelope.create(
                "job-1",
                "job-1-attempt-1",
                1,
                1L,
                System.currentTimeMillis(),
                JobRuntimeEvent.transition(
                        JobRuntimeEventType.JOB_SUBMITTED,
                        ServerJobStatus.CREATED,
                        ServerJobStatus.SUBMITTED,
                        "submission-accepted"));

        new JobEventBus(
                Arrays.asList(failing, succeeding))
                .publish(event);

        assertSame(event, received[0]);
    }
}
