package com.link.up.server.application;

import com.link.up.server.application.port.JobEventListener;
import com.link.up.server.runtime.event.JobEventEnvelope;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Synchronous ordered event dispatcher with listener-failure isolation.
 *
 * <p>JobSnapshot remains the control-plane source of truth in this phase. A
 * failing observer is logged and isolated so observability storage cannot
 * change Job execution semantics.</p>
 */
public final class JobEventBus
        implements JobEventListener {

    private static final Logger LOG =
            LogManager.getLogger(JobEventBus.class);

    private static final JobEventBus NOOP =
            new JobEventBus(
                    Collections.<JobEventListener>emptyList());

    private final List<JobEventListener> listeners;

    public JobEventBus(
            Iterable<? extends JobEventListener> listeners) {

        Objects.requireNonNull(
                listeners,
                "listeners must not be null");

        List<JobEventListener> copy =
                new ArrayList<JobEventListener>();

        for (JobEventListener listener : listeners) {
            copy.add(
                    Objects.requireNonNull(
                            listener,
                            "listeners must not contain null"));
        }

        this.listeners =
                Collections.unmodifiableList(copy);
    }

    public static JobEventBus noop() {
        return NOOP;
    }

    @Override
    public void onEvent(JobEventEnvelope event) {
        publish(event);
    }

    public void publish(JobEventEnvelope event) {
        JobEventEnvelope safeEvent =
                Objects.requireNonNull(
                        event,
                        "event must not be null");

        for (JobEventListener listener : listeners) {
            try {
                listener.onEvent(safeEvent);
            } catch (RuntimeException failure) {
                LOG.error(
                        "Job event listener failed, jobId={}, attemptId={}, sequence={}, eventType={}, listener={}",
                        safeEvent.getJobId(),
                        safeEvent.getAttemptId(),
                        safeEvent.getSequence(),
                        safeEvent.getEvent().getType(),
                        listener.getClass().getName(),
                        failure);
            }
        }
    }
}
