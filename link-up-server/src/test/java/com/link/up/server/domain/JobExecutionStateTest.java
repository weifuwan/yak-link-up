package com.link.up.server.domain;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import com.link.up.server.runtime.JobStateTransition;
import com.link.up.server.runtime.ServerJobStatus;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobExecutionStateTest {

    @Test
    public void shouldRecordLifecycleWithoutRuntimeOwnership() {
        JobExecutionState state =
                new JobExecutionState(
                        "job-1",
                        submission("digest-1"));

        state.markSubmitted();
        state.markQueued();
        assertTrue(state.markRunning());
        assertTrue(state.complete(
                ServerJobStatus.LOST,
                null,
                null));

        List<JobStateTransition> transitions =
                state.getTransitions();
        assertEquals(5, transitions.size());
        assertEquals(ServerJobStatus.CREATED,
                transitions.get(0).getToStatus());
        assertEquals(ServerJobStatus.SUBMITTED,
                transitions.get(1).getToStatus());
        assertEquals(ServerJobStatus.QUEUED,
                transitions.get(2).getToStatus());
        assertEquals(ServerJobStatus.RUNNING,
                transitions.get(3).getToStatus());
        assertEquals(ServerJobStatus.LOST,
                transitions.get(4).getToStatus());
        assertEquals(4L, state.getStateVersion());
    }

    private static JobSubmission submission(String digest) {
        ReadonlyConfig options =
                ReadonlyConfig.fromMap(
                        Collections.<String, Object>emptyMap());
        JobDefinition definition =
                new JobDefinition(
                        "protocol-test",
                        new SourceDefinition("test-source", options),
                        new SinkDefinition("test-sink", options),
                        new ExecutionConfig(100, 1, 1, 1));
        return new JobSubmission(
                "execution-1",
                "idempotency-1",
                1,
                digest,
                definition);
    }
}
