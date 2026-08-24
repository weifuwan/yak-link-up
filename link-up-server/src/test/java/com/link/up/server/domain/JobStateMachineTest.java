package com.link.up.server.domain;

import com.link.up.server.runtime.ServerJobStatus;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobStateMachineTest {

    @Test
    public void shouldAllowOnlyOfflineWorkerLifecycle() {
        assertTrue(JobStateMachine.canTransition(
                ServerJobStatus.CREATED,
                ServerJobStatus.SUBMITTED));
        assertTrue(JobStateMachine.canTransition(
                ServerJobStatus.SUBMITTED,
                ServerJobStatus.QUEUED));
        assertTrue(JobStateMachine.canTransition(
                ServerJobStatus.QUEUED,
                ServerJobStatus.RUNNING));
        assertTrue(JobStateMachine.canTransition(
                ServerJobStatus.RUNNING,
                ServerJobStatus.SUCCEEDED));
        assertTrue(JobStateMachine.canTransition(
                ServerJobStatus.RUNNING,
                ServerJobStatus.FAILED));
        assertTrue(JobStateMachine.canTransition(
                ServerJobStatus.RUNNING,
                ServerJobStatus.CANCELED));
        assertTrue(JobStateMachine.canTransition(
                ServerJobStatus.RUNNING,
                ServerJobStatus.LOST));

        assertFalse(JobStateMachine.canTransition(
                ServerJobStatus.CREATED,
                ServerJobStatus.RUNNING));
        assertFalse(JobStateMachine.canTransition(
                ServerJobStatus.QUEUED,
                ServerJobStatus.SUCCEEDED));
        assertFalse(JobStateMachine.canTransition(
                ServerJobStatus.SUCCEEDED,
                ServerJobStatus.RUNNING));
    }
}
