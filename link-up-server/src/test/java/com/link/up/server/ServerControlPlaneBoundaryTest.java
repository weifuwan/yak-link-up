package com.link.up.server;

import com.link.up.server.application.JobApplicationService;
import com.link.up.server.domain.JobExecutionAttempt;
import com.link.up.server.domain.JobExecutionState;
import com.link.up.server.service.JobRestService;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertFalse;

/** Architecture guards for Server control-plane boundaries. */
public class ServerControlPlaneBoundaryTest {

    @Test
    public void domainStateMustNotOwnLocalRuntimeObjects() {
        assertNoFieldType(
                JobExecutionState.class,
                "java.lang.Thread",
                "java.util.concurrent.Future",
                "java.util.concurrent.Executor",
                "java.util.concurrent.Semaphore",
                "com.link.up.framework.execution.JobExecution");
        assertNoFieldType(
                JobExecutionAttempt.class,
                "java.lang.Thread",
                "java.util.concurrent.Future",
                "java.util.concurrent.Executor",
                "java.util.concurrent.Semaphore",
                "com.link.up.framework.execution.JobExecution");
    }

    @Test
    public void applicationMustDependOnPortsInsteadOfLocalRuntime() {
        assertNoFieldType(
                JobApplicationService.class,
                "java.lang.Thread",
                "java.util.concurrent.Future",
                "java.util.concurrent.Executor",
                "java.util.concurrent.Semaphore",
                "com.link.up.framework.execution.JobExecution",
                "com.link.up.server.infrastructure.");
    }

    @Test
    public void httpAdapterMustNotDependOnInfrastructure() {
        assertNoFieldType(
                JobRestService.class,
                "com.link.up.server.infrastructure.");
    }

    private static void assertNoFieldType(
            Class<?> type,
            String... forbiddenTypes) {
        for (Field field : type.getDeclaredFields()) {
            String fieldType = field.getGenericType().getTypeName();
            for (String forbidden : forbiddenTypes) {
                assertFalse(
                        type.getSimpleName()
                                + "."
                                + field.getName()
                                + " must not depend on "
                                + forbidden,
                        fieldType.contains(forbidden));
            }
        }
    }
}
