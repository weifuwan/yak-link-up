package com.link.up.server.infrastructure.runtime;

import com.link.up.framework.execution.JobExecution;
import com.link.up.server.application.port.JobExecutor;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Architecture guards for local runtime ownership. */
public class LocalJobRuntimeSchedulerBoundaryTest {

    @Test
    public void schedulerShouldDelegateFrameworkExecutionToRunner() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        LocalJobRuntimeScheduler.class);

        assertTrue(
                fieldTypes.contains(
                        LocalJobRunner.class));
        assertFalse(
                fieldTypes.contains(
                        JobExecution.class));
        assertFalse(
                fieldTypes.contains(
                        Thread.class));
    }

    @Test
    public void activeExecutionShouldOwnThreadAndFrameworkHandle() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        ActiveJobExecution.class);

        assertTrue(fieldTypes.contains(Thread.class));
        assertTrue(fieldTypes.contains(JobExecution.class));
    }

    @Test
    public void localRunnerShouldDependOnExecutorPort() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        LocalJobRunner.class);

        assertTrue(fieldTypes.contains(JobExecutor.class));
    }

    private static Set<Class<?>> instanceFieldTypes(
            Class<?> type) {

        Set<Class<?>> types =
                new HashSet<Class<?>>();

        for (Field field : type.getDeclaredFields()) {
            if (!java.lang.reflect.Modifier.isStatic(
                    field.getModifiers())) {
                types.add(field.getType());
            }
        }

        return types;
    }
}
