package com.link.up.framework.execution;

import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.PipelineGraph;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Architecture guards for Link-Up runtime ownership roles. */
public class ExecutionRoleBoundaryTest {

    @Test
    public void jobExecutionShouldRemainAThinFacade() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(JobExecution.class);

        assertTrue(
                fieldTypes.contains(
                        ExecutionGraph.class));
        assertTrue(
                fieldTypes.contains(
                        JobCoordinator.class));
        assertFalse(
                fieldTypes.contains(
                        JobGraph.class));
        assertFalse(
                fieldTypes.contains(
                        PipelineGraph.class));

        assertNoExecutorService(
                JobExecution.class);
    }

    @Test
    public void jobCoordinatorShouldDependOnSchedulerAndExecutorRoles() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        JobCoordinator.class);

        assertTrue(
                fieldTypes.contains(
                        ExecutionGraph.class));
        assertTrue(
                fieldTypes.contains(
                        PipelineScheduler.class));
        assertTrue(
                fieldTypes.contains(
                        PipelineExecutor.class));
        assertFalse(
                fieldTypes.contains(
                        ClassLoader.class));

        assertNoExecutorService(
                JobCoordinator.class);
    }

    @Test
    public void localPipelineSchedulerShouldNotOwnExecutionState() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        LocalPipelineScheduler.class);

        assertFalse(
                fieldTypes.contains(
                        ExecutionGraph.class));
        assertFalse(
                fieldTypes.contains(
                        JobGraph.class));
        assertFalse(
                fieldTypes.contains(
                        PipelineGraph.class));
    }

    @Test
    public void pipelineExecutionShouldDelegateMutableResources() {
        assertNoFieldTypeName(
                PipelineExecution.class,
                "com.link.up.framework.channel.DataChannel",
                "com.link.up.framework.execution.split.SplitProvider");

        assertHasFieldTypeName(
                PipelineRuntimeResources.class,
                "com.link.up.framework.channel.DataChannel");
        assertHasFieldTypeName(
                PipelineRuntimeResources.class,
                "com.link.up.framework.execution.split.SplitProvider");
    }

    private static Set<Class<?>> instanceFieldTypes(
            Class<?> type) {

        Set<Class<?>> fieldTypes =
                new HashSet<Class<?>>();

        for (Field field : type.getDeclaredFields()) {
            if (!Modifier.isStatic(
                    field.getModifiers())) {
                fieldTypes.add(field.getType());
            }
        }

        return fieldTypes;
    }

    private static void assertNoExecutorService(
            Class<?> type) {

        for (Field field : type.getDeclaredFields()) {
            if (Modifier.isStatic(
                    field.getModifiers())) {
                continue;
            }

            assertFalse(
                    type.getSimpleName()
                            + " must not own ExecutorService field "
                            + field.getName(),
                    ExecutorService.class.isAssignableFrom(
                            field.getType()));
        }
    }

    private static void assertNoFieldTypeName(
            Class<?> type,
            String... forbiddenTypes) {

        for (Field field : type.getDeclaredFields()) {
            String typeName =
                    field.getGenericType()
                            .getTypeName();

            for (String forbidden : forbiddenTypes) {
                assertFalse(
                        type.getSimpleName()
                                + "."
                                + field.getName()
                                + " must not own "
                                + forbidden,
                        typeName.contains(forbidden));
            }
        }
    }

    private static void assertHasFieldTypeName(
            Class<?> type,
            String expectedType) {

        for (Field field : type.getDeclaredFields()) {
            if (field.getGenericType()
                    .getTypeName()
                    .contains(expectedType)) {
                return;
            }
        }

        throw new AssertionError(
                type.getSimpleName()
                        + " must own "
                        + expectedType);
    }
}
