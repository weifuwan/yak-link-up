package com.link.up.framework.planner;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Architecture guards for the planner/runtime boundary.
 */
public class PlannerBoundaryTest {

    private static final Class<?>[] PLANNER_MODELS = {
            JobGraph.class,
            PipelineGraph.class,
            SourceTaskPlan.class,
            SinkTaskPlan.class
    };

    private static final String[] FORBIDDEN_RUNTIME_TYPES = {
            "com.link.up.framework.execution.CancellationToken",
            "com.link.up.framework.execution.split.",
            "com.link.up.framework.metrics.",
            "com.link.up.framework.channel.",
            "java.util.concurrent.Executor",
            "java.util.concurrent.Future"
    };

    @Test
    public void plannerModelsMustNotOwnRuntimeState() {
        for (Class<?> model : PLANNER_MODELS) {
            for (Field field : model.getDeclaredFields()) {
                String typeName = field.getGenericType().getTypeName();

                for (String forbidden : FORBIDDEN_RUNTIME_TYPES) {
                    assertFalse(
                            model.getSimpleName()
                                    + "."
                                    + field.getName()
                                    + " must not reference runtime ownership type "
                                    + forbidden,
                            typeName.contains(forbidden));
                }
            }
        }
    }

    @Test
    public void plannerModelFieldsMustBeFinal() {
        for (Class<?> model : PLANNER_MODELS) {
            for (Field field : model.getDeclaredFields()) {
                assertTrue(
                        model.getSimpleName()
                                + "."
                                + field.getName()
                                + " must be final",
                        Modifier.isFinal(field.getModifiers()));
            }
        }
    }
}
