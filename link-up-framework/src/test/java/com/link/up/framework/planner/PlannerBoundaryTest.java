package com.link.up.framework.planner;

import com.link.up.framework.source.SourceCoordinator;
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

    @Test
    public void jobPlannerMustDelegateSourceDiscoveryToSourceCoordinator() {
        boolean foundSourceCoordinator = false;

        for (Field field : JobPlanner.class.getDeclaredFields()) {
            if (SourceCoordinator.class.isAssignableFrom(field.getType())) {
                foundSourceCoordinator = true;
            }

            String typeName = field.getGenericType().getTypeName();
            assertFalse(
                    "JobPlanner must not own SourceSplitEnumerator directly",
                    typeName.contains(
                            "com.link.up.api.source.SourceSplitEnumerator"));
        }

        assertTrue(
                "JobPlanner must delegate split discovery to SourceCoordinator",
                foundSourceCoordinator);
    }
}
