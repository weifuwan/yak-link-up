package com.link.up.framework.planner;

import com.link.up.framework.source.SourceCoordinator;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Architecture guards for the planner/runtime boundary. */
public class PlannerBoundaryTest {

    private static final Class<?>[] PLANNER_MODELS = {
            JobGraph.class,
            PipelineGraph.class,
            SourceTaskPlan.class,
            SinkTaskPlan.class
    };

    private static final Class<?>[] PLANNER_SERVICES = {
            JobPlanner.class,
            PipelinePlanner.class,
            DataSetSplitGrouper.class
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
        assertNoRuntimeFields(PLANNER_MODELS);
    }

    @Test
    public void plannerServicesMustNotOwnRuntimeState() {
        assertNoRuntimeFields(PLANNER_SERVICES);
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
                        Modifier.isFinal(
                                field.getModifiers()));
            }
        }
    }

    @Test
    public void jobPlannerMustDelegateDiscoveryAndPipelinePlanning() {
        boolean foundSourceCoordinator = false;
        boolean foundPipelinePlanner = false;

        for (Field field : JobPlanner.class.getDeclaredFields()) {
            if (SourceCoordinator.class.isAssignableFrom(
                    field.getType())) {
                foundSourceCoordinator = true;
            }

            if (PipelinePlanner.class.isAssignableFrom(
                    field.getType())) {
                foundPipelinePlanner = true;
            }

            assertFalse(
                    "JobPlanner must not own SourceSplitEnumerator directly",
                    field.getGenericType()
                            .getTypeName()
                            .contains(
                                    "com.link.up.api.source.SourceSplitEnumerator"));
        }

        assertTrue(
                "JobPlanner must delegate split discovery to SourceCoordinator",
                foundSourceCoordinator);
        assertTrue(
                "JobPlanner must delegate per-data-set planning to PipelinePlanner",
                foundPipelinePlanner);
    }

    private static void assertNoRuntimeFields(
            Class<?>[] types) {

        for (Class<?> type : types) {
            for (Field field : type.getDeclaredFields()) {
                String typeName =
                        field.getGenericType()
                                .getTypeName();

                for (String forbidden :
                        FORBIDDEN_RUNTIME_TYPES) {
                    assertFalse(
                            type.getSimpleName()
                                    + "."
                                    + field.getName()
                                    + " must not reference runtime ownership type "
                                    + forbidden,
                            typeName.contains(forbidden));
                }
            }
        }
    }
}
