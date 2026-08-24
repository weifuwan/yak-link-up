package com.link.up.framework.planner;

import com.link.up.framework.execution.split.SplitProvider;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertFalse;

/**
 * Architecture guard for the planner/runtime boundary.
 */
public class PlannerBoundaryTest {

    @Test
    public void sourceTaskPlanMustNotOwnRuntimeSplitProvider() {
        for (Field field : SourceTaskPlan.class.getDeclaredFields()) {
            assertFalse(
                    "Planner model must not hold runtime SplitProvider field: "
                            + field.getName(),
                    SplitProvider.class.isAssignableFrom(
                            field.getType()));
        }
    }
}
