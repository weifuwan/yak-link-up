package com.link.up.framework.planner;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SplitAssignerTest {

    private final SplitAssigner assigner = new SplitAssigner();

    @Test
    public void shouldRoundRobinAssignmentsAcrossSourceTasks() {
        List<String> splits = Arrays.asList("a", "b", "c", "d", "e");

        List<List<String>> assignments = assigner.assign(splits, 3);

        assertEquals(3, assignments.size());
        assertEquals(Arrays.asList("a", "d"), assignments.get(0));
        assertEquals(Arrays.asList("b", "e"), assignments.get(1));
        assertEquals(Collections.singletonList("c"), assignments.get(2));
    }

    @Test
    public void shouldNotCreateMoreTasksThanSplits() {
        List<List<String>> assignments =
                assigner.assign(Collections.singletonList("only"), 8);

        assertEquals(1, assignments.size());
        assertEquals(Collections.singletonList("only"), assignments.get(0));
    }

    @Test
    public void shouldReturnEmptyAssignmentsForEmptySplits() {
        assertTrue(assigner.assign(Collections.<String>emptyList(), 4).isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonPositiveParallelism() {
        assigner.assign(Collections.singletonList("only"), 0);
    }
}
