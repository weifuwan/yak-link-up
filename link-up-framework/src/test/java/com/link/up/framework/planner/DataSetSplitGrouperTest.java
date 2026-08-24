package com.link.up.framework.planner;

import com.link.up.api.source.SourceSplit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataSetSplitGrouperTest {

    @Test
    public void shouldPreserveDataSetAndSplitEnumerationOrder() {
        TestSplit firstA =
                new TestSplit(
                        "a-1",
                        "db.a");
        TestSplit firstB =
                new TestSplit(
                        "b-1",
                        "db.b");
        TestSplit secondA =
                new TestSplit(
                        "a-2",
                        "db.a");

        Map<String, List<TestSplit>> grouped =
                DataSetSplitGrouper.group(
                        Arrays.asList(
                                firstA,
                                firstB,
                                secondA));

        assertEquals(
                Arrays.asList(
                        "db.a",
                        "db.b"),
                new ArrayList<String>(
                        grouped.keySet()));

        assertEquals(
                Arrays.asList(
                        firstA,
                        secondA),
                grouped.get("db.a"));
    }

    private static final class TestSplit
            implements SourceSplit {

        private final String splitId;
        private final String dataSetId;

        private TestSplit(
                String splitId,
                String dataSetId) {

            this.splitId = splitId;
            this.dataSetId = dataSetId;
        }

        @Override
        public String splitId() {
            return splitId;
        }

        @Override
        public String dataSetId() {
            return dataSetId;
        }
    }
}
