package com.link.up.framework.planner;

import com.link.up.api.source.SourceSplit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Groups enumerated source splits by data set while preserving source order. */
final class DataSetSplitGrouper {

    private DataSetSplitGrouper() {
    }

    static <SplitT extends SourceSplit> Map<String, List<SplitT>> group(
            List<SplitT> splits) {

        Objects.requireNonNull(
                splits,
                "splits must not be null");

        Map<String, List<SplitT>> byDataSet =
                new LinkedHashMap<String, List<SplitT>>();

        for (SplitT split : splits) {
            Objects.requireNonNull(
                    split,
                    "splits must not contain null values");

            String dataSetId = split.dataSetId().trim();
            List<SplitT> group = byDataSet.get(dataSetId);

            if (group == null) {
                group = new ArrayList<SplitT>();
                byDataSet.put(dataSetId, group);
            }

            group.add(split);
        }

        return byDataSet;
    }
}
