package com.link.up.framework.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.framework.classloading.ClassLoaderScope;
import com.link.up.framework.connector.PreparedSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Coordinates bounded Source split discovery for the local planner.
 *
 * <p>This class owns the framework side of the enumerator lifecycle: it opens
 * the connector classloader scope, creates one {@link SourceSplitEnumerator},
 * validates its output and closes it on both success and failure. Connector
 * configuration/parallelism validation happens earlier in ConnectorPreparer so
 * unsupported modes fail before sink preparation side effects occur.</p>
 *
 * <p>The coordinator does not assign splits to tasks; that remains a planner
 * responsibility.</p>
 */
public final class SourceCoordinator {

    public <SplitT extends SourceSplit> List<SplitT> enumerateSplits(
            PreparedSource<SplitT> preparedSource,
            int parallelism)
            throws Exception {

        PreparedSource<SplitT> source =
                Objects.requireNonNull(
                        preparedSource,
                        "preparedSource must not be null");

        SourceEnumeratorContext context =
                new SourceEnumeratorContext(parallelism);
        Source<SplitT> connectorSource = source.getSource();

        try (ClassLoaderScope ignored =
                     ClassLoaderScope.open(source.getClassLoader())) {

            SourceSplitEnumerator<SplitT> enumerator =
                    connectorSource.createEnumerator(
                            source.getTables(),
                            context);

            if (enumerator == null) {
                throw new IllegalStateException(
                        "Source '"
                                + source.getFactoryIdentifier()
                                + "' returned a null SourceSplitEnumerator");
            }

            try (SourceSplitEnumerator<SplitT> closeable = enumerator) {
                return validateAndCopy(
                        source.getFactoryIdentifier(),
                        closeable.enumerateSplits());
            }
        }
    }

    private <SplitT extends SourceSplit> List<SplitT> validateAndCopy(
            String sourceIdentifier,
            List<SplitT> splits) {

        if (splits == null) {
            throw new IllegalStateException(
                    "Source '"
                            + sourceIdentifier
                            + "' enumerator returned null splits");
        }

        List<SplitT> result =
                new ArrayList<SplitT>(splits.size());
        Map<String, Set<String>> splitIdsByDataSet =
                new HashMap<String, Set<String>>();

        for (SplitT split : splits) {
            if (split == null) {
                throw new IllegalStateException(
                        "Source '"
                                + sourceIdentifier
                                + "' enumerator returned a null split");
            }

            String splitId = requireText(
                    split.splitId(),
                    "splitId",
                    sourceIdentifier);
            String dataSetId = requireText(
                    split.dataSetId(),
                    "dataSetId",
                    sourceIdentifier);

            Set<String> dataSetSplitIds =
                    splitIdsByDataSet.get(dataSetId);
            if (dataSetSplitIds == null) {
                dataSetSplitIds = new HashSet<String>();
                splitIdsByDataSet.put(dataSetId, dataSetSplitIds);
            }

            if (!dataSetSplitIds.add(splitId)) {
                throw new IllegalStateException(
                        "Source '"
                                + sourceIdentifier
                                + "' returned duplicate splitId '"
                                + splitId
                                + "' for dataSetId '"
                                + dataSetId
                                + "'");
            }

            result.add(split);
        }

        return Collections.unmodifiableList(result);
    }

    private String requireText(
            String value,
            String fieldName,
            String sourceIdentifier) {

        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(
                    "Source '"
                            + sourceIdentifier
                            + "' returned a split with blank "
                            + fieldName);
        }
        return value.trim();
    }
}
