package com.link.up.framework.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.framework.connector.PreparedSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SourceCoordinatorTest {

    @Test
    public void shouldAdaptLegacyCreateSplitsSource() throws Exception {
        final boolean[] legacyCalled = {false};
        Source<TestSplit> source = new Source<TestSplit>() {
            @Override
            public List<TestSplit> createSplits(
                    Map<TablePath, CatalogTable> tables) {
                legacyCalled[0] = true;
                return Collections.singletonList(
                        new TestSplit("legacy-1", "default"));
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> tables,
                    int batchSize) {
                return null;
            }
        };

        List<TestSplit> splits =
                new SourceCoordinator().enumerateSplits(
                        new PreparedSource<TestSplit>(
                                "legacy",
                                source,
                                Collections.<TablePath, CatalogTable>emptyMap()),
                        3);

        assertTrue(legacyCalled[0]);
        assertEquals(1, splits.size());
        assertEquals("legacy-1", splits.get(0).splitId());
    }

    @Test
    public void shouldUseNativeEnumeratorContextClassLoaderAndClose()
            throws Exception {

        final ClassLoader previous =
                Thread.currentThread().getContextClassLoader();
        final ClassLoader connectorLoader =
                new ClassLoader(previous) {
                };
        final boolean[] enumeratorCreated = {false};
        final boolean[] enumerated = {false};
        final boolean[] closed = {false};

        Source<TestSplit> source = new Source<TestSplit>() {
            @Override
            public SourceSplitEnumerator<TestSplit> createEnumerator(
                    Map<TablePath, CatalogTable> tables,
                    SourceEnumeratorContext context) {

                enumeratorCreated[0] = true;
                assertEquals(4, context.getParallelism());
                assertSame(
                        connectorLoader,
                        Thread.currentThread().getContextClassLoader());

                return new SourceSplitEnumerator<TestSplit>() {
                    @Override
                    public List<TestSplit> enumerateSplits() {
                        enumerated[0] = true;
                        assertSame(
                                connectorLoader,
                                Thread.currentThread()
                                        .getContextClassLoader());
                        return Arrays.asList(
                                new TestSplit("s1", "demo.users"),
                                new TestSplit("s2", "demo.users"));
                    }

                    @Override
                    public void close() {
                        closed[0] = true;
                        assertSame(
                                connectorLoader,
                                Thread.currentThread()
                                        .getContextClassLoader());
                    }
                };
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> tables,
                    int batchSize) {
                return null;
            }
        };

        List<TestSplit> splits =
                new SourceCoordinator().enumerateSplits(
                        new PreparedSource<TestSplit>(
                                "native",
                                source,
                                Collections.<TablePath, CatalogTable>emptyMap(),
                                connectorLoader),
                        4);

        assertTrue(enumeratorCreated[0]);
        assertTrue(enumerated[0]);
        assertTrue(closed[0]);
        assertSame(
                previous,
                Thread.currentThread().getContextClassLoader());
        assertEquals(2, splits.size());

        try {
            splits.add(new TestSplit("s3", "demo.users"));
            fail("enumerated split view must be immutable");
        } catch (UnsupportedOperationException expected) {
            // expected
        }
    }

    @Test
    public void shouldCloseEnumeratorWhenEnumerationFails() {
        final boolean[] closed = {false};
        final RuntimeException failure =
                new RuntimeException("enumeration failed");

        Source<TestSplit> source = new Source<TestSplit>() {
            @Override
            public SourceSplitEnumerator<TestSplit> createEnumerator(
                    Map<TablePath, CatalogTable> tables,
                    SourceEnumeratorContext context) {
                return new SourceSplitEnumerator<TestSplit>() {
                    @Override
                    public List<TestSplit> enumerateSplits() {
                        throw failure;
                    }

                    @Override
                    public void close() {
                        closed[0] = true;
                    }
                };
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> tables,
                    int batchSize) {
                return null;
            }
        };

        try {
            new SourceCoordinator().enumerateSplits(
                    new PreparedSource<TestSplit>(
                            "failing",
                            source,
                            Collections.<TablePath, CatalogTable>emptyMap()),
                    1);
            fail("enumeration failure should be propagated");
        } catch (Exception actual) {
            assertSame(failure, actual);
        }

        assertTrue(closed[0]);
    }

    @Test
    public void shouldRejectDuplicateSplitIdentityWithinDataSet() {
        Source<TestSplit> source = sourceWithSplits(
                Arrays.asList(
                        new TestSplit("duplicate", "demo.users"),
                        new TestSplit("duplicate", "demo.users")));

        try {
            new SourceCoordinator().enumerateSplits(
                    new PreparedSource<TestSplit>(
                            "duplicates",
                            source,
                            Collections.<TablePath, CatalogTable>emptyMap()),
                    1);
            fail("duplicate split identity should be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("duplicate splitId"));
        } catch (Exception unexpected) {
            throw new AssertionError(unexpected);
        }
    }

    @Test
    public void shouldAllowSameSplitIdAcrossDifferentDataSets() throws Exception {
        Source<TestSplit> source = sourceWithSplits(
                Arrays.asList(
                        new TestSplit("split-1", "demo.users"),
                        new TestSplit("split-1", "demo.orders")));

        List<TestSplit> splits =
                new SourceCoordinator().enumerateSplits(
                        new PreparedSource<TestSplit>(
                                "multi-dataset",
                                source,
                                Collections.<TablePath, CatalogTable>emptyMap()),
                        1);

        assertEquals(2, splits.size());
        assertFalse(splits.isEmpty());
    }

    private static Source<TestSplit> sourceWithSplits(
            final List<TestSplit> splits) {
        return new Source<TestSplit>() {
            @Override
            public SourceSplitEnumerator<TestSplit> createEnumerator(
                    Map<TablePath, CatalogTable> tables,
                    SourceEnumeratorContext context) {
                return new SourceSplitEnumerator<TestSplit>() {
                    @Override
                    public List<TestSplit> enumerateSplits() {
                        return splits;
                    }
                };
            }

            @Override
            public SourceReader<FluxRow, TestSplit> createReader(
                    Map<TablePath, CatalogTable> tables,
                    int batchSize) {
                return null;
            }
        };
    }

    private static final class TestSplit implements SourceSplit {
        private static final long serialVersionUID = 1L;

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
