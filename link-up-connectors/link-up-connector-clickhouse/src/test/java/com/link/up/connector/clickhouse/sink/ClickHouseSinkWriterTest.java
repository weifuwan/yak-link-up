package com.link.up.connector.clickhouse.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClickHouseSinkWriterTest {

    @Test
    public void flushesAtThresholdAndPrepareCommitFlushesRemainder() throws Exception {
        ClickHouseSinkConfig config = config(2);
        CatalogTable source = sourceTable(schema());
        RecordingExecutor executor = new RecordingExecutor();
        ClickHouseSinkWriter writer = writer(config, source, executor);

        writer.open();
        writer.write(
                batch(
                        Arrays.asList(
                                FluxRow.of(1L, "a"),
                                FluxRow.of(2L, "b"),
                                FluxRow.of(3L, "c"))),
                source);

        assertEquals(1, executor.flushCount);
        assertEquals(1, executor.pendingRows);

        writer.prepareCommit();
        assertEquals(2, executor.flushCount);
        assertEquals(0, executor.pendingRows);

        writer.commit();
        writer.close();
        assertTrue(executor.closed);
    }

    @Test
    public void abortClearsPendingAndCloseDoesNotFlush() throws Exception {
        ClickHouseSinkConfig config = config(10);
        CatalogTable source = sourceTable(schema());
        RecordingExecutor executor = new RecordingExecutor();
        ClickHouseSinkWriter writer = writer(config, source, executor);

        writer.open();
        writer.write(
                batch(Collections.singletonList(FluxRow.of(1L, "a"))),
                source);
        assertEquals(1, executor.pendingRows);
        assertEquals(0, executor.flushCount);

        writer.abort();
        assertEquals(0, executor.pendingRows);
        writer.close();

        assertEquals(0, executor.flushCount);
        assertTrue(executor.closed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsRuntimeSchemaChanges() throws Exception {
        ClickHouseSinkConfig config = config(10);
        CatalogTable source = sourceTable(schema());
        RecordingExecutor executor = new RecordingExecutor();
        ClickHouseSinkWriter writer = writer(config, source, executor);

        TableSchema changed =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                        .column(Column.builder("name", BasicType.STRING_TYPE).build())
                        .column(Column.builder("extra", BasicType.STRING_TYPE).build())
                        .build();

        writer.open();
        try {
            writer.write(
                    batch(Collections.singletonList(FluxRow.of(1L, "a"))),
                    source);
            writer.write(
                    batch(Collections.singletonList(FluxRow.of(2L, "b", "x"))),
                    sourceTable(changed));
        } finally {
            writer.close();
        }
    }

    private static RecordBatch<FluxRow> batch(List<FluxRow> rows) {
        return RecordBatch.of("source.orders", "split-0", rows);
    }

    private static ClickHouseSinkWriter writer(
            ClickHouseSinkConfig config,
            CatalogTable source,
            RecordingExecutor executor) {
        Map<TablePath, CatalogTable> targets = new LinkedHashMap<TablePath, CatalogTable>();
        targets.put(
                source.getTablePath(),
                CatalogTable.builder(TablePath.of("analytics", "orders"), source.getTableSchema())
                        .build());
        return new ClickHouseSinkWriter(config, new PreparedSinkMetadata(targets), executor);
    }

    private static ClickHouseSinkConfig config(int batchSize) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("host", "ch-1:8123");
        values.put("username", "default");
        values.put("password", "");
        values.put("database", "analytics");
        values.put("table", "orders");
        values.put("sink.batch_size", batchSize);
        return ClickHouseSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static TableSchema schema() {
        return TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .build();
    }

    private static CatalogTable sourceTable(TableSchema schema) {
        return CatalogTable.builder(TablePath.of("source", "orders"), schema).build();
    }

    private static final class RecordingExecutor implements ClickHouseSinkWriter.BatchExecutor {
        private int pendingRows;
        private int flushCount;
        private boolean opened;
        private boolean closed;

        @Override
        public void open(CatalogTable sourceTable, CatalogTable targetTable) {
            opened = true;
        }

        @Override
        public void add(FluxRow row) {
            if (!opened) {
                throw new IllegalStateException("not open");
            }
            pendingRows++;
        }

        @Override
        public int flush() {
            int rows = pendingRows;
            pendingRows = 0;
            flushCount++;
            return rows;
        }

        @Override
        public void clearBatch() {
            pendingRows = 0;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
