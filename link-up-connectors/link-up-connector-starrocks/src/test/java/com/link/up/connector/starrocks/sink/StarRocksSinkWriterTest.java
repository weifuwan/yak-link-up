package com.link.up.connector.starrocks.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.dirtydata.DirtyDataContext;
import com.link.up.api.dirtydata.DirtyDataSummary;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.starrocks.client.sink.StarRocksStreamLoadResponse;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksSinkWriterTest {

    @Test
    public void flushesAtRowThresholdAndPrepareCommitFlushesRemainder() throws Exception {
        StarRocksSinkConfig config = config(2);
        TableSchema schema = schema();
        CatalogTable sourceTable = sourceTable(schema);
        RecordingExecutor executor = new RecordingExecutor();
        StarRocksSinkWriter writer = writer(config, executor);

        writer.open();
        writer.write(
                RecordBatch.data(
                        Arrays.asList(
                                FluxRow.of(1L, "a"),
                                FluxRow.of(2L, "b"),
                                FluxRow.of(3L, "c"))),
                sourceTable);

        assertEquals(1, executor.payloads.size());
        assertTrue(executor.payloads.get(0).contains("\"id\":1"));
        assertTrue(executor.payloads.get(0).contains("\"id\":2"));

        writer.prepareCommit();
        assertEquals(2, executor.payloads.size());
        assertTrue(executor.payloads.get(1).contains("\"id\":3"));

        writer.commit();
        writer.close();
        assertTrue(executor.closed);
    }

    @Test
    public void abortDiscardsUnsentBufferAndCloseDoesNotFlush() throws Exception {
        StarRocksSinkConfig config = config(10);
        RecordingExecutor executor = new RecordingExecutor();
        StarRocksSinkWriter writer = writer(config, executor);

        writer.open();
        writer.write(
                RecordBatch.data(Collections.singletonList(FluxRow.of(1L, "a"))),
                sourceTable(schema()));
        assertEquals(0, executor.payloads.size());

        writer.abort();
        writer.close();

        assertEquals(0, executor.payloads.size());
        assertTrue(executor.closed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsRuntimeSchemaChanges() throws Exception {
        StarRocksSinkConfig config = config(10);
        RecordingExecutor executor = new RecordingExecutor();
        StarRocksSinkWriter writer = writer(config, executor);
        TableSchema first = schema();
        TableSchema second = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .column(Column.builder("extra", BasicType.STRING_TYPE).build())
                .build();

        writer.open();
        try {
            writer.write(
                    RecordBatch.data(Collections.singletonList(FluxRow.of(1L, "a"))),
                    sourceTable(first));
            writer.write(
                    RecordBatch.data(Collections.singletonList(FluxRow.of(2L, "b", "x"))),
                    sourceTable(second));
        } finally {
            writer.close();
        }
    }

    @Test
    public void recordsFilteredRowsAsDirtyDataEvidence() throws Exception {
        StarRocksSinkConfig config = config(2);
        RecordingExecutor executor = new RecordingExecutor();
        executor.responseBody =
                "{\"Status\":\"Success\",\"Label\":\"batch_1\","
                        + "\"NumberLoadedRows\":1,\"NumberFilteredRows\":1,"
                        + "\"Message\":\"one row filtered\","
                        + "\"ErrorURL\":\"http://fe/error\"}";
        StarRocksSinkWriter writer = writer(config, executor);
        writer.configureDirtyData(
                new DirtyDataContext(
                        "job-1",
                        "task-1",
                        "starrocks",
                        "orders",
                        null));

        writer.open();
        writer.write(
                RecordBatch.data(
                        Arrays.asList(
                                FluxRow.of(1L, "a"),
                                FluxRow.of(2L, "b"))),
                sourceTable(schema()));

        DirtyDataSummary summary = writer.getDirtyDataSummary();
        assertEquals(1L, summary.getDirtyCount());
        assertEquals(2L, summary.getAttemptedCount());
        assertEquals(Long.valueOf(1L), summary.getTaskCounts().get("task-1"));
        assertEquals(1, summary.getSampleCount());

        writer.close();
    }

    private static StarRocksSinkWriter writer(
            StarRocksSinkConfig config,
            RecordingExecutor executor) {
        return new StarRocksSinkWriter(
                config,
                new PreparedSinkMetadata(Collections.<TablePath, CatalogTable>emptyMap()),
                executor);
    }

    private static StarRocksSinkConfig config(int batchRows) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("node_urls", Arrays.asList("127.0.0.1:8030"));
        values.put("username", "root");
        values.put("database", "demo");
        values.put("table", "orders");
        values.put("batch_max_rows", batchRows);
        values.put("batch_max_bytes", 1024L * 1024L);
        return StarRocksSinkConfig.of(ReadonlyConfig.fromMap(values));
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

    private static final class RecordingExecutor
            implements StarRocksSinkWriter.StreamLoadExecutor {
        private final List<String> payloads = new ArrayList<String>();
        private String responseBody =
                "{\"Status\":\"Success\",\"Label\":\"batch_1\","
                        + "\"NumberLoadedRows\":2,\"NumberFilteredRows\":0}";
        private boolean closed;

        @Override
        public StarRocksStreamLoadResponse load(byte[] payload, TableSchema schema) throws Exception {
            payloads.add(new String(payload, StandardCharsets.UTF_8));
            return StarRocksStreamLoadResponse.parse(responseBody);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
