package com.link.up.connector.starrocks.sink;

import com.link.up.api.dirtydata.BoundedMemoryDirtyDataCollector;
import com.link.up.api.dirtydata.DirtyDataCollector;
import com.link.up.api.dirtydata.DirtyDataContext;
import com.link.up.api.dirtydata.DirtyDataSummary;
import com.link.up.api.dirtydata.DirtyRecord;
import com.link.up.api.sink.CommitScope;
import com.link.up.api.sink.DirtyDataAwareSinkWriter;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.starrocks.client.sink.StarRocksStreamLoadClient;
import com.link.up.connector.starrocks.client.sink.StarRocksStreamLoadResponse;
import com.link.up.connector.starrocks.config.StarRocksSinkConfig;
import com.link.up.connector.starrocks.converter.StarRocksRowSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Writes bounded FluxRow batches through StarRocks Stream Load. */
public final class StarRocksSinkWriter
        implements SinkWriter<FluxRow>, DirtyDataAwareSinkWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkWriter.class);

    private final StarRocksSinkConfig config;
    private final StreamLoadExecutor streamLoadExecutor;
    private final List<byte[]> bufferedRecords = new ArrayList<byte[]>();

    private TableSchema schema;
    private StarRocksRowSerializer serializer;
    private long bufferedRecordBytes;
    private long totalWrittenRows;
    private long totalLoadRequests;
    private long totalFilteredRows;
    private boolean opened;

    private DirtyDataCollector dirtyDataCollector;
    private DirtyDataContext dirtyDataContext;

    public StarRocksSinkWriter(
            StarRocksSinkConfig config,
            PreparedSinkMetadata metadata) {
        this(
                config,
                metadata,
                new HttpStreamLoadExecutor(
                        new StarRocksStreamLoadClient(
                                Objects.requireNonNull(config, "config must not be null"))));
    }

    StarRocksSinkWriter(
            StarRocksSinkConfig config,
            PreparedSinkMetadata metadata,
            StreamLoadExecutor streamLoadExecutor) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        this.streamLoadExecutor = Objects.requireNonNull(
                streamLoadExecutor,
                "streamLoadExecutor must not be null");
    }

    @Override
    public void open() throws Exception {
        opened = true;
        if (dirtyDataCollector != null) {
            dirtyDataCollector.open();
        }
        LOG.info(
                "StarRocks SinkWriter opened: database={}, table={}, format={}, maxRows={}, maxBytes={}",
                config.getDatabase(),
                config.getTable(),
                config.getLoadFormat(),
                config.getBatchMaxRows(),
                config.getBatchMaxBytes());
    }

    @Override
    public void write(
            RecordBatch<FluxRow> batch,
            CatalogTable sourceTable)
            throws Exception {
        checkOpened();
        if (batch == null || batch.isEndOfInput() || batch.getRecords().isEmpty()) {
            return;
        }

        initializeSchema(sourceTable);

        for (FluxRow row : batch.getRecords()) {
            byte[] record = serializer.serializeRow(row);
            long singlePayloadBytes = serializer.payloadSizeBytes(1, record.length);
            if (singlePayloadBytes > config.getBatchMaxBytes()) {
                throw new IllegalArgumentException(
                        "One serialized StarRocks row exceeds batch_max_bytes: payloadBytes="
                                + singlePayloadBytes
                                + ", limit="
                                + config.getBatchMaxBytes());
            }

            long nextPayloadBytes =
                    serializer.payloadSizeBytes(
                            bufferedRecords.size() + 1,
                            bufferedRecordBytes + record.length);

            if (!bufferedRecords.isEmpty()
                    && (bufferedRecords.size() >= config.getBatchMaxRows()
                    || nextPayloadBytes > config.getBatchMaxBytes())) {
                flush();
            }

            bufferedRecords.add(record);
            bufferedRecordBytes += record.length;

            if (bufferedRecords.size() >= config.getBatchMaxRows()
                    || serializer.payloadSizeBytes(
                                    bufferedRecords.size(),
                                    bufferedRecordBytes)
                            >= config.getBatchMaxBytes()) {
                flush();
            }
        }
    }

    @Override
    public void prepareCommit() throws Exception {
        checkOpened();
        flush();
    }

    @Override
    public void commit() {
        checkOpened();
        LOG.info(
                "StarRocks SinkWriter commit boundary reached: totalWrittenRows={}, totalLoadRequests={}",
                totalWrittenRows,
                totalLoadRequests);
    }

    @Override
    public void abort() {
        bufferedRecords.clear();
        bufferedRecordBytes = 0L;
        LOG.warn(
                "StarRocks SinkWriter aborted unsent buffer; already successful Stream Load batches cannot be rolled back: writtenRows={}",
                totalWrittenRows);
    }

    @Override
    public CommitScope getCommitScope() {
        return CommitScope.TASK_LOCAL;
    }

    @Override
    public String getRetryAdvice() {
        return "StarRocks Stream Load commits each successful flush independently. "
                + "The connector reuses labels inside one flush retry, but a whole-task retry creates new labels; "
                + "verify already loaded target data or rely on target-table key semantics before retrying.";
    }

    @Override
    public void configureDirtyData(DirtyDataContext context) throws Exception {
        dirtyDataContext = Objects.requireNonNull(context, "context must not be null");
        dirtyDataCollector =
                new BoundedMemoryDirtyDataCollector(
                        context.getTaskId(),
                        100,
                        1000,
                        0.1);
        if (opened) {
            dirtyDataCollector.open();
        }
    }

    @Override
    public DirtyDataSummary getDirtyDataSummary() {
        return dirtyDataCollector == null
                ? DirtyDataSummary.empty()
                : dirtyDataCollector.summary();
    }

    @Override
    public void close() throws Exception {
        try {
            bufferedRecords.clear();
            bufferedRecordBytes = 0L;
            streamLoadExecutor.close();
        } finally {
            opened = false;
            closeDirtyDataCollector();
        }
        LOG.info(
                "StarRocks SinkWriter closed: totalWrittenRows={}, totalLoadRequests={}, totalFilteredRows={}",
                totalWrittenRows,
                totalLoadRequests,
                totalFilteredRows);
    }

    private void initializeSchema(CatalogTable sourceTable) {
        if (sourceTable == null || sourceTable.getTableSchema() == null) {
            throw new IllegalArgumentException(
                    "StarRocks Stream Load Sink requires CatalogTable schema on write");
        }

        TableSchema incoming = sourceTable.getTableSchema();
        if (schema == null) {
            schema = incoming;
            serializer = new StarRocksRowSerializer(config, schema);
            return;
        }

        if (!schema.equals(incoming)) {
            throw new IllegalArgumentException(
                    "StarRocks Stream Load Sink does not support runtime schema changes in Stage 2");
        }
    }

    private void flush() throws Exception {
        if (bufferedRecords.isEmpty()) {
            return;
        }
        if (serializer == null || schema == null) {
            throw new IllegalStateException(
                    "Cannot flush StarRocks Sink before source schema is initialized");
        }

        int flushRows = bufferedRecords.size();
        byte[] payload = serializer.joinRecords(bufferedRecords, bufferedRecordBytes);
        LOG.debug(
                "Flushing StarRocks Stream Load batch: rows={}, payloadBytes={}",
                flushRows,
                payload.length);

        StarRocksStreamLoadResponse response = streamLoadExecutor.load(payload, schema);
        if (!response.isSuccess()) {
            throw new IllegalStateException(
                    "StarRocks Stream Load client returned non-success response: "
                            + response.getStatus());
        }

        totalWrittenRows += flushRows;
        totalLoadRequests++;
        totalFilteredRows += response.getNumberFilteredRows();
        recordFilteredRows(response, flushRows);

        bufferedRecords.clear();
        bufferedRecordBytes = 0L;
    }

    private void recordFilteredRows(
            StarRocksStreamLoadResponse response,
            int flushRows) {
        if (response.getNumberFilteredRows() <= 0L) {
            return;
        }

        LOG.warn(
                "StarRocks Stream Load filtered {} rows: label={}, message={}, errorUrl={}",
                response.getNumberFilteredRows(),
                response.getLabel(),
                response.getMessage(),
                response.getErrorUrl());

        if (dirtyDataCollector == null) {
            return;
        }

        try {
            dirtyDataCollector.recordAttempt(flushRows);
            String message =
                    "StarRocks Stream Load filtered "
                            + response.getNumberFilteredRows()
                            + " rows: "
                            + response.getMessage();
            if (response.getErrorUrl() != null && !response.getErrorUrl().trim().isEmpty()) {
                message += ", ErrorURL=" + response.getErrorUrl();
            }
            dirtyDataCollector.collect(
                    new DirtyRecord(
                            "STREAM_LOAD_FILTERED",
                            message,
                            dirtyDataContext,
                            System.currentTimeMillis()));
        } catch (Exception failure) {
            LOG.error("Failed to record StarRocks dirty-data evidence", failure);
        }
    }

    private void closeDirtyDataCollector() {
        if (dirtyDataCollector == null) {
            return;
        }
        try {
            dirtyDataCollector.close(true);
        } catch (Exception failure) {
            LOG.warn("Failed to close StarRocks dirty data collector", failure);
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("StarRocks SinkWriter has not been opened");
        }
    }

    interface StreamLoadExecutor extends AutoCloseable {
        StarRocksStreamLoadResponse load(byte[] payload, TableSchema schema) throws Exception;

        @Override
        void close() throws Exception;
    }

    private static final class HttpStreamLoadExecutor implements StreamLoadExecutor {
        private final StarRocksStreamLoadClient client;

        private HttpStreamLoadExecutor(StarRocksStreamLoadClient client) {
            this.client = client;
        }

        @Override
        public StarRocksStreamLoadResponse load(byte[] payload, TableSchema schema) throws Exception {
            return client.load(payload, schema);
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
