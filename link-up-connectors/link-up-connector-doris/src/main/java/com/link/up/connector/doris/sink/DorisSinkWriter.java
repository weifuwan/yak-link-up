package com.link.up.connector.doris.sink;

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
import com.link.up.connector.doris.client.DorisStreamLoadClient;
import com.link.up.connector.doris.client.DorisStreamLoadClient.StreamLoadResponse;
import com.link.up.connector.doris.config.DorisSinkConfig;
import com.link.up.connector.doris.converter.DorisRowSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Writes FluxRow batches through Doris Stream Load.
 *
 * <p>The writer owns buffering, serialization and dirty-data reporting.
 * Task-local 2PC transaction state belongs to
 * {@link DorisTwoPhaseCommitController}.</p>
 */
public final class DorisSinkWriter
        implements SinkWriter<FluxRow>, DirtyDataAwareSinkWriter {

    private static final Logger LOG =
            LoggerFactory.getLogger(DorisSinkWriter.class);

    private final DorisSinkConfig config;
    private final DorisStreamLoadClient client;
    private final DorisTwoPhaseCommitController transactions;
    private final List<FluxRow> buffer =
            new ArrayList<FluxRow>();

    private TableSchema schema;
    private DorisRowSerializer serializer;
    private long totalWrittenRows;
    private long totalLoadRequests;
    private long totalFilteredRows;

    private DirtyDataCollector dirtyDataCollector;
    private DirtyDataContext dirtyDataContext;

    public DorisSinkWriter(
            DorisSinkConfig config,
            PreparedSinkMetadata metadata) {

        this.config = Objects.requireNonNull(
                config,
                "config must not be null");
        Objects.requireNonNull(
                metadata,
                "metadata must not be null");

        this.client = new DorisStreamLoadClient(config);
        this.transactions =
                new DorisTwoPhaseCommitController(
                        client,
                        config.isEnable2pc());
    }

    @Override
    public void open() throws Exception {
        LOG.info(
                "Doris SinkWriter opened: database={}, table={}, batchSize={}, format={}, 2pc={}",
                config.getDatabase(),
                config.getTable(),
                config.getBatchSize(),
                config.getLoadFormat(),
                transactions.isEnabled());

        if (dirtyDataCollector != null) {
            dirtyDataCollector.open();
        }
    }

    @Override
    public void write(
            RecordBatch<FluxRow> batch,
            CatalogTable sourceTable)
            throws Exception {

        if (batch == null
                || batch.isEndOfInput()
                || batch.getRecords().isEmpty()) {
            return;
        }

        initializeSchema(sourceTable);

        for (FluxRow row : batch.getRecords()) {
            buffer.add(row);

            if (buffer.size() >= config.getBatchSize()) {
                flush();
            }
        }
    }

    @Override
    public void prepareCommit() throws Exception {
        flush();
    }

    @Override
    public void commit() throws Exception {
        transactions.commit();

        LOG.info(
                "Doris SinkWriter commit: totalWrittenRows={}, totalLoadRequests={}, 2pc={}",
                totalWrittenRows,
                totalLoadRequests,
                transactions.isEnabled());
    }

    @Override
    public void abort() throws Exception {
        buffer.clear();
        transactions.abort();

        LOG.warn(
                "Doris SinkWriter aborted: totalWrittenRows={}",
                totalWrittenRows);
    }

    @Override
    public CommitScope getCommitScope() {
        return CommitScope.TASK_LOCAL;
    }

    @Override
    public String getRetryAdvice() {
        return transactions.retryAdvice();
    }

    @Override
    public void close() throws Exception {
        try {
            closePendingWork();
        } finally {
            closeResources();
        }

        LOG.info(
                "Doris SinkWriter closed: totalWrittenRows={}, totalLoadRequests={}, "
                        + "totalFilteredRows={}, 2pc={}",
                totalWrittenRows,
                totalLoadRequests,
                totalFilteredRows,
                transactions.isEnabled());
    }

    @Override
    public void configureDirtyData(
            DirtyDataContext context)
            throws Exception {

        dirtyDataContext = Objects.requireNonNull(
                context,
                "context must not be null");

        dirtyDataCollector =
                new BoundedMemoryDirtyDataCollector(
                        context.getTaskId(),
                        100,
                        1000,
                        0.1);
    }

    @Override
    public DirtyDataSummary getDirtyDataSummary() {
        return dirtyDataCollector == null
                ? DirtyDataSummary.empty()
                : dirtyDataCollector.summary();
    }

    private void initializeSchema(
            CatalogTable sourceTable) {

        if (schema != null
                || sourceTable == null
                || sourceTable.getTableSchema() == null) {
            return;
        }

        schema = sourceTable.getTableSchema();
    }

    private void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }

        ensureSerializer();

        String data = serializer.serialize(buffer);

        LOG.debug(
                "Flushing {} rows via Doris Stream Load: payloadBytes={}",
                buffer.size(),
                data.length());

        StreamLoadResponse response =
                client.load(data);

        response.checkSuccess();

        totalWrittenRows += buffer.size();
        totalLoadRequests++;

        LOG.debug(
                "Doris Stream Load succeeded: label={}, loadedRows={}, txnId={}, txnState={}",
                response.getLabel(),
                response.getNumberLoadedRows(),
                response.getTxnId(),
                response.getTxnState());

        recordFilteredRows(response);
        transactions.record(response);
        buffer.clear();
    }

    private void ensureSerializer() {
        if (schema == null) {
            throw new IllegalStateException(
                    "Cannot flush: table schema is not initialized. "
                            + "Ensure at least one write() call provides "
                            + "a CatalogTable with schema.");
        }

        if (serializer == null) {
            serializer =
                    new DorisRowSerializer(
                            config,
                            schema);
        }
    }

    private void recordFilteredRows(
            StreamLoadResponse response) {

        if (response.getNumberFilteredRows() <= 0) {
            return;
        }

        totalFilteredRows +=
                response.getNumberFilteredRows();

        LOG.warn(
                "Doris Stream Load filtered {} rows: {}",
                response.getNumberFilteredRows(),
                response.getMessage());

        if (dirtyDataCollector == null) {
            return;
        }

        try {
            dirtyDataCollector.recordAttempt(
                    buffer.size());

            String message =
                    "Doris Stream Load filtered "
                            + response.getNumberFilteredRows()
                            + " rows: "
                            + response.getMessage();

            if (response.getBody() != null
                    && response.getBody().contains("ErrorURL")) {
                message += ", check ErrorURL for details";
            }

            dirtyDataCollector.collect(
                    new DirtyRecord(
                            "STREAM_LOAD_FILTERED",
                            message,
                            dirtyDataContext,
                            System.currentTimeMillis()));

        } catch (Exception failure) {
            LOG.error(
                    "Failed to record Doris dirty-data evidence",
                    failure);
        }
    }

    private void closePendingWork()
            throws Exception {

        if (transactions.isEnabled()) {
            transactions.close();
            return;
        }

        if (!buffer.isEmpty()) {
            flush();
        }
    }

    private void closeResources()
            throws Exception {

        try {
            client.close();
        } finally {
            closeDirtyDataCollector();
        }
    }

    private void closeDirtyDataCollector() {
        if (dirtyDataCollector == null) {
            return;
        }

        try {
            dirtyDataCollector.close(true);
        } catch (Exception failure) {
            LOG.warn(
                    "Failed to close dirty data collector",
                    failure);
        }
    }
}
