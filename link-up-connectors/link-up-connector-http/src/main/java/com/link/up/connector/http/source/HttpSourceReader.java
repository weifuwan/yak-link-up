package com.link.up.connector.http.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.http.client.HttpSourceClient;
import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.converter.HttpResponseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reads one bounded HTTP source.
 *
 * <p>The reader owns SourceReader lifecycle and row buffering only. Pagination
 * state belongs to {@link HttpPaginationState}; transport belongs to
 * {@link HttpSourceClient}; response conversion belongs to
 * {@link HttpResponseParser}.</p>
 */
public final class HttpSourceReader
        implements SourceReader<FluxRow, HttpSourceSplit> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpSourceReader.class);

    private final HttpSourceConfig config;
    private final CatalogTable catalogTable;
    private final int batchSize;
    private final HttpSourceClient client;

    private HttpSourceSplit currentSplit;
    private HttpPaginationState pagination;
    private List<FluxRow> buffer =
            Collections.emptyList();
    private int bufferIndex;
    private boolean opened;
    private boolean finished;

    public HttpSourceReader(
            HttpSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        this.config = Objects.requireNonNull(
                config,
                "config must not be null");
        this.catalogTable = firstTable(tables);

        if (batchSize <= 0) {
            throw new IllegalArgumentException(
                    "batchSize must be greater than 0");
        }

        this.batchSize = batchSize;
        this.client = new HttpSourceClient(config);
    }

    @Override
    public void open(List<HttpSourceSplit> splits)
            throws Exception {

        if (opened) {
            throw new IllegalStateException(
                    "HttpSourceReader has already been opened");
        }

        Objects.requireNonNull(
                splits,
                "splits must not be null");

        currentSplit = splits.isEmpty()
                ? new HttpSourceSplit()
                : Objects.requireNonNull(
                        splits.get(0),
                        "splits must not contain null values");

        pagination = new HttpPaginationState(config);
        buffer = Collections.emptyList();
        bufferIndex = 0;
        finished = false;
        opened = true;

        LOG.info(
                "HTTP Source Reader opened: url={}, method={}, format={}, pagination={}",
                config.getUrl(),
                config.getMethod(),
                config.getFormat(),
                config.hasPagination()
                        ? config.getPageType()
                        : "none");
    }

    @Override
    public RecordBatch<FluxRow> readBatch()
            throws Exception {

        checkOpened();

        if (finished) {
            return RecordBatch.endOfInput();
        }

        while (true) {
            RecordBatch<FluxRow> buffered =
                    nextBufferedBatch();

            if (buffered != null) {
                return buffered;
            }

            if (pagination.isExhausted()) {
                finished = true;
                return RecordBatch.endOfInput();
            }

            fetchNextPage();
        }
    }

    @Override
    public void close() throws Exception {
        if (!opened) {
            return;
        }

        try {
            client.close();
        } finally {
            opened = false;
        }

        LOG.info("HTTP Source Reader closed");
    }

    private RecordBatch<FluxRow> nextBufferedBatch() {
        if (bufferIndex >= buffer.size()) {
            return null;
        }

        int end =
                Math.min(
                        bufferIndex + batchSize,
                        buffer.size());

        List<FluxRow> rows =
                new ArrayList<FluxRow>(
                        buffer.subList(
                                bufferIndex,
                                end));

        bufferIndex = end;
        return RecordBatch.of(
                currentSplit,
                rows);
    }

    private void fetchNextPage()
            throws Exception {

        HttpPageRequest request =
                pagination.currentRequest();

        LOG.debug(
                "HTTP request page={}, cursor={}",
                pagination.getCurrentPage(),
                pagination.getCurrentCursor());

        String responseBody =
                client.execute(
                        request.getHeaders(),
                        request.getParams(),
                        request.getBody());

        List<FluxRow> rows =
                HttpResponseParser.parseResponse(
                        responseBody,
                        config,
                        catalogTable.getTableSchema());

        buffer = rows;
        bufferIndex = 0;
        pagination.advance(
                responseBody,
                rows.size());
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException(
                    "HttpSourceReader has not been opened");
        }
    }

    private static CatalogTable firstTable(
            Map<TablePath, CatalogTable> tables) {

        Objects.requireNonNull(
                tables,
                "tables must not be null");

        if (tables.isEmpty()) {
            throw new IllegalArgumentException(
                    "tables must not be empty");
        }

        CatalogTable table =
                tables.values()
                        .iterator()
                        .next();

        return Objects.requireNonNull(
                table,
                "tables must not contain null values");
    }
}
