package com.link.up.connector.starrocks.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.starrocks.client.source.StarRocksBeReadClient;
import com.link.up.connector.starrocks.config.StarRocksSourceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reads assigned StarRocks native splits through task-local BE scanner clients. */
public final class StarRocksSourceReader
        implements SourceReader<FluxRow, StarRocksSourceSplit> {

    private final StarRocksSourceConfig config;
    private final Map<TablePath, CatalogTable> tables;
    private final int batchSize;

    private List<StarRocksSourceSplit> splits = Collections.emptyList();
    private int splitIndex;
    private StarRocksSourceSplit currentSplit;
    private StarRocksBeReadClient currentClient;
    private boolean opened;
    private boolean finished;

    public StarRocksSourceReader(
            StarRocksSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.tables = Objects.requireNonNull(tables, "tables must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        this.batchSize = batchSize;
    }

    @Override
    public void open(List<StarRocksSourceSplit> splits) throws Exception {
        if (opened) {
            throw new IllegalStateException("StarRocksSourceReader has already been opened");
        }
        if (splits == null) {
            throw new IllegalArgumentException("splits must not be null");
        }
        this.splits =
                Collections.unmodifiableList(new ArrayList<StarRocksSourceSplit>(splits));
        splitIndex = 0;
        currentSplit = null;
        currentClient = null;
        finished = false;
        opened = true;
    }

    @Override
    public void open() throws Exception {
        open(Collections.<StarRocksSourceSplit>emptyList());
    }

    @Override
    public void openSplit(StarRocksSourceSplit split) throws Exception {
        checkOpened();
        if (currentSplit != null) {
            throw new IllegalStateException("A StarRocks split is already open");
        }
        // Dynamic split assignment reuses one reader for multiple splits. A
        // previous split may already have driven readBatch() to END_OF_INPUT.
        finished = false;
        openCurrentSplit(Objects.requireNonNull(split, "split must not be null"));
    }

    @Override
    public void closeSplit() throws Exception {
        closeCurrentSplit();
    }

    @Override
    public RecordBatch<FluxRow> readBatch() throws Exception {
        checkOpened();
        if (finished) {
            return RecordBatch.endOfInput();
        }

        while (true) {
            if (currentSplit == null) {
                if (!openNextSplit()) {
                    finished = true;
                    return RecordBatch.endOfInput();
                }
            }

            StarRocksSourceSplit batchSplit = currentSplit;
            List<FluxRow> rows = new ArrayList<FluxRow>(batchSize);
            boolean exhausted = false;

            while (rows.size() < batchSize) {
                if (!currentClient.hasNext()) {
                    exhausted = true;
                    break;
                }
                rows.add(currentClient.next());
            }

            if (exhausted) {
                closeCurrentSplit();
            }

            if (!rows.isEmpty()) {
                return RecordBatch.of(batchSplit, rows);
            }
        }
    }

    private boolean openNextSplit() throws Exception {
        if (splitIndex >= splits.size()) {
            return false;
        }
        openCurrentSplit(splits.get(splitIndex++));
        return true;
    }

    private void openCurrentSplit(StarRocksSourceSplit split) throws Exception {
        CatalogTable table = tables.get(split.getTablePath());
        if (table == null) {
            throw new IllegalArgumentException(
                    "Cannot find schema for StarRocks split table: " + split.getTablePath());
        }

        StarRocksBeReadClient client =
                new StarRocksBeReadClient(
                        config,
                        split.getPartition(),
                        table.getRowType());
        try {
            client.open();
        } catch (Exception failure) {
            try {
                client.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }

        currentSplit = split;
        currentClient = client;
    }

    private void closeCurrentSplit() throws Exception {
        if (currentClient == null) {
            currentSplit = null;
            return;
        }
        try {
            currentClient.close();
        } finally {
            currentClient = null;
            currentSplit = null;
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("StarRocksSourceReader has not been opened");
        }
    }

    @Override
    public void close() throws Exception {
        if (!opened) {
            return;
        }
        try {
            closeCurrentSplit();
        } finally {
            opened = false;
            finished = true;
            splits = Collections.emptyList();
        }
    }
}
