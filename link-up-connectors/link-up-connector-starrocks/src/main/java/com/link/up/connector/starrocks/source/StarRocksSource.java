package com.link.up.connector.starrocks.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.starrocks.config.StarRocksSourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** StarRocks bounded source using FE query plans and direct BE native scans. */
public final class StarRocksSource implements Source<StarRocksSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final StarRocksSourceConfig config;

    public StarRocksSource(StarRocksSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<StarRocksSourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {
        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return new StarRocksSourceSplitEnumerator(config);
    }

    @Override
    @Deprecated
    public List<StarRocksSourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables) throws Exception {
        try (StarRocksSourceSplitEnumerator enumerator =
                     new StarRocksSourceSplitEnumerator(config)) {
            return enumerator.enumerateSplits();
        }
    }

    @Override
    public SourceReader<FluxRow, StarRocksSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new StarRocksSourceReader(config, tables, batchSize);
    }

    public StarRocksSourceConfig getConfig() {
        return config;
    }
}
