package com.link.up.connector.http.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.http.config.HttpSourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HTTP bounded Source.
 *
 * <p>The source exposes one logical split; pagination remains reader-owned.
 * Split discovery uses the canonical enumerator contract introduced in Phase 4.</p>
 */
public final class HttpSource implements Source<HttpSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final HttpSourceConfig config;

    public HttpSource(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<HttpSourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {

        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return new HttpSourceSplitEnumerator();
    }

    /**
     * Compatibility bridge for callers that still invoke the legacy Source API.
     */
    @Override
    @Deprecated
    public List<HttpSourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables) {
        return new HttpSourceSplitEnumerator().enumerateSplits();
    }

    @Override
    public SourceReader<FluxRow, HttpSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new HttpSourceReader(config, tables, batchSize);
    }

    public HttpSourceConfig getConfig() {
        return config;
    }
}
