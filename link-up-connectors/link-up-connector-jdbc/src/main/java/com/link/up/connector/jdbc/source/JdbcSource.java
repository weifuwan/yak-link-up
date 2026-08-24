package com.link.up.connector.jdbc.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * JDBC 离线 Source。
 * <p>
 * Source 本身只保存不可变配置，不直接持有 JDBC 连接。Split discovery
 * is delegated to a per-planning {@link SourceSplitEnumerator}.
 */
public final class JdbcSource
        implements Source<JdbcSourceSplit> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(JdbcSource.class);

    private final JdbcSourceConfig config;

    public JdbcSource(JdbcSourceConfig config) {
        this.config = Objects.requireNonNull(
                config,
                "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context)
            throws Exception {

        Objects.requireNonNull(
                context,
                "context must not be null");

        return JdbcSourceSplitGenerator.createEnumerator(
                config,
                tables,
                context.getParallelism());
    }

    /**
     * Compatibility bridge for callers that still invoke the legacy Source API.
     */
    @Override
    @Deprecated
    public List<JdbcSourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables)
            throws Exception {

        return createSplits(tables, 1);
    }

    @Override
    public void validateParallelism(int parallelism) {
        Source.super.validateParallelism(parallelism);

        ReadConsistency consistency = config.getReadConsistency();
        if (consistency == ReadConsistency.BEST_EFFORT) {
            if (parallelism > 1) {
                LOG.warn("JDBC read_consistency=BEST_EFFORT with source parallelism {} does not guarantee a single snapshot across readers", parallelism);
            }
            return;
        }

        if (consistency == ReadConsistency.SINGLE_CONNECTION_SNAPSHOT
                && parallelism != 1) {
            throw new IllegalArgumentException(
                    "read_consistency=SINGLE_CONNECTION_SNAPSHOT requires source parallelism=1");
        }

        JdbcDialect dialect = JdbcDialectLoader.load(config.getConnectionConfig());
        if (!dialect.supportedReadConsistencies().contains(consistency)) {
            throw new IllegalArgumentException(
                    "JDBC dialect '" + dialect.name()
                            + "' does not support read_consistency=" + consistency);
        }
    }

    /**
     * Compatibility bridge for callers that still invoke the legacy Source API.
     */
    @Override
    @Deprecated
    public List<JdbcSourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables,
            int parallelism)
            throws Exception {

        try (SourceSplitEnumerator<JdbcSourceSplit> enumerator =
                     createEnumerator(
                             tables,
                             new SourceEnumeratorContext(parallelism))) {
            return enumerator.enumerateSplits();
        }
    }

    @Override
    public SourceReader<FluxRow, JdbcSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        return new JdbcSourceReader(
                config,
                tables,
                batchSize);
    }
}
