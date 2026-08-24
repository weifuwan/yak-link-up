package com.link.up.connector.jdbc.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.core.split.JdbcSplitPlanner;

import java.util.List;
import java.util.Map;

/**
 * JDBC bounded split enumerator.
 *
 * <p>Connector-specific metadata/statistics are prepared before construction;
 * this role owns the final deterministic split calculation for one planning
 * attempt. It does not communicate with SourceReaders and does not own runtime
 * task/checkpoint state.</p>
 */
public final class JdbcSourceSplitEnumerator
        implements SourceSplitEnumerator<JdbcSourceSplit> {

    private final JdbcSourceConfig config;
    private final Map<TablePath, JdbcSourceTable> sourceTables;
    private final int parallelism;

    public JdbcSourceSplitEnumerator(
            JdbcSourceConfig config,
            Map<TablePath, JdbcSourceTable> sourceTables,
            int parallelism) {

        this.config = config;
        this.sourceTables = sourceTables;
        this.parallelism = parallelism;
    }

    @Override
    public List<JdbcSourceSplit> enumerateSplits()
            throws Exception {

        return JdbcSplitPlanner.plan(
                config,
                sourceTables,
                parallelism);
    }
}
