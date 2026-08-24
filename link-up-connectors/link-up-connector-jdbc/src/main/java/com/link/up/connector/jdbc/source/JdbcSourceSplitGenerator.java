package com.link.up.connector.jdbc.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogUtils;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Prepares bounded JDBC split enumeration from prepared source metadata. */
final class JdbcSourceSplitGenerator {

    private JdbcSourceSplitGenerator() {
    }

    /**
     * Compatibility helper for callers that still request the split list
     * directly.
     */
    static List<JdbcSourceSplit> generate(
            JdbcSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int parallelism)
            throws Exception {

        try (SourceSplitEnumerator<JdbcSourceSplit> enumerator =
                     createEnumerator(
                             config,
                             tables,
                             parallelism)) {

            return enumerator.enumerateSplits();
        }
    }

    static SourceSplitEnumerator<JdbcSourceSplit> createEnumerator(
            JdbcSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int parallelism)
            throws Exception {

        Objects.requireNonNull(
                config,
                "config must not be null");
        Objects.requireNonNull(
                tables,
                "tables must not be null");

        if (parallelism <= 0) {
            throw new IllegalArgumentException(
                    "parallelism must be greater than 0");
        }

        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        config.getConnectionConfig());

        Map<TablePath, JdbcSourceTable> sourceTables =
                reconcilePreparedTables(
                        JdbcCatalogUtils.getTables(
                                config,
                                dialect),
                        tables);

        Map<TablePath, JdbcSourceTable> plannedTables =
                new JdbcSplitPlanningService(
                        config,
                        dialect)
                        .plan(sourceTables);

        return new JdbcSourceSplitEnumerator(
                config,
                plannedTables,
                parallelism);
    }

    /**
     * Reconciles connector metadata discovery with the table metadata already
     * prepared by the framework.
     */
    static Map<TablePath, JdbcSourceTable> reconcilePreparedTables(
            Map<TablePath, JdbcSourceTable> discoveredTables,
            Map<TablePath, CatalogTable> preparedTables) {

        Objects.requireNonNull(
                discoveredTables,
                "discoveredTables must not be null");
        Objects.requireNonNull(
                preparedTables,
                "preparedTables must not be null");

        Map<TablePath, JdbcSourceTable> result =
                new LinkedHashMap<TablePath, JdbcSourceTable>();

        for (JdbcSourceTable discoveredTable :
                discoveredTables.values()) {

            if (discoveredTable == null) {
                throw new IllegalArgumentException(
                        "discoveredTables must not contain null values");
            }

            CatalogTable discoveredCatalogTable =
                    Objects.requireNonNull(
                            discoveredTable.getCatalogTable(),
                            "discovered CatalogTable must not be null");

            TablePath normalizedPath =
                    Objects.requireNonNull(
                            discoveredCatalogTable.getTablePath(),
                            "discovered CatalogTable path must not be null");

            CatalogTable preparedCatalogTable =
                    preparedTables.get(normalizedPath);

            if (preparedCatalogTable == null) {
                throw new IllegalArgumentException(
                        "No prepared table metadata for "
                                + normalizedPath
                                + ", preparedTables="
                                + preparedTables.keySet());
            }

            JdbcSourceTable normalizedTable =
                    JdbcSourceTable.builder()
                            .tablePath(normalizedPath)
                            .query(discoveredTable.getQuery())
                            .partitionColumn(
                                    discoveredTable.getPartitionColumn())
                            .partitionNumber(
                                    discoveredTable.getPartitionNumber())
                            .partitionStart(
                                    discoveredTable.getPartitionStart())
                            .partitionEnd(
                                    discoveredTable.getPartitionEnd())
                            .useSelectCount(
                                    discoveredTable.getUseSelectCount())
                            .skipAnalyze(
                                    discoveredTable.getSkipAnalyze())
                            .catalogTable(
                                    preparedCatalogTable)
                            .build();

            JdbcSourceTable previous =
                    result.put(
                            normalizedPath,
                            normalizedTable);

            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicated normalized source table path: "
                                + normalizedPath);
            }
        }

        return result;
    }
}
