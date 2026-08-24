package com.link.up.connector.jdbc.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.config.SplitPlanningMode;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import com.link.up.connector.jdbc.core.split.JdbcSplitStatistics;
import com.link.up.connector.jdbc.core.split.JdbcSplitStatisticsProvider;
import com.link.up.connector.jdbc.core.split.JdbcSplitStatisticsRequest;
import com.link.up.connector.jdbc.utils.JdbcCatalogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Prepares bounded JDBC split enumeration from a prepared source schema.
 */
final class JdbcSourceSplitGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitGenerator.class);

    private JdbcSourceSplitGenerator() {
    }

    /**
     * Compatibility helper for legacy callers that still request the split
     * list directly.
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

    /**
     * Resolves the connector-specific table metadata/statistics needed by the
     * JDBC enumerator, then returns the canonical bounded split enumerator.
     */
    static SourceSplitEnumerator<JdbcSourceSplit> createEnumerator(
            JdbcSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int parallelism)
            throws Exception {

        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(tables, "tables must not be null");
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be greater than 0");
        }

        JdbcDialect dialect = JdbcDialectLoader.load(config.getConnectionConfig());
        Map<TablePath, JdbcSourceTable> sourceTables =
                reconcilePreparedTables(
                        JdbcCatalogUtils.getTables(config, dialect),
                        tables);

        Map<TablePath, JdbcSourceTable> plannedTables =
                new LinkedHashMap<TablePath, JdbcSourceTable>();
        JdbcSplitStatisticsProvider statisticsProvider =
                new JdbcSplitStatisticsProvider(
                        config.getConnectionConfig(),
                        dialect);

        for (JdbcSourceTable table : sourceTables.values()) {
            JdbcSourceTable planned = table;
            if (table.getPartitionColumn() != null
                    && config.getSplitPlanningMode() != SplitPlanningMode.MANUAL) {
                SplitPlanningMode mode = config.getSplitPlanningMode();
                try {
                    JdbcSplitStatistics statistics =
                            statisticsProvider.collect(
                                    new JdbcSplitStatisticsRequest(
                                            table,
                                            mode,
                                            config.getStatisticsQueryTimeout(),
                                            config.getSampleSize()));
                    if (statistics.isEmpty()) {
                        plannedTables.put(table.getTablePath(), null);
                        continue;
                    }
                    if (statistics.isAllNull()) {
                        if (!config.isNullPartitionSingleSplit()) {
                            throw new IllegalArgumentException(
                                    "Partition column is entirely NULL: "
                                            + table.getTablePath());
                        }
                    } else {
                        planned = JdbcSourceTable.builder()
                                .tablePath(table.getTablePath())
                                .query(table.getQuery())
                                .partitionColumn(table.getPartitionColumn())
                                .partitionNumber(table.getPartitionNumber())
                                .partitionStart(
                                        statistics.getLowerBound().orElse(null))
                                .partitionEnd(
                                        statistics.getUpperBound().orElse(null))
                                .catalogTable(table.getCatalogTable())
                                .build();
                    }
                } catch (UnsupportedOperationException e) {
                    if (mode != SplitPlanningMode.AUTO_SAMPLE
                            || !config.isAllowStatisticsFallback()) {
                        throw e;
                    }
                    LOG.warn(
                            "AUTO_SAMPLE statistics unsupported for table {}; falling back to AUTO_MIN_MAX",
                            table.getTablePath());
                    JdbcSplitStatistics statistics =
                            statisticsProvider.collect(
                                    new JdbcSplitStatisticsRequest(
                                            table,
                                            SplitPlanningMode.AUTO_MIN_MAX,
                                            config.getStatisticsQueryTimeout(),
                                            config.getSampleSize()));
                    if (statistics.isEmpty()) {
                        plannedTables.put(table.getTablePath(), null);
                        continue;
                    }
                    planned = JdbcSourceTable.builder()
                            .tablePath(table.getTablePath())
                            .query(table.getQuery())
                            .partitionColumn(table.getPartitionColumn())
                            .partitionNumber(table.getPartitionNumber())
                            .partitionStart(
                                    statistics.getLowerBound().orElse(null))
                            .partitionEnd(
                                    statistics.getUpperBound().orElse(null))
                            .catalogTable(table.getCatalogTable())
                            .build();
                } catch (Exception e) {
                    if (config.isMultiTable()
                            && config.getMultiTableFailurePolicy()
                            .continueOtherTables()) {
                        LOG.warn(
                                "Split statistics failed for table {}; continuing according to multi-table policy",
                                table.getTablePath());
                        continue;
                    }
                    throw e;
                }
            }
            plannedTables.put(planned.getTablePath(), planned);
        }

        plannedTables.values().removeIf(java.util.Objects::isNull);
        return new JdbcSourceSplitEnumerator(
                config,
                plannedTables,
                parallelism);
    }

    /**
     * Reconciles the connector's second metadata discovery with the metadata
     * already prepared by the framework.
     *
     * <p>A JDBC catalog may normalize an unqualified configured path such as
     * {@code sink_user_info} to {@code test1.sink_user_info}. The discovered
     * {@link JdbcSourceTable} historically retained the unqualified path while
     * its {@link CatalogTable} contained the normalized path. Using the former
     * as the data-set identity made split planning fail with
     * {@code No prepared table metadata} even though preparation and sink DDL
     * had succeeded.</p>
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

        for (JdbcSourceTable discoveredTable : discoveredTables.values()) {
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
                            .partitionColumn(discoveredTable.getPartitionColumn())
                            .partitionNumber(discoveredTable.getPartitionNumber())
                            .partitionStart(discoveredTable.getPartitionStart())
                            .partitionEnd(discoveredTable.getPartitionEnd())
                            .useSelectCount(discoveredTable.getUseSelectCount())
                            .skipAnalyze(discoveredTable.getSkipAnalyze())
                            .catalogTable(preparedCatalogTable)
                            .build();

            JdbcSourceTable previous =
                    result.put(normalizedPath, normalizedTable);

            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicated normalized source table path: "
                                + normalizedPath);
            }
        }

        return result;
    }
}
