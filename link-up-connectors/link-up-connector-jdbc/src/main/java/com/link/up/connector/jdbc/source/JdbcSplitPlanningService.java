package com.link.up.connector.jdbc.source;

import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.config.SplitPlanningMode;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.split.JdbcSplitStatistics;
import com.link.up.connector.jdbc.core.split.JdbcSplitStatisticsProvider;
import com.link.up.connector.jdbc.core.split.JdbcSplitStatisticsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Resolves bounded JDBC split statistics before enumeration. */
final class JdbcSplitPlanningService {

    private static final Logger LOG =
            LoggerFactory.getLogger(
                    JdbcSplitPlanningService.class);

    private final JdbcSourceConfig config;
    private final JdbcSplitStatisticsProvider statisticsProvider;

    JdbcSplitPlanningService(
            JdbcSourceConfig config,
            JdbcDialect dialect) {

        this.config = Objects.requireNonNull(
                config,
                "config must not be null");

        this.statisticsProvider =
                new JdbcSplitStatisticsProvider(
                        config.getConnectionConfig(),
                        Objects.requireNonNull(
                                dialect,
                                "dialect must not be null"));
    }

    Map<TablePath, JdbcSourceTable> plan(
            Map<TablePath, JdbcSourceTable> sourceTables)
            throws Exception {

        Objects.requireNonNull(
                sourceTables,
                "sourceTables must not be null");

        Map<TablePath, JdbcSourceTable> planned =
                new LinkedHashMap<TablePath, JdbcSourceTable>();

        for (JdbcSourceTable table : sourceTables.values()) {
            JdbcSourceTable plannedTable =
                    planTable(
                            Objects.requireNonNull(
                                    table,
                                    "sourceTables must not contain null values"));

            if (plannedTable != null) {
                planned.put(
                        plannedTable.getTablePath(),
                        plannedTable);
            }
        }

        return planned;
    }

    private JdbcSourceTable planTable(
            JdbcSourceTable table)
            throws Exception {

        if (table.getPartitionColumn() == null
                || config.getSplitPlanningMode()
                == SplitPlanningMode.MANUAL) {
            return table;
        }

        SplitPlanningMode mode =
                config.getSplitPlanningMode();

        try {
            JdbcSplitStatistics statistics =
                    collect(table, mode);

            return applyStatistics(
                    table,
                    statistics,
                    true);

        } catch (UnsupportedOperationException unsupported) {
            if (mode != SplitPlanningMode.AUTO_SAMPLE
                    || !config.isAllowStatisticsFallback()) {
                throw unsupported;
            }

            LOG.warn(
                    "AUTO_SAMPLE statistics unsupported for table {}; "
                            + "falling back to AUTO_MIN_MAX",
                    table.getTablePath());

            JdbcSplitStatistics statistics =
                    collect(
                            table,
                            SplitPlanningMode.AUTO_MIN_MAX);

            return applyStatistics(
                    table,
                    statistics,
                    false);

        } catch (Exception failure) {
            if (config.isMultiTable()
                    && config.getMultiTableFailurePolicy()
                    .continueOtherTables()) {

                LOG.warn(
                        "Split statistics failed for table {}; "
                                + "continuing according to multi-table policy",
                        table.getTablePath());
                return null;
            }

            throw failure;
        }
    }

    private JdbcSplitStatistics collect(
            JdbcSourceTable table,
            SplitPlanningMode mode)
            throws Exception {

        return statisticsProvider.collect(
                new JdbcSplitStatisticsRequest(
                        table,
                        mode,
                        config.getStatisticsQueryTimeout(),
                        config.getSampleSize()));
    }

    private JdbcSourceTable applyStatistics(
            JdbcSourceTable table,
            JdbcSplitStatistics statistics,
            boolean enforceAllNullPolicy) {

        if (statistics.isEmpty()) {
            return null;
        }

        if (statistics.isAllNull()) {
            if (enforceAllNullPolicy
                    && !config.isNullPartitionSingleSplit()) {
                throw new IllegalArgumentException(
                        "Partition column is entirely NULL: "
                                + table.getTablePath());
            }

            if (enforceAllNullPolicy) {
                return table;
            }
        }

        return withBounds(
                table,
                statistics);
    }

    private JdbcSourceTable withBounds(
            JdbcSourceTable table,
            JdbcSplitStatistics statistics) {

        return JdbcSourceTable.builder()
                .tablePath(table.getTablePath())
                .query(table.getQuery())
                .partitionColumn(
                        table.getPartitionColumn())
                .partitionNumber(
                        table.getPartitionNumber())
                .partitionStart(
                        statistics.getLowerBound()
                                .orElse(null))
                .partitionEnd(
                        statistics.getUpperBound()
                                .orElse(null))
                .catalogTable(
                        table.getCatalogTable())
                .build();
    }
}
