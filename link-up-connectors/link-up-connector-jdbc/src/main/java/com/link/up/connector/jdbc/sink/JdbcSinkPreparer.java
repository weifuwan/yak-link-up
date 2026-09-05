package com.link.up.connector.jdbc.sink;

import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.TableDdl;
import com.link.up.api.table.catalog.*;
import com.link.up.connector.jdbc.config.JdbcSinkConfig;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import com.link.up.connector.jdbc.sink.savemode.JdbcSaveModeHandler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes JDBC target mapping, validation and DDL once, before any task starts.
 */
final class JdbcSinkPreparer implements SinkPreparer {
    private final JdbcSinkConfig config;
    private final JdbcDialect dialect;

    JdbcSinkPreparer(JdbcSinkConfig config) {
        this.config = config;
        this.dialect = JdbcDialectLoader.load(config.getConnectionConfig());
    }

    private static List<String> columnNames(CatalogTable table) {
        List<String> names = new ArrayList<String>();
        table.getTableSchema().getColumns().forEach(column -> names.add(column.getName()));
        return names;
    }

    @Override
    public PreparedSinkMetadata prepare(SinkPrepareContext context) throws Exception {
        Map<TablePath, CatalogTable> targets = new LinkedHashMap<TablePath, CatalogTable>();
        Map<TablePath, Object> keys = new LinkedHashMap<TablePath, Object>();
        Map<TablePath, TableDdl> tableDdls = new LinkedHashMap<TablePath, TableDdl>();
        Catalog catalog = dialect.createCatalog(config.getConnectionConfig());
        try {
            if (!(catalog instanceof WritableCatalog)) {
                throw new IllegalStateException("JDBC catalog does not support DDL: " + dialect.name());
            }
            WritableCatalog writableCatalog = (WritableCatalog) catalog;
            for (Map.Entry<TablePath, CatalogTable> entry : context.getSourceTables().entrySet()) {
                CatalogTable target = resolveTargetTable(entry.getValue());
                List<String> primaryKeys = resolvePrimaryKeys(target);
                if (config.isUpsert() && !dialect.buildUpsertSql(target.getTablePath(), columnNames(target), primaryKeys).isPresent())
                    throw new IllegalArgumentException("Dialect does not support UPSERT: " + dialect.name());

                JdbcSaveModeHandler handler = new JdbcSaveModeHandler(
                        config.getSchemaSaveMode(),
                        config.getDataSaveMode(),
                        writableCatalog,
                        target,
                        config.isCreatePrimaryKey());

                boolean targetExistedBefore;
                long startedAtNanos;
                long durationMillis;
                try {
                    handler.open();
                    targetExistedBefore = writableCatalog.tableExists(target.getTablePath());
                    startedAtNanos = System.nanoTime();
                    handler.handleSaveMode();
                    durationMillis = TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - startedAtNanos);
                } finally {
                    handler.close();
                }

                CatalogTable createDefinition = handler.getCreateTableDefinition();
                TablePath ddlTargetPath = resolveTargetPath(createDefinition.getTablePath());
                boolean executed = handler.isTableCreated();
                String reason = executed
                        ? TableDdl.REASON_TARGET_TABLE_CREATED
                        : targetExistedBefore
                        ? TableDdl.REASON_TARGET_TABLE_ALREADY_EXISTS
                        : TableDdl.REASON_CREATE_TABLE_NOT_EXECUTED;

                TableDdl tableDdl = new TableDdl(
                        dialect.name(),
                        entry.getKey().toString(),
                        ddlTargetPath == null
                                ? target.getTablePath().toString()
                                : ddlTargetPath.toString(),
                        resolveCreateTableSql(createDefinition),
                        executed,
                        executed ? TableDdl.STATUS_SUCCEEDED : TableDdl.STATUS_SKIPPED,
                        reason,
                        durationMillis,
                        null,
                        null);

                targets.put(entry.getKey(), target);
                keys.put(entry.getKey(), new ArrayList<String>(primaryKeys));
                tableDdls.put(entry.getKey(), tableDdl);
            }
            return new PreparedSinkMetadata(targets, keys, tableDdls);
        } finally {
            catalog.close();
        }
    }

    private CatalogTable resolveTargetTable(CatalogTable source) {
        String path = config.resolveTargetTablePath(source.getTablePath());
        CatalogTable mapped = path == null
                ? source
                : source.withPath(dialect.parseTablePath(path));
        TablePath targetPath = resolveTargetPath(mapped.getTablePath());
        return targetPath == null || mapped.getTablePath().equals(targetPath)
                ? mapped
                : mapped.withPath(targetPath);
    }

    private TablePath resolveTargetPath(TablePath tablePath) {
        if (DuckDbSinkSupport.accepts(config.getConnectionConfig())) {
            return DuckDbSinkSupport.resolveTargetPath(
                    config.getConnectionConfig(), tablePath);
        }
        if (HighGoSinkSupport.accepts(config.getConnectionConfig())) {
            return HighGoSinkSupport.resolveTargetPath(
                    config.getConnectionConfig(), tablePath);
        }
        return JdbcCreateTableSqlResolver.resolveTargetPath(
                config.getConnectionConfig(), tablePath);
    }

    private String resolveCreateTableSql(CatalogTable table) {
        if (DuckDbSinkSupport.accepts(config.getConnectionConfig())) {
            return DuckDbSinkSupport.resolveCreateTableSql(
                    config.getConnectionConfig(), table);
        }
        if (HighGoSinkSupport.accepts(config.getConnectionConfig())) {
            return HighGoSinkSupport.resolveCreateTableSql(
                    config.getConnectionConfig(), table);
        }
        return JdbcCreateTableSqlResolver.resolve(
                dialect,
                config.getConnectionConfig(),
                table);
    }

    private List<String> resolvePrimaryKeys(CatalogTable table) {
        if (config.hasConfiguredPrimaryKeys()) return config.getPrimaryKeys();
        PrimaryKey key = table.getTableSchema().getPrimaryKey();
        return key == null ? new ArrayList<String>() : key.getColumnNames();
    }
}
