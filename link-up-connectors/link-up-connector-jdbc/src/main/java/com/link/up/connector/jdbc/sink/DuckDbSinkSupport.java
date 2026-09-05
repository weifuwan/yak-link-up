package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.duckdb.DuckDbCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.duckdb.DuckDbJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.duckdb.DuckDbTypeMapper;

/** DuckDB target-path and DDL-preview support for sink preparation. */
final class DuckDbSinkSupport {

    private DuckDbSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        if (config == null) {
            return false;
        }
        if (DatabaseIdentifier.DUCKDB.equalsIgnoreCase(config.getDialect())) {
            return true;
        }
        return DuckDbJdbcUrl.accepts(config.getUrl());
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {

        if (config == null || tablePath == null) {
            return tablePath;
        }
        String database = DuckDbJdbcUrl.databaseName(config.getUrl());
        if (!hasText(database)) {
            return null;
        }

        String pathDatabase = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();
        String schema;
        if (hasText(pathSchema)
                && (!hasText(pathDatabase)
                || database.equalsIgnoreCase(pathDatabase))) {
            schema = pathSchema.trim();
        } else if (hasText(config.getSchema())) {
            schema = config.getSchema().trim();
        } else {
            String configured = DuckDbJdbcUrl.configuredSchema(
                    config.getUrl(), config.getProperties());
            schema = hasText(configured) ? configured.trim() : "main";
        }

        return TablePath.of(database, schema, tablePath.getTableName());
    }

    static String resolveCreateTableSql(
            JdbcConnectionConfig config,
            CatalogTable table) {

        if (config == null || table == null) {
            return null;
        }
        TablePath targetPath = resolveTargetPath(config, table.getTablePath());
        if (targetPath == null) {
            return null;
        }
        CatalogTable ddlTable = table.getTablePath().equals(targetPath)
                ? table
                : table.withPath(targetPath);
        return new DuckDbCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                new DuckDbTypeMapper())
                .build();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
