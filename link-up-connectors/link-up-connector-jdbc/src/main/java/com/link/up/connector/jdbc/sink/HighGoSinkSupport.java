package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.highgo.HighGoCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.highgo.HighGoJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.highgo.HighGoTypeMapper;

/** Small HighGo target-path/DDL adapter kept out of the already-large generic resolver. */
final class HighGoSinkSupport {

    private HighGoSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        return config != null
                && (DatabaseIdentifier.HIGHGO.equalsIgnoreCase(config.getDialect())
                || HighGoJdbcUrl.accepts(config.getUrl()));
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {

        if (config == null || tablePath == null) {
            return tablePath;
        }
        String database = HighGoJdbcUrl.databaseName(config.getUrl());
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
        } else {
            schema = defaultSchema(config);
        }

        return hasText(schema)
                ? TablePath.of(database, schema, tablePath.getTableName())
                : null;
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
        return new HighGoCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                new HighGoTypeMapper())
                .build();
    }

    private static String defaultSchema(JdbcConnectionConfig config) {
        String urlSchema = HighGoJdbcUrl.currentSchema(config.getUrl(), null);
        if (hasText(urlSchema)) {
            return urlSchema.trim();
        }
        String propertySchema = HighGoJdbcUrl.currentSchema(
                config.getUrl(), config.getProperties());
        if (hasText(propertySchema)) {
            return propertySchema.trim();
        }
        if (hasText(config.getSchema())) {
            return config.getSchema().trim();
        }
        return "public";
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
