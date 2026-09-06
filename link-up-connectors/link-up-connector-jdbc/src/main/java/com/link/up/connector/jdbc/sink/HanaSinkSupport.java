package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.hana.HanaCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.hana.HanaJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.hana.HanaTypeMapper;

/** SAP HANA target normalization for the shared JDBC offline sink. */
final class HanaSinkSupport {

    private HanaSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            return false;
        }
        if (DatabaseIdentifier.HANA.equalsIgnoreCase(connectionConfig.getDialect())) {
            return true;
        }
        return HanaJdbcUrl.accepts(connectionConfig.getUrl());
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig connectionConfig,
            TablePath tablePath) {

        if (connectionConfig == null || tablePath == null) {
            return tablePath;
        }

        String database = HanaJdbcUrl.databaseName(
                connectionConfig.getUrl(),
                connectionConfig.getProperties());
        String pathDatabase = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();

        String schema = null;
        if (hasText(pathSchema)
                && (!hasText(pathDatabase)
                || (hasText(database) && database.equalsIgnoreCase(pathDatabase)))) {
            schema = pathSchema.trim();
        }

        if (!hasText(schema)) {
            schema = HanaJdbcUrl.currentSchema(
                    connectionConfig.getUrl(),
                    connectionConfig.getProperties(),
                    connectionConfig.getSchema());
        }

        return TablePath.of(
                hasText(database) ? database.trim() : null,
                hasText(schema) ? schema.trim() : null,
                tablePath.getTableName());
    }

    static String resolveCreateTableSql(
            JdbcConnectionConfig connectionConfig,
            CatalogTable table) {

        if (connectionConfig == null || table == null) {
            return null;
        }

        TablePath targetPath = resolveTargetPath(
                connectionConfig,
                table.getTablePath());
        CatalogTable ddlTable = table.getTablePath().equals(targetPath)
                ? table
                : table.withPath(targetPath);

        return new HanaCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                new HanaTypeMapper())
                .build();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
