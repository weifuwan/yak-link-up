package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.xugu.XuguCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.xugu.XuguJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.xugu.XuguTypeMapper;

/** Small XuguDB target-path/DDL adapter kept out of the generic resolver. */
final class XuguSinkSupport {

    private XuguSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        return config != null
                && (DatabaseIdentifier.XUGU.equalsIgnoreCase(config.getDialect())
                || XuguJdbcUrl.accepts(config.getUrl()));
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {

        if (config == null || tablePath == null) {
            return tablePath;
        }
        String database = XuguJdbcUrl.databaseName(config.getUrl());
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
        return new XuguCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                new XuguTypeMapper())
                .build();
    }

    private static String defaultSchema(JdbcConnectionConfig config) {
        String jdbcMode = XuguJdbcUrl.compatibleMode(
                config.getUrl(), config.getProperties());
        String mode = hasText(jdbcMode) ? jdbcMode : config.getCompatibleMode();

        String jdbcSchema = XuguJdbcUrl.currentSchema(
                config.getUrl(), config.getProperties());
        if (hasText(jdbcSchema)) {
            return XuguJdbcUrl.normalizeSessionIdentifier(jdbcSchema, mode);
        }
        if (hasText(config.getSchema())) {
            // Connector schema is an explicit physical object name and is not
            // reinterpreted as an unquoted SQL token.
            return config.getSchema().trim();
        }
        String user = config.getUsername();
        if (!hasText(user)) {
            user = XuguJdbcUrl.user(config.getUrl(), config.getProperties());
        }
        return XuguJdbcUrl.normalizeSessionIdentifier(user, mode);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
