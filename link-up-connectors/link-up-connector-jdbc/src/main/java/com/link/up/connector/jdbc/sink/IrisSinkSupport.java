package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.iris.IrisCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.iris.IrisJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.iris.IrisTypeMapper;

/** Small IRIS target-path/DDL adapter kept out of the generic resolver. */
final class IrisSinkSupport {

    private IrisSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        return config != null
                && (DatabaseIdentifier.IRIS.equalsIgnoreCase(config.getDialect())
                || IrisJdbcUrl.accepts(config.getUrl()));
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {

        if (config == null || tablePath == null) {
            return tablePath;
        }
        String namespace = IrisJdbcUrl.namespaceName(config.getUrl());
        if (!hasText(namespace)) {
            return null;
        }

        String pathNamespace = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();
        String schema;
        if (hasText(pathSchema)
                && (!hasText(pathNamespace)
                || namespace.equalsIgnoreCase(pathNamespace))) {
            schema = pathSchema.trim();
        } else {
            schema = defaultSchema(config);
        }
        return TablePath.of(namespace, schema, tablePath.getTableName());
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
        return new IrisCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                new IrisTypeMapper())
                .build();
    }

    private static String defaultSchema(JdbcConnectionConfig config) {
        return hasText(config.getSchema())
                ? config.getSchema().trim()
                : "SQLUser";
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
