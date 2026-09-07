package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cJdbcUrl;

/** Target-path normalization for the GBase 8c existing-table Sink stage. */
final class GBase8cSinkSupport {

    private static final String DEFAULT_SCHEMA = "public";

    private GBase8cSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        if (config == null) {
            return false;
        }
        if (hasText(config.getDialect())) {
            return DatabaseIdentifier.GBASE8C.equalsIgnoreCase(config.getDialect());
        }
        return GBase8cJdbcUrl.accepts(config.getUrl());
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {
        if (config == null || tablePath == null) {
            return tablePath;
        }

        String database = GBase8cJdbcUrl.databaseName(config.getUrl());
        if (!hasText(database)) {
            return null;
        }

        String pathDatabase = tablePath.getDatabaseName();
        String pathSchema = tablePath.getSchemaName();
        String schema;

        /*
         * Keep schema.table from an explicit target mapping. A three-part source path is safe to
         * reuse only when its database is already the target database; otherwise prefer target
         * connection settings so source database/schema metadata cannot leak into the Sink.
         */
        if (hasText(pathSchema)
                && (!hasText(pathDatabase)
                || database.equals(pathDatabase.trim()))) {
            schema = pathSchema.trim();
        } else if (hasText(config.getSchema())) {
            schema = config.getSchema().trim();
        } else {
            schema = DEFAULT_SCHEMA;
        }

        return TablePath.of(
                database,
                schema,
                tablePath.getTableName());
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
