package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cDialect;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GBase8cSinkSupportTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoGBase8cTarget() {
        TablePath target =
                GBase8cSinkSupport.resolveTargetPath(
                        config("target_db", "landing", DatabaseIdentifier.GBASE8C),
                        TablePath.of("source_db", "source_schema", "orders"));

        assertEquals(
                TablePath.of("target_db", "landing", "orders"),
                target);
    }

    @Test
    public void foreignSourceSchemaFallsBackToPublicWithoutTargetSchema() {
        TablePath target =
                GBase8cSinkSupport.resolveTargetPath(
                        config("target_db", null, DatabaseIdentifier.GBASE8C),
                        TablePath.of("source_db", "source_schema", "orders"));

        assertEquals(
                TablePath.of("target_db", "public", "orders"),
                target);
    }

    @Test
    public void explicitSchemaTableMappingWinsOverConfiguredDefaultSchema() {
        TablePath target =
                GBase8cSinkSupport.resolveTargetPath(
                        config("target_db", "public", DatabaseIdentifier.GBASE8C),
                        TablePath.of(null, "archive", "orders"));

        assertEquals(
                TablePath.of("target_db", "archive", "orders"),
                target);
    }

    @Test
    public void schemaCanBePreservedWhenPathAlreadyTargetsSameDatabase() {
        TablePath target =
                GBase8cSinkSupport.resolveTargetPath(
                        config("target_db", "public", DatabaseIdentifier.GBASE8C),
                        TablePath.of("target_db", "sales", "orders"));

        assertEquals(
                TablePath.of("target_db", "sales", "orders"),
                target);
    }

    @Test
    public void dedicatedUrlCanSelectSinkSupportWithoutExplicitDialect() {
        assertTrue(GBase8cSinkSupport.accepts(config("target_db", null, null)));
        assertTrue(GBase8cSinkSupport.accepts(
                config("target_db", null, DatabaseIdentifier.GBASE8C)));
        assertFalse(GBase8cSinkSupport.accepts(
                config("target_db", null, DatabaseIdentifier.POSTGRESQL)));
    }

    @Test
    public void existingTableSinkDoesNotGenerateAutomaticCreateTableSql() {
        JdbcConnectionConfig config =
                config("target_db", "public", DatabaseIdentifier.GBASE8C);

        assertNull(
                JdbcCreateTableSqlResolver.resolve(
                        new GBase8cDialect(config),
                        config,
                        table()));
    }

    private static CatalogTable table() {
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        Column.builder("id", BasicType.LONG_TYPE)
                                                .nullable(false)
                                                .build()))
                        .build();

        return CatalogTable.builder(
                        TablePath.of("source_db", "source_schema", "orders"),
                        schema)
                .build();
    }

    private static JdbcConnectionConfig config(
            String database,
            String schema,
            String dialect) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:gbase8c://127.0.0.1:5432/" + database);
        values.put("driver", "com.gbase8c.Driver");
        values.put("username", "gbase");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (dialect != null) {
            values.put("dialect", dialect);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }
}
