package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcHanaTargetPathTest {

    @Test
    public void resolvesHanaTargetSchemaFromConnectionSettings() {
        JdbcConnectionConfig config = config("TARGET");

        assertEquals(
                TablePath.of("HXE", "TARGET", "ORDERS"),
                HanaSinkSupport.resolveTargetPath(
                        config,
                        TablePath.of("ORDERS")));
    }

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoHanaTarget() {
        JdbcConnectionConfig config = config("TARGET");

        assertEquals(
                TablePath.of("HXE", "TARGET", "ORDERS"),
                HanaSinkSupport.resolveTargetPath(
                        config,
                        TablePath.of("source_db", "public", "ORDERS")));
    }

    @Test
    public void explicitSchemaTableMappingIsPreserved() {
        JdbcConnectionConfig config = config("TARGET");

        assertEquals(
                TablePath.of("HXE", "ARCHIVE", "ORDERS"),
                HanaSinkSupport.resolveTargetPath(
                        config,
                        TablePath.of(null, "ARCHIVE", "ORDERS")));
    }

    @Test
    public void resolvesHanaSpecificCreateTableSql() {
        JdbcConnectionConfig config = config("TARGET");
        CatalogTable table = CatalogTable.builder(
                TablePath.of("source_db", "public", "ORDERS"),
                TableSchema.builder()
                        .column(Column.builder("ID", BasicType.LONG_TYPE)
                                .nullable(false)
                                .build())
                        .build())
                .build();

        String ddl = HanaSinkSupport.resolveCreateTableSql(config, table);
        assertTrue(ddl.contains("CREATE COLUMN TABLE \"TARGET\".\"ORDERS\""));
        assertTrue(ddl.contains("\"ID\" BIGINT NOT NULL"));
    }

    private static JdbcConnectionConfig config(String schema) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:sap://127.0.0.1:30013/?databaseName=HXE");
        values.put("driver", "com.sap.db.jdbc.Driver");
        values.put("username", "SYSTEM");
        values.put("schema", schema);
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }
}
