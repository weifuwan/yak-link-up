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

public class JdbcHighGoTargetPathTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoHighGoTarget() {
        TablePath target = HighGoSinkSupport.resolveTargetPath(
                config(baseUrl(), "target", null),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("highgo", "target", "orders"),
                target);
    }

    @Test
    public void explicitTargetSchemaIsPreserved() {
        TablePath target = HighGoSinkSupport.resolveTargetPath(
                config(baseUrl(), "target", null),
                TablePath.of(null, "app", "orders"));

        assertEquals(
                TablePath.of("highgo", "app", "orders"),
                target);
    }

    @Test
    public void urlCurrentSchemaHasHighestPrecedence() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("currentSchema", "from_properties");
        TablePath target = HighGoSinkSupport.resolveTargetPath(
                config(
                        baseUrl() + "?currentSchema=from_url",
                        "from_connector",
                        properties),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("highgo", "from_url", "orders"),
                target);
    }

    @Test
    public void propertiesSchemaPrecedesConnectorSchema() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("currentSchema", "from_properties");
        TablePath target = HighGoSinkSupport.resolveTargetPath(
                config(baseUrl(), "from_connector", properties),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("highgo", "from_properties", "orders"),
                target);
    }

    @Test
    public void publicIsDefaultTargetSchema() {
        TablePath target = HighGoSinkSupport.resolveTargetPath(
                config(baseUrl(), null, null),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("highgo", "public", "orders"),
                target);
    }

    @Test
    public void resolvesHighGoCreateTablePreview() {
        TablePath sourcePath = TablePath.of("mysql_db", "orders");
        CatalogTable table = CatalogTable.builder(
                sourcePath,
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE)
                                .nullable(false)
                                .build())
                        .column(Column.builder("name", BasicType.STRING_TYPE)
                                .length(64L)
                                .build())
                        .build())
                .build();

        String ddl = HighGoSinkSupport.resolveCreateTableSql(
                config(baseUrl(), "app", null),
                table);

        assertTrue(ddl.contains("CREATE TABLE \"app\".\"orders\""));
        assertTrue(ddl.contains("\"id\" BIGINT NOT NULL"));
        assertTrue(ddl.contains("\"name\" VARCHAR(64) NULL"));
    }

    private static JdbcConnectionConfig config(
            String url,
            String schema,
            Map<String, String> properties) {

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.highgo.jdbc.Driver");
        values.put("username", "highgo");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:highgo://127.0.0.1:5866/highgo";
    }
}
