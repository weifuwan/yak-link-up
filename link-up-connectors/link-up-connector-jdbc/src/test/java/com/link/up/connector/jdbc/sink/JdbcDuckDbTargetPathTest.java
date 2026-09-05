package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcDuckDbTargetPathTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoDuckDbTarget() {
        TablePath target = DuckDbSinkSupport.resolveTargetPath(
                config(fileUrl(), "target", null),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("warehouse", "target", "orders"),
                target);
    }

    @Test
    public void explicitTargetSchemaIsPreserved() {
        TablePath target = DuckDbSinkSupport.resolveTargetPath(
                config(fileUrl(), "target", null),
                TablePath.of(null, "analytics", "orders"));

        assertEquals(
                TablePath.of("warehouse", "analytics", "orders"),
                target);
    }

    @Test
    public void urlSchemaIsUsedWhenConnectorSchemaIsAbsent() {
        TablePath target = DuckDbSinkSupport.resolveTargetPath(
                config(fileUrl() + ";schema=analytics", null, null),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("warehouse", "analytics", "orders"),
                target);
    }

    @Test
    public void mainIsDefaultSchema() {
        TablePath target = DuckDbSinkSupport.resolveTargetPath(
                config(fileUrl(), null, null),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("warehouse", "main", "orders"),
                target);
    }

    @Test
    public void pinnedNamedMemoryUsesMemoryCatalog() {
        TablePath target = DuckDbSinkSupport.resolveTargetPath(
                config(
                        "jdbc:duckdb:memory:shared;jdbc_pin_db=true",
                        null,
                        null),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("memory", "main", "orders"),
                target);
    }

    private static JdbcConnectionConfig config(
            String url,
            String schema,
            Map<String, String> properties) {

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "org.duckdb.DuckDBDriver");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String fileUrl() {
        return "jdbc:duckdb:/tmp/warehouse.duckdb";
    }
}
