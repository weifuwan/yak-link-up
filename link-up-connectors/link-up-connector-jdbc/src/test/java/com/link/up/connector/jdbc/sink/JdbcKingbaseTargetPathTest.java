package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcKingbaseTargetPathTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoKingbaseTarget() {
        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                config("target"),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("TEST", "target", "orders"),
                target);
    }

    @Test
    public void explicitTargetSchemaIsPreserved() {
        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                config("target"),
                TablePath.of(null, "app", "orders"));

        assertEquals(
                TablePath.of("TEST", "app", "orders"),
                target);
    }

    @Test
    public void urlCurrentSchemaIsUsedWhenConnectorSchemaIsAbsent() {
        Map<String, Object> values = baseValues();
        values.put(
                "url",
                "jdbc:kingbase8://127.0.0.1:54321/TEST?currentSchema=app");

        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values)),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("TEST", "app", "orders"),
                target);
    }

    private static JdbcConnectionConfig config(String schema) {
        Map<String, Object> values = baseValues();
        values.put("schema", schema);
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> baseValues() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(
                "url",
                "jdbc:kingbase8://127.0.0.1:54321/TEST");
        values.put("driver", "com.kingbase8.Driver");
        values.put("username", "system");
        return values;
    }
}
