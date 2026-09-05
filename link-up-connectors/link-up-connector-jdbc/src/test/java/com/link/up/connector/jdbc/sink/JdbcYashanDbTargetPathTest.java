package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcYashanDbTargetPathTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoYashanTarget() {
        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                config("TARGET"),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("YASDB", "TARGET", "orders"),
                target);
    }

    @Test
    public void explicitTargetSchemaIsPreserved() {
        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                config("TARGET"),
                TablePath.of(null, "APP", "ORDERS"));

        assertEquals(
                TablePath.of("YASDB", "APP", "ORDERS"),
                target);
    }

    @Test
    public void usernameIsUsedWhenConnectorSchemaIsAbsent() {
        Map<String, Object> values = baseValues();
        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values)),
                TablePath.of("ORDERS"));

        assertEquals(
                TablePath.of("YASDB", "APPUSER", "ORDERS"),
                target);
    }

    private static JdbcConnectionConfig config(String schema) {
        Map<String, Object> values = baseValues();
        values.put("schema", schema);
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> baseValues() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:yasdb://127.0.0.1:1688/YASDB");
        values.put("driver", "com.yashandb.jdbc.Driver");
        values.put("username", "appuser");
        return values;
    }
}
