package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcOpenGaussTargetPathTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoOpenGaussTarget() {
        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                config("target"),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("postgres", "target", "orders"),
                target);
    }

    @Test
    public void explicitTargetSchemaIsPreserved() {
        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                config("target"),
                TablePath.of(null, "app", "orders"));

        assertEquals(
                TablePath.of("postgres", "app", "orders"),
                target);
    }

    @Test
    public void urlCurrentSchemaIsUsedWhenConnectorSchemaIsAbsent() {
        Map<String, Object> values = baseValues();
        values.put(
                "url",
                "jdbc:opengauss://127.0.0.1:5432/postgres?currentSchema=app");

        TablePath target = JdbcCreateTableSqlResolver.resolveTargetPath(
                JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values)),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("postgres", "app", "orders"),
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
                "jdbc:opengauss://127.0.0.1:5432/postgres");
        values.put("driver", "org.opengauss.Driver");
        values.put("username", "gaussdb");
        return values;
    }
}
