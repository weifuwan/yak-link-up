package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcIrisTargetPathTest {

    @Test
    public void sourceNamespaceAndSchemaDoNotLeakIntoIrisTarget() {
        TablePath target = IrisSinkSupport.resolveTargetPath(
                config("Target"),
                TablePath.of("source_db", "public", "Orders"));

        assertEquals(
                TablePath.of("USER", "Target", "Orders"),
                target);
    }

    @Test
    public void explicitTargetSchemaIsPreserved() {
        TablePath target = IrisSinkSupport.resolveTargetPath(
                config("Target"),
                TablePath.of(null, "App", "Orders"));

        assertEquals(
                TablePath.of("USER", "App", "Orders"),
                target);
    }

    @Test
    public void currentNamespaceSchemaIsPreserved() {
        TablePath target = IrisSinkSupport.resolveTargetPath(
                config("Target"),
                TablePath.of("USER", "Analytics", "Orders"));

        assertEquals(
                TablePath.of("USER", "Analytics", "Orders"),
                target);
    }

    @Test
    public void sqlUserIsDefaultTargetSchema() {
        TablePath target = IrisSinkSupport.resolveTargetPath(
                config(null),
                TablePath.of("Orders"));

        assertEquals(
                TablePath.of("USER", "SQLUser", "Orders"),
                target);
    }

    @Test
    public void namespaceParserIsNotConfusedByDriverArguments() {
        Map<String, Object> values = baseValues();
        values.put("url", "jdbc:IRIS://127.0.0.1:1972/APP/UTF-8/1");
        JdbcConnectionConfig config =
                JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));

        TablePath target = IrisSinkSupport.resolveTargetPath(
                config,
                TablePath.of("Orders"));

        assertEquals(
                TablePath.of("APP", "SQLUser", "Orders"),
                target);
    }

    private static JdbcConnectionConfig config(String schema) {
        Map<String, Object> values = baseValues();
        if (schema != null) {
            values.put("schema", schema);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> baseValues() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:IRIS://127.0.0.1:1972/USER");
        values.put("driver", "com.intersystems.jdbc.IRISDriver");
        values.put("username", "_SYSTEM");
        return values;
    }
}
