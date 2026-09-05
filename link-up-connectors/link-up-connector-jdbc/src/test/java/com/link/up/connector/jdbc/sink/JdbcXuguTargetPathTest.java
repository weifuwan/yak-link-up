package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcXuguTargetPathTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoXuguTarget() {
        TablePath target = XuguSinkSupport.resolveTargetPath(
                config(null, null, baseUrl(), "SYSDBA", null),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("SYSTEM", "SYSDBA", "orders"),
                target);
    }

    @Test
    public void explicitTargetSchemaWithoutDatabaseIsPreserved() {
        TablePath target = XuguSinkSupport.resolveTargetPath(
                config("App", null, baseUrl(), "SYSDBA", null),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("SYSTEM", "App", "orders"),
                target);
    }

    @Test
    public void explicitMappedTargetSchemaIsPreserved() {
        TablePath target = XuguSinkSupport.resolveTargetPath(
                config("TARGET", null, baseUrl(), "SYSDBA", null),
                TablePath.of(null, "APP", "orders"));

        assertEquals(
                TablePath.of("SYSTEM", "APP", "orders"),
                target);
    }

    @Test
    public void urlCurrentSchemaHasHighestPrecedenceForUnqualifiedTarget() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("current_schema", "FROM_PROPERTIES");
        TablePath target = XuguSinkSupport.resolveTargetPath(
                config(
                        "FROM_CONNECTOR",
                        null,
                        baseUrl() + "?current_schema=from_url",
                        "SYSDBA",
                        properties),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("SYSTEM", "FROM_URL", "orders"),
                target);
    }

    @Test
    public void postgresModeNormalizesUsernameFallbackToLowercase() {
        TablePath target = XuguSinkSupport.resolveTargetPath(
                config(null, "POSTGRESQL", baseUrl(), "AppUser", null),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("SYSTEM", "appuser", "orders"),
                target);
    }

    @Test
    public void mysqlModePreservesUsernameFallbackSpelling() {
        TablePath target = XuguSinkSupport.resolveTargetPath(
                config(null, "MYSQL", baseUrl(), "AppUser", null),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("SYSTEM", "AppUser", "orders"),
                target);
    }

    @Test
    public void defaultModeUppercasesUsernameFallback() {
        TablePath target = XuguSinkSupport.resolveTargetPath(
                config(null, null, baseUrl(), "appuser", null),
                TablePath.of("orders"));

        assertEquals(
                TablePath.of("SYSTEM", "APPUSER", "orders"),
                target);
    }

    private static JdbcConnectionConfig config(
            String schema,
            String compatibleMode,
            String url,
            String username,
            Map<String, String> properties) {

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.xugu.cloudjdbc.Driver");
        if (username != null) {
            values.put("username", username);
        }
        if (schema != null) {
            values.put("schema", schema);
        }
        if (compatibleMode != null) {
            values.put("compatible_mode", compatibleMode);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:xugu://127.0.0.1:5138/SYSTEM";
    }
}
