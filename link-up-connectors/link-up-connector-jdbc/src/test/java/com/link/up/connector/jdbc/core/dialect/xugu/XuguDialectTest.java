package com.link.up.connector.jdbc.core.dialect.xugu;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XuguDialectTest {

    @Test
    public void loadsXuguDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null, null, baseUrl(), null));
        assertEquals(DatabaseIdentifier.XUGU, dialect.name());
    }

    @Test
    public void defaultModeUppercasesUnquotedIdentifiersAndPreservesQuotedCase() {
        XuguDialect dialect = dialect("APP", null);
        assertEquals(
                TablePath.of(null, "APP", "ORDERS"),
                dialect.parseTablePath("app.orders"));
        assertEquals(
                TablePath.of(null, "App", "Orders"),
                dialect.parseTablePath("\"App\".\"Orders\""));
    }

    @Test
    public void postgresModeLowercasesAndMysqlModePreservesIdentifiers() {
        assertEquals(
                TablePath.of(null, "app", "orders"),
                dialect("app", "POSTGRESQL").parseTablePath("APP.Orders"));
        assertEquals(
                TablePath.of(null, "App", "Orders"),
                dialect("App", "MYSQL").parseTablePath("App.Orders"));
    }

    @Test
    public void currentDatabaseThreePartPathIsAccepted() {
        assertEquals(
                TablePath.of("SYSTEM", "APP", "ORDERS"),
                dialect("SYSDBA", null)
                        .parseTablePath("system.app.orders"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void foreignDatabaseThreePartPathIsRejected() {
        dialect("SYSDBA", null).parseTablePath("other.app.orders");
    }

    @Test
    public void urlCurrentSchemaHasHighestPrecedence() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("current_schema", "FROM_PROPERTIES");
        XuguDialect dialect = new XuguDialect(config(
                "FROM_CONNECTOR",
                null,
                baseUrl() + "?current_schema=FROM_URL",
                properties));
        assertEquals(
                "\"FROM_URL\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void defaultModeFoldsLowercaseUrlCurrentSchemaToUppercase() {
        XuguDialect dialect = new XuguDialect(config(
                null,
                null,
                baseUrl() + "?current_schema=app_schema",
                null));
        assertEquals(
                "\"APP_SCHEMA\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void quotedUrlCurrentSchemaPreservesExactCase() {
        XuguDialect dialect = new XuguDialect(config(
                null,
                null,
                baseUrl() + "?current_schema=%22AppSchema%22",
                null));
        assertEquals(
                "\"AppSchema\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void usernameSchemaIsUsedWhenNoSchemaConfigured() {
        XuguDialect dialect = new XuguDialect(config(null, null, baseUrl(), null));
        assertEquals(
                "\"SYSDBA\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void postgresModeNormalizesUsernameDefaultSchema() {
        XuguDialect dialect = new XuguDialect(
                configWithUsername(null, "POSTGRESQL", "AppUser"));
        assertEquals(
                "\"appuser\".\"orders\"",
                dialect.tableIdentifier(dialect.parseTablePath("orders")));
    }

    @Test
    public void buildsMergeUpsertWithJdbcPlaceholders() {
        String sql = dialect("APP", null).buildUpsertSql(
                TablePath.of("SYSTEM", "APP", "USERS"),
                Arrays.asList("ID", "NAME"),
                Arrays.asList("ID")).get();

        assertTrue(sql.startsWith(
                "MERGE INTO \"APP\".\"USERS\" TARGET USING (SELECT ? \"ID\", ? \"NAME\" FROM DUAL) SOURCE"));
        assertTrue(sql.contains(
                "ON (TARGET.\"ID\"=SOURCE.\"ID\")"));
        assertTrue(sql.contains(
                "WHEN MATCHED THEN UPDATE SET TARGET.\"NAME\"=SOURCE.\"NAME\""));
        assertTrue(sql.contains(
                "WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\") VALUES (SOURCE.\"ID\", SOURCE.\"NAME\")"));
        assertFalse(sql.contains("TARGET.\"ID\"=SOURCE.\"ID\", TARGET.\"NAME\""));
    }

    @Test
    public void primaryKeyOnlyMergeUsesInsertOnlyBranch() {
        String sql = dialect("APP", null).buildUpsertSql(
                TablePath.of("SYSTEM", "APP", "USERS"),
                Arrays.asList("ID"),
                Arrays.asList("ID")).get();
        assertFalse(sql.contains("WHEN MATCHED THEN UPDATE"));
        assertTrue(sql.contains(
                "WHEN NOT MATCHED THEN INSERT (\"ID\") VALUES (SOURCE.\"ID\")"));
    }

    @Test
    public void stringHashPartitionRemainsUnsupported() {
        Column column = Column.builder("CODE", BasicType.STRING_TYPE).build();
        assertFalse(dialect("APP", null)
                .buildHashPartitionPredicate(column, 0, 4)
                .isPresent());
    }

    @Test
    public void parsesDatabaseSchemaAndUserFromUrl() {
        String url = baseUrl()
                + "?USER=guest&current_schema=analytics&compatiblemode=postgresql";
        assertEquals("SYSTEM", XuguJdbcUrl.databaseName(url));
        assertEquals("analytics", XuguJdbcUrl.currentSchema(url, null));
        assertEquals("guest", XuguJdbcUrl.user(url, null));
        assertEquals("postgresql", XuguJdbcUrl.compatibleMode(url, null));
    }

    private static XuguDialect dialect(String schema, String compatibleMode) {
        return new XuguDialect(config(schema, compatibleMode, baseUrl(), null));
    }

    private static JdbcConnectionConfig config(
            String schema,
            String compatibleMode,
            String url,
            Map<String, String> properties) {
        return configWithUsername(schema, compatibleMode, "SYSDBA", url, properties);
    }

    private static JdbcConnectionConfig configWithUsername(
            String schema,
            String compatibleMode,
            String username) {
        return configWithUsername(schema, compatibleMode, username, baseUrl(), null);
    }

    private static JdbcConnectionConfig configWithUsername(
            String schema,
            String compatibleMode,
            String username,
            String url,
            Map<String, String> properties) {

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.xugu.cloudjdbc.Driver");
        values.put("username", username);
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
