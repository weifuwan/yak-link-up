package com.link.up.connector.jdbc.core.dialect.highgo;

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

public class HighGoDialectTest {

    @Test
    public void loadsHighGoDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null, baseUrl(), null));
        assertEquals(DatabaseIdentifier.HIGHGO, dialect.name());
    }

    @Test
    public void parsesUnquotedIdentifiersAsLowercaseAndPreservesQuotedCase() {
        HighGoDialect dialect = dialect("target");
        assertEquals(
                TablePath.of(null, "app", "orders"),
                dialect.parseTablePath("APP.ORDERS"));
        assertEquals(
                TablePath.of(null, "App", "Orders"),
                dialect.parseTablePath("\"App\".\"Orders\""));
        assertEquals(
                TablePath.of(null, "A.B", "Orders"),
                dialect.parseTablePath("\"A.B\".\"Orders\""));
    }

    @Test
    public void currentDatabaseThreePartPathIsAccepted() {
        HighGoDialect dialect = dialect("public");
        assertEquals(
                TablePath.of("highgo", "app", "orders"),
                dialect.parseTablePath("HIGHGO.APP.ORDERS"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void foreignDatabaseThreePartPathIsRejected() {
        dialect("public").parseTablePath("other.app.orders");
    }

    @Test
    public void foreignSourceSchemaDoesNotLeakIntoHighGoTargetSql() {
        HighGoDialect dialect = dialect("target");
        assertEquals(
                "\"target\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of("source_db", "public", "orders")));
    }

    @Test
    public void urlCurrentSchemaHasHighestPrecedence() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("currentSchema", "from_properties");
        HighGoDialect dialect = new HighGoDialect(config(
                "from_connector",
                baseUrl() + "?currentSchema=from_url",
                properties));
        assertEquals(
                "\"from_url\".\"orders\"",
                dialect.tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void propertiesCurrentSchemaPrecedesConnectorSchema() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("currentSchema", "from_properties");
        HighGoDialect dialect = new HighGoDialect(config(
                "from_connector",
                baseUrl(),
                properties));
        assertEquals(
                "\"from_properties\".\"orders\"",
                dialect.tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void connectorSchemaIsUsedWhenJdbcSchemaIsAbsent() {
        assertEquals(
                "\"app\".\"orders\"",
                dialect("app").tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void publicIsDefaultSchema() {
        HighGoDialect dialect = new HighGoDialect(config(null, baseUrl(), null));
        assertEquals(
                "\"public\".\"orders\"",
                dialect.tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void buildsPrimaryKeyScopedOnConflictUpsert() {
        String sql = dialect("app").buildUpsertSql(
                TablePath.of("highgo", "app", "users"),
                Arrays.asList("id", "name"),
                Arrays.asList("id")).get();

        assertTrue(sql.startsWith(
                "INSERT INTO \"app\".\"users\" (\"id\", \"name\") VALUES (?, ?)"));
        assertTrue(sql.contains(
                "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""));
        assertFalse(sql.contains("\"id\" = EXCLUDED.\"id\""));
    }

    @Test
    public void allPrimaryKeyUpsertUsesDoNothing() {
        String sql = dialect("app").buildUpsertSql(
                TablePath.of("highgo", "app", "users"),
                Arrays.asList("id"),
                Arrays.asList("id")).get();
        assertTrue(sql.endsWith("ON CONFLICT (\"id\") DO NOTHING"));
    }

    @Test
    public void buildsHashtextBucketPredicate() {
        Column column = Column.builder("code", BasicType.STRING_TYPE).build();
        assertEquals(
                "MOD(ABS(HASHTEXT(CAST(\"code\" AS TEXT))::BIGINT), 8) = 3",
                dialect("app")
                        .buildHashPartitionPredicate(column, 3, 8)
                        .get());
    }

    @Test
    public void parsesDatabaseAndCurrentSchemaFromUrl() {
        String url = baseUrl() + "?currentSchema=app,public&sslmode=require";
        assertEquals("highgo", HighGoJdbcUrl.databaseName(url));
        assertEquals("app", HighGoJdbcUrl.currentSchema(url, null));
        assertEquals(
                "jdbc:highgo://127.0.0.1:5866/analytics?currentSchema=app,public&sslmode=require",
                HighGoJdbcUrl.withDatabase(url, "analytics"));
    }

    @Test
    public void addsBatchRewriteAndConnectorSchemaDefaults() {
        HighGoDialect dialect = dialect("app");
        assertEquals("true", dialect.defaultConnectionProperties().get("reWriteBatchedInserts"));
        assertEquals("app", dialect.defaultConnectionProperties().get("currentSchema"));
    }

    private static HighGoDialect dialect(String schema) {
        return new HighGoDialect(config(schema, baseUrl(), null));
    }

    private static JdbcConnectionConfig config(
            String schema,
            String url,
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
