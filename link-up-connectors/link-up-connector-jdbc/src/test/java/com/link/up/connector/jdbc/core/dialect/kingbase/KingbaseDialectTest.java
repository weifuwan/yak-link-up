package com.link.up.connector.jdbc.core.dialect.kingbase;

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

public class KingbaseDialectTest {

    @Test
    public void loadsKingbaseDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null, baseUrl(), null));
        assertEquals(DatabaseIdentifier.KINGBASE, dialect.name());
    }

    @Test
    public void parsesUnquotedIdentifiersAsLowercaseAndPreservesQuotedCase() {
        KingbaseDialect dialect = dialect("target");
        assertEquals(
                TablePath.of(null, "app", "orders"),
                dialect.parseTablePath("APP.ORDERS"));
        assertEquals(
                TablePath.of(null, "App", "Orders"),
                dialect.parseTablePath("\"App\".\"Orders\""));
    }

    @Test
    public void foreignSourceSchemaDoesNotLeakIntoKingbaseTarget() {
        KingbaseDialect dialect = dialect("target");
        assertEquals(
                "\"target\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of("source_db", "public", "orders")));
    }

    @Test
    public void currentSchemaFromUrlIsUsedForUnqualifiedTables() {
        KingbaseDialect dialect = new KingbaseDialect(config(
                null,
                baseUrl() + "?currentSchema=app",
                null));
        assertEquals(
                "\"app\".\"orders\"",
                dialect.tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void explicitPropertiesCurrentSchemaOverridesUrlCurrentSchema() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("currentSchema", "target");
        KingbaseDialect dialect = new KingbaseDialect(config(
                null,
                baseUrl() + "?currentSchema=app",
                properties));
        assertEquals(
                "\"target\".\"orders\"",
                dialect.tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void buildsPrimaryKeyScopedOnConflictUpsert() {
        String sql = dialect("app").buildUpsertSql(
                TablePath.of(null, "app", "users"),
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
                TablePath.of(null, "app", "users"),
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
    public void parsesNetworkAndShortKingbaseUrls() {
        String url = baseUrl() + "?currentSchema=app&ssl=true";
        assertEquals("TEST", KingbaseJdbcUrl.databaseName(url));
        assertEquals("app", KingbaseJdbcUrl.currentSchema(url, null));
        assertEquals("TEST", KingbaseJdbcUrl.databaseName("jdbc:kingbase8:TEST"));
    }

    private static KingbaseDialect dialect(String schema) {
        return new KingbaseDialect(config(schema, baseUrl(), null));
    }

    private static JdbcConnectionConfig config(
            String schema,
            String url,
            Map<String, String> properties) {

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.kingbase8.Driver");
        values.put("username", "system");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:kingbase8://127.0.0.1:54321/TEST";
    }
}
