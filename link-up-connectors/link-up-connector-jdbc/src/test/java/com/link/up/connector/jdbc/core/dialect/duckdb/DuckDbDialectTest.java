package com.link.up.connector.jdbc.core.dialect.duckdb;

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

public class DuckDbDialectTest {

    @Test
    public void loadsDuckDbDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(fileUrl(), null, null));
        assertEquals(DatabaseIdentifier.DUCKDB, dialect.name());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsAnonymousMemoryUrl() {
        new DuckDbDialect(config("jdbc:duckdb:", null, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsExplicitPrivateMemoryUrl() {
        new DuckDbDialect(config("jdbc:duckdb:memory:", null, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnpinnedNamedMemoryUrl() {
        new DuckDbDialect(config("jdbc:duckdb:memory:shared", null, null));
    }

    @Test
    public void acceptsPinnedNamedMemoryUrl() {
        String url = "jdbc:duckdb:memory:shared;jdbc_pin_db=true";
        DuckDbDialect dialect = new DuckDbDialect(config(url, null, null));
        assertEquals(DatabaseIdentifier.DUCKDB, dialect.name());
        assertEquals("memory", DuckDbJdbcUrl.databaseName(url));
    }

    @Test
    public void acceptsNamedMemoryPinnedThroughProperties() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("jdbc_pin_db", "true");
        DuckDbDialect dialect = new DuckDbDialect(
                config("jdbc:duckdb:memory:shared", null, properties));
        assertEquals(DatabaseIdentifier.DUCKDB, dialect.name());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDisabledInstanceCacheFromUrl() {
        new DuckDbDialect(config(
                fileUrl() + ";jdbc_instance_cache=false",
                null,
                null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDisabledInstanceCacheFromProperties() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("jdbc_instance_cache", "false");
        new DuckDbDialect(config(fileUrl(), null, properties));
    }

    @Test
    public void urlOptionOverridesPropertiesForInstanceCacheAndSchema() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("jdbc_instance_cache", "false");
        properties.put("schema", "from_properties");
        String url = fileUrl()
                + ";jdbc_instance_cache=true;schema=from_url";

        DuckDbDialect dialect = new DuckDbDialect(config(url, null, properties));
        assertEquals(
                "\"from_url\".\"orders\"",
                dialect.tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void parsesFileDatabaseNameAndOptions() {
        String url = fileUrl() + ";threads=4;schema=analytics";
        assertEquals("warehouse", DuckDbJdbcUrl.databaseName(url));
        assertEquals("analytics", DuckDbJdbcUrl.configuredSchema(url, null));
    }

    @Test
    public void preservesIdentifierSpellingAndQuotedDots() {
        DuckDbDialect dialect = dialect("main");
        assertEquals(
                TablePath.of(null, "App", "Orders"),
                dialect.parseTablePath("App.Orders"));
        assertEquals(
                TablePath.of(null, "A.B", "Orders"),
                dialect.parseTablePath("\"A.B\".\"Orders\""));
    }

    @Test
    public void foreignSourceSchemaDoesNotLeakIntoDuckDbTarget() {
        DuckDbDialect dialect = dialect("target");
        assertEquals(
                "\"target\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of("source_db", "public", "orders")));
    }

    @Test
    public void buildsPrimaryKeyScopedOnConflictUpsert() {
        String sql = dialect("app").buildUpsertSql(
                TablePath.of("warehouse", "app", "users"),
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
                TablePath.of("warehouse", "app", "users"),
                Arrays.asList("id"),
                Arrays.asList("id")).get();
        assertTrue(sql.endsWith("ON CONFLICT (\"id\") DO NOTHING"));
    }

    @Test
    public void buildsUnsignedHashBucketPredicate() {
        Column column = Column.builder("code", BasicType.STRING_TYPE).build();
        assertEquals(
                "MOD(HASH(CAST(\"code\" AS VARCHAR)), 8) = 3",
                dialect("main")
                        .buildHashPartitionPredicate(column, 3, 8)
                        .get());
    }

    private static DuckDbDialect dialect(String schema) {
        return new DuckDbDialect(config(fileUrl(), schema, null));
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
