package com.link.up.connector.jdbc.core.dialect.yashandb;

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

public class YashanDbDialectTest {

    @Test
    public void loadsYashanDbDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null));
        assertEquals(DatabaseIdentifier.YASHANDB, dialect.name());
    }

    @Test
    public void parsesUnquotedIdentifiersAsUppercaseAndPreservesQuotedCase() {
        YashanDbDialect dialect = dialect("TARGET");
        assertEquals(
                TablePath.of(null, "APP", "ORDERS"),
                dialect.parseTablePath("app.orders"));
        assertEquals(
                TablePath.of(null, "App", "Orders"),
                dialect.parseTablePath("\"App\".\"Orders\""));
    }

    @Test
    public void foreignSourceSchemaDoesNotLeakIntoYashanTarget() {
        YashanDbDialect dialect = dialect("TARGET");
        assertEquals(
                "\"TARGET\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of("source_db", "public", "orders")));
    }

    @Test
    public void usernameIsDefaultSchemaWhenExplicitSchemaIsMissing() {
        YashanDbDialect dialect = new YashanDbDialect(config(null));
        assertEquals(
                "\"APPUSER\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void buildsPrimaryKeyScopedMergeUpsertWithJdbcPlaceholders() {
        String sql = dialect("APP").buildUpsertSql(
                TablePath.of(null, "APP", "USERS"),
                Arrays.asList("ID", "NAME"),
                Arrays.asList("ID")).get();

        assertTrue(sql.startsWith(
                "MERGE INTO \"APP\".\"USERS\" TARGET USING "
                        + "(SELECT ? AS \"ID\", ? AS \"NAME\" FROM DUAL) SOURCE"));
        assertTrue(sql.contains(
                "ON (TARGET.\"ID\" = SOURCE.\"ID\")"));
        assertTrue(sql.contains(
                "WHEN MATCHED THEN UPDATE SET TARGET.\"NAME\" = SOURCE.\"NAME\""));
        assertTrue(sql.contains(
                "WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\") "
                        + "VALUES (SOURCE.\"ID\", SOURCE.\"NAME\")"));
        assertFalse(sql.contains(":ID"));
    }

    @Test
    public void allPrimaryKeyMergeOmitsMatchedUpdate() {
        String sql = dialect("APP").buildUpsertSql(
                TablePath.of(null, "APP", "USERS"),
                Arrays.asList("ID"),
                Arrays.asList("ID")).get();
        assertFalse(sql.contains("WHEN MATCHED"));
        assertTrue(sql.contains("WHEN NOT MATCHED"));
    }

    @Test
    public void buildsOraHashBucketPredicate() {
        Column column = Column.builder("CODE", BasicType.STRING_TYPE).build();
        assertEquals(
                "MOD(ORA_HASH(\"CODE\"), 8) = 3",
                dialect("APP")
                        .buildHashPartitionPredicate(column, 3, 8)
                        .get());
    }

    @Test
    public void parsesLogicalDatabaseTokenFromUrl() {
        String url = baseUrl() + "?connectTimeout=3000";
        assertEquals("YASDB", YashanDbJdbcUrl.databaseName(url));
        assertTrue(YashanDbJdbcUrl.accepts(url));
    }

    private static YashanDbDialect dialect(String schema) {
        return new YashanDbDialect(config(schema));
    }

    private static JdbcConnectionConfig config(String schema) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", baseUrl());
        values.put("driver", "com.yashandb.jdbc.Driver");
        values.put("username", "appuser");
        if (schema != null) {
            values.put("schema", schema);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:yasdb://127.0.0.1:1688/YASDB";
    }
}
