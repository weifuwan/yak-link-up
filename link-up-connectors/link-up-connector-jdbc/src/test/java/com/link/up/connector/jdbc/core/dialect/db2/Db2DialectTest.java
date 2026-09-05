package com.link.up.connector.jdbc.core.dialect.db2;

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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Db2DialectTest {

    @Test
    public void loadsDb2DialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null, baseUrl(), null));
        assertEquals(DatabaseIdentifier.DB2, dialect.name());
    }

    @Test
    public void parsesUnquotedIdentifiersAsUppercaseAndPreservesQuotedCase() {
        Db2Dialect dialect = dialect("TARGET");
        assertEquals(
                TablePath.of(null, "APP", "ORDERS"),
                dialect.parseTablePath("app.orders"));
        assertEquals(
                TablePath.of(null, "app", "orders"),
                dialect.parseTablePath("\"app\".\"orders\""));
    }

    @Test
    public void foreignSourceSchemaDoesNotLeakIntoDb2Target() {
        Db2Dialect dialect = dialect("TARGET");
        assertEquals(
                "\"TARGET\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of("source_db", "public", "orders")));
    }

    @Test
    public void explicitTargetSchemaWins() {
        Db2Dialect dialect = dialect("TARGET");
        assertEquals(
                "\"APP\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of(null, "APP", "ORDERS")));
    }

    @Test
    public void currentSchemaFromUrlIsUsedForUnqualifiedTables() {
        Db2Dialect dialect = new Db2Dialect(config(
                null,
                baseUrl() + ":currentSchema=APP;sslConnection=false;",
                null));
        assertEquals(
                "\"APP\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void explicitPropertiesCurrentSchemaOverridesUrlCurrentSchema() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("currentSchema", "TARGET");
        Db2Dialect dialect = new Db2Dialect(config(
                null,
                baseUrl() + ":currentSchema=APP;",
                properties));
        assertEquals(
                "\"TARGET\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void buildsMergeWithOnlyNonPrimaryKeyUpdates() {
        String sql = dialect("APP").buildUpsertSql(
                TablePath.of(null, "APP", "USERS"),
                Arrays.asList("ID", "NAME"),
                Arrays.asList("ID")).get();
        assertTrue(sql.startsWith(
                "MERGE INTO \"APP\".\"USERS\" AS TARGET USING (VALUES (?, ?))"));
        assertTrue(sql.contains(
                "AS SOURCE (\"ID\", \"NAME\") ON TARGET.\"ID\" = SOURCE.\"ID\""));
        assertTrue(sql.contains(
                "WHEN MATCHED THEN UPDATE SET TARGET.\"NAME\" = SOURCE.\"NAME\""));
        assertFalse(sql.contains(
                "UPDATE SET TARGET.\"ID\""));
        assertTrue(sql.contains(
                "WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\")"));
    }

    @Test
    public void allPrimaryKeyMergeSkipsMatchedUpdate() {
        String sql = dialect("APP").buildUpsertSql(
                TablePath.of(null, "APP", "USERS"),
                Arrays.asList("ID"),
                Arrays.asList("ID")).get();
        assertFalse(sql.contains("WHEN MATCHED"));
        assertTrue(sql.contains("WHEN NOT MATCHED"));
    }

    @Test
    public void buildsNonNegativeHash8BucketPredicate() {
        Column column = Column.builder("CODE", BasicType.STRING_TYPE).build();
        assertEquals(
                "MOD(MOD(HASH8(\"CODE\", 0), 8) + 8, 8) = 3",
                dialect("APP").buildHashPartitionPredicate(column, 3, 8).get());
    }

    @Test
    public void parsesDatabaseAndCurrentSchemaBeforeDb2UrlProperties() {
        String url = baseUrl() + ":currentSchema=APP;sslConnection=false;";
        assertEquals("SAMPLE", Db2JdbcUrl.databaseName(url));
        assertEquals("APP", Db2JdbcUrl.currentSchema(url, Collections.<String, String>emptyMap()));
        assertEquals("SAMPLE", Db2JdbcUrl.databaseName("jdbc:db2:SAMPLE"));
    }

    private static Db2Dialect dialect(String schema) {
        return new Db2Dialect(config(schema, baseUrl(), null));
    }

    private static JdbcConnectionConfig config(
            String schema,
            String url,
            Map<String, String> properties) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.ibm.db2.jcc.DB2Driver");
        values.put("username", "db2inst1");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:db2://127.0.0.1:50000/SAMPLE";
    }
}
