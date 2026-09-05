package com.link.up.connector.jdbc.core.dialect.db2;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Db2DialectTest {

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
    public void parsesDatabaseBeforeDb2UrlProperties() {
        assertEquals(
                "SAMPLE",
                Db2JdbcUrl.databaseName(
                        "jdbc:db2://127.0.0.1:50000/SAMPLE:currentSchema=APP;sslConnection=false;"));
        assertEquals(
                "SAMPLE",
                Db2JdbcUrl.databaseName("jdbc:db2:SAMPLE"));
    }

    private static Db2Dialect dialect(String schema) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:db2://127.0.0.1:50000/SAMPLE");
        values.put("driver", "com.ibm.db2.jcc.DB2Driver");
        values.put("username", "db2inst1");
        if (schema != null) {
            values.put("schema", schema);
        }
        return new Db2Dialect(
                JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values)));
    }
}
