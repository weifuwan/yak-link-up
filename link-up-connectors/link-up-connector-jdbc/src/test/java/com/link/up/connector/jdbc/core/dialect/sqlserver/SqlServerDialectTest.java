package com.link.up.connector.jdbc.core.dialect.sqlserver;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SqlServerDialectTest {

    @Test
    public void parsesSchemaTableAndUsesTargetDatabase() {
        SqlServerDialect dialect = dialect("dbo");
        assertEquals(
                TablePath.of(null, "sales", "orders"),
                dialect.parseTablePath("sales.orders"));
        assertEquals(
                "[app].[sales].[orders]",
                dialect.tableIdentifier(TablePath.of(null, "sales", "orders")));
    }

    @Test
    public void foreignSourceDatabaseDoesNotLeakIntoTarget() {
        SqlServerDialect dialect = dialect("warehouse");
        assertEquals(
                "[app].[warehouse].[orders]",
                dialect.tableIdentifier(TablePath.of("source_db", "public", "orders")));
    }

    @Test
    public void escapesBracketIdentifiers() {
        assertEquals("[a]]b]", dialect(null).quoteIdentifier("a]b"));
    }

    @Test
    public void buildsMergeWithJdbcPlaceholders() {
        String sql = dialect("dbo").buildUpsertSql(
                TablePath.of(null, "dbo", "orders"),
                Arrays.asList("id", "name"),
                Arrays.asList("id")).get();
        assertTrue(sql.contains("MERGE INTO [app].[dbo].[orders] AS TARGET"));
        assertTrue(sql.contains("USING (VALUES (?, ?))"));
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET TARGET.[name] = SOURCE.[name]"));
        assertTrue(sql.endsWith(";"));
    }

    @Test
    public void allPrimaryKeyMergeSkipsUpdate() {
        String sql = dialect("dbo").buildUpsertSql(
                TablePath.of("orders"), Arrays.asList("id"), Arrays.asList("id")).get();
        assertFalse(sql.contains("WHEN MATCHED"));
        assertTrue(sql.contains("WHEN NOT MATCHED"));
    }

    @Test
    public void hashPredicateUsesBigintChecksum() {
        String predicate = dialect(null).buildHashPartitionPredicate(
                Column.builder("id", BasicType.INT_TYPE).build(), 1, 4).get();
        assertEquals(
                "(ABS(CAST(CHECKSUM(CAST([id] AS NVARCHAR(4000))) AS BIGINT)) % 4) = 1",
                predicate);
    }

    private static SqlServerDialect dialect(String schema) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:sqlserver://127.0.0.1:1433;databaseName=app;encrypt=false");
        values.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        if (schema != null) {
            values.put("schema", schema);
        }
        return new SqlServerDialect(
                JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values)));
    }
}
