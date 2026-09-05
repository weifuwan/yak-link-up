package com.link.up.connector.jdbc.core.dialect.iris;

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

public class IrisDialectTest {

    @Test
    public void loadsIrisDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null));
        assertEquals(DatabaseIdentifier.IRIS, dialect.name());
    }

    @Test
    public void parsesSchemaTableAndQuotedDots() {
        IrisDialect dialect = dialect("SQLUser");
        assertEquals(
                TablePath.of(null, "App", "Orders"),
                dialect.parseTablePath("App.Orders"));
        assertEquals(
                TablePath.of(null, "A.B", "Orders"),
                dialect.parseTablePath("\"A.B\".\"Orders\""));
    }

    @Test
    public void currentNamespaceThreePartPathIsAccepted() {
        assertEquals(
                TablePath.of("USER", "SQLUser", "Orders"),
                dialect("SQLUser").parseTablePath("USER.SQLUser.Orders"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void foreignNamespaceThreePartPathIsRejected() {
        dialect("SQLUser").parseTablePath("OTHER.SQLUser.Orders");
    }

    @Test
    public void connectorSchemaIsUsedForUnqualifiedTables() {
        assertEquals(
                "\"App\".\"Orders\"",
                dialect("App").tableIdentifier(TablePath.of("Orders")));
    }

    @Test
    public void sqlUserIsDefaultSchema() {
        assertEquals(
                "\"SQLUser\".\"Orders\"",
                new IrisDialect(config(null)).tableIdentifier(TablePath.of("Orders")));
    }

    @Test
    public void buildsInsertOrUpdateWithJdbcPlaceholders() {
        String sql = dialect("App").buildUpsertSql(
                TablePath.of("USER", "App", "Users"),
                Arrays.asList("Id", "Name"),
                Arrays.asList("Id")).get();

        assertEquals(
                "INSERT OR UPDATE \"App\".\"Users\" (\"Id\", \"Name\") VALUES (?, ?)",
                sql);
        assertFalse(sql.contains(":Id"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void upsertRequiresPrimaryKeyContract() {
        dialect("App").buildUpsertSql(
                TablePath.of("USER", "App", "Users"),
                Arrays.asList("Id", "Name"),
                Arrays.<String>asList());
    }

    @Test
    public void stringHashPartitionIsExplicitlyUnsupported() {
        Column column = Column.builder("Code", BasicType.STRING_TYPE).build();
        assertFalse(
                dialect("App")
                        .buildHashPartitionPredicate(column, 0, 4)
                        .isPresent());
    }

    @Test
    public void parsesNamespaceFromNetworkUrlAndIgnoresDriverArguments() {
        assertTrue(IrisJdbcUrl.accepts(baseUrl()));
        assertEquals("USER", IrisJdbcUrl.namespaceName(baseUrl()));
        assertEquals(
                "USER",
                IrisJdbcUrl.namespaceName(
                        "jdbc:IRIS://127.0.0.1:1972/USER/UTF-8/1"));
    }

    private static IrisDialect dialect(String schema) {
        return new IrisDialect(config(schema));
    }

    private static JdbcConnectionConfig config(String schema) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", baseUrl());
        values.put("driver", "com.intersystems.jdbc.IRISDriver");
        values.put("username", "_SYSTEM");
        if (schema != null) {
            values.put("schema", schema);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:IRIS://127.0.0.1:1972/USER";
    }
}
