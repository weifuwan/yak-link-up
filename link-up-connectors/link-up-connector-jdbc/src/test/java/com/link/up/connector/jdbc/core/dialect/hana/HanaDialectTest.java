package com.link.up.connector.jdbc.core.dialect.hana;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.WritableCatalog;
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

public class HanaDialectTest {

    @Test
    public void loadsHanaDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null, baseUrl(), null));
        assertEquals(DatabaseIdentifier.HANA, dialect.name());
    }

    @Test
    public void parsesSchemaTableAndPreservesQuotedCase() {
        HanaDialect dialect = dialect("SALES");
        assertEquals(
                TablePath.of(null, "SALES", "ORDERS"),
                dialect.parseTablePath("sales.orders"));
        assertEquals(
                TablePath.of(null, "Sales", "Order.Items"),
                dialect.parseTablePath("\"Sales\".\"Order.Items\""));
    }

    @Test
    public void parsesThreePartPathWhenDatabaseNameIsKnown() {
        HanaDialect dialect = dialect("SALES");
        assertEquals(
                TablePath.of("HXE", "SALES", "ORDERS"),
                dialect.parseTablePath("hxe.sales.orders"));
    }

    @Test
    public void configuredSchemaQualifiesUnqualifiedTable() {
        assertEquals(
                "\"SALES\".\"ORDERS\"",
                dialect("SALES").tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void explicitSchemaWinsOverDefaultSchema() {
        assertEquals(
                "\"ARCHIVE\".\"ORDERS\"",
                dialect("SALES").tableIdentifier(
                        TablePath.of(null, "ARCHIVE", "ORDERS")));
    }

    @Test
    public void currentSchemaUrlPropertyWinsOverConnectorSchema() {
        HanaDialect dialect = new HanaDialect(config(
                "SALES",
                baseUrl() + "&currentSchema=ARCHIVE",
                null));
        assertEquals(
                "\"ARCHIVE\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
        assertTrue(dialect.defaultConnectionProperties().isEmpty());
    }

    @Test
    public void connectorSchemaBecomesDefaultCurrentSchemaProperty() {
        HanaDialect dialect = dialect("SALES");
        assertEquals(
                "SALES",
                dialect.defaultConnectionProperties().get("currentSchema"));
    }

    @Test
    public void stageTwoCatalogIsWritableAndMergeUsesOneMarkerPerField() {
        HanaDialect dialect = dialect("SALES");
        Catalog catalog = dialect.createCatalog(config("SALES", baseUrl(), null));
        assertTrue(catalog instanceof WritableCatalog);

        String sql = dialect.buildUpsertSql(
                TablePath.of(null, "SALES", "ORDERS"),
                Arrays.asList("ID", "NAME", "AMOUNT"),
                Arrays.asList("ID")).get();

        assertTrue(sql.startsWith(
                "MERGE INTO \"SALES\".\"ORDERS\" AS TARGET USING (SELECT "));
        assertTrue(sql.contains(
                "? AS \"ID\", ? AS \"NAME\", ? AS \"AMOUNT\" FROM DUMMY"));
        assertTrue(sql.contains(
                "ON TARGET.\"ID\" = SOURCE.\"ID\""));
        assertTrue(sql.contains(
                "WHEN MATCHED THEN UPDATE SET TARGET.\"NAME\" = SOURCE.\"NAME\""));
        assertTrue(sql.contains(
                "WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\", \"AMOUNT\")"));
        assertEquals(3, count(sql, '?'));
    }

    @Test
    public void allPrimaryKeyMergeSkipsMatchedUpdate() {
        String sql = dialect("SALES").buildUpsertSql(
                TablePath.of(null, "SALES", "ORDERS"),
                Arrays.asList("ID"),
                Arrays.asList("ID")).get();
        assertFalse(sql.contains("WHEN MATCHED"));
        assertTrue(sql.contains("WHEN NOT MATCHED"));
    }

    @Test
    public void parsesDatabaseNameAndCurrentSchemaFromUrl() {
        assertEquals("HXE", HanaJdbcUrl.databaseName(baseUrl()));
        assertEquals(
                "Sales Space",
                HanaJdbcUrl.currentSchema(
                        baseUrl() + "&currentSchema=Sales%20Space",
                        null,
                        null));
    }

    private static int count(String value, char target) {
        int result = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == target) {
                result++;
            }
        }
        return result;
    }

    private static HanaDialect dialect(String schema) {
        return new HanaDialect(config(schema, baseUrl(), null));
    }

    private static JdbcConnectionConfig config(
            String schema,
            String url,
            Map<String, String> properties) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.sap.db.jdbc.Driver");
        values.put("username", "SYSTEM");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:sap://127.0.0.1:30013/?databaseName=HXE";
    }
}
