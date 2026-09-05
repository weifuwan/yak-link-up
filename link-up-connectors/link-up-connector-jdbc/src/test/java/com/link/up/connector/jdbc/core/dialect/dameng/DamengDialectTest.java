package com.link.up.connector.jdbc.core.dialect.dameng;

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

public class DamengDialectTest {

    @Test
    public void loadsDamengDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null, baseUrl(), null, "SYSDBA"));
        assertEquals(DatabaseIdentifier.DAMENG, dialect.name());
    }

    @Test
    public void parsesUnquotedIdentifiersAsUppercaseAndPreservesQuotedCase() {
        DamengDialect dialect = dialect("TARGET");
        assertEquals(
                TablePath.of(null, "APP", "ORDERS"),
                dialect.parseTablePath("app.orders"));
        assertEquals(
                TablePath.of(null, "app", "orders"),
                dialect.parseTablePath("\"app\".\"orders\""));
    }

    @Test
    public void urlPathSchemaIsUsedForUnqualifiedTables() {
        DamengDialect dialect = new DamengDialect(
                config(null, baseUrl() + "/APP", null, "SYSDBA"));
        assertEquals(
                "\"APP\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void schemaPropertyOverridesUrlSchema() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("schema", "TARGET");
        DamengDialect dialect = new DamengDialect(
                config(null, baseUrl() + "/APP?logLevel=1", properties, "SYSDBA"));
        assertEquals(
                "\"TARGET\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void usernameIsDefaultSchemaWhenNoSchemaIsConfigured() {
        DamengDialect dialect = new DamengDialect(
                config(null, baseUrl(), null, "app_user"));
        assertEquals(
                "\"APP_USER\".\"ORDERS\"",
                dialect.tableIdentifier(TablePath.of("ORDERS")));
    }

    @Test
    public void explicitSchemaBecomesDefaultConnectionProperty() {
        assertEquals(
                "TARGET",
                dialect("TARGET").defaultConnectionProperties().get("schema"));
    }

    @Test
    public void buildsNativeMergeWithOnlyNonPrimaryKeyUpdates() {
        String sql = dialect("APP").buildUpsertSql(
                TablePath.of(null, "APP", "USERS"),
                Arrays.asList("ID", "NAME"),
                Arrays.asList("ID")).get();

        assertTrue(sql.startsWith(
                "MERGE INTO \"APP\".\"USERS\" TARGET USING (SELECT ? \"ID\", ? \"NAME\") SOURCE"));
        assertTrue(sql.contains(
                "ON (TARGET.\"ID\" = SOURCE.\"ID\")"));
        assertTrue(sql.contains(
                "WHEN MATCHED THEN UPDATE SET TARGET.\"NAME\" = SOURCE.\"NAME\""));
        assertFalse(sql.contains("UPDATE SET TARGET.\"ID\""));
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
    public void buildsOraHashBucketPredicate() {
        Column column = Column.builder("CODE", BasicType.STRING_TYPE).build();
        assertEquals(
                "ORA_HASH(\"CODE\", 7) = 3",
                dialect("APP").buildHashPartitionPredicate(column, 3, 8).get());
    }

    @Test
    public void parsesSchemaFromUrlAndProperties() {
        assertEquals(
                "APP",
                DamengJdbcUrl.schema(baseUrl() + "/APP?logLevel=1", null));
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("SCHEMA", "TARGET");
        assertEquals(
                "TARGET",
                DamengJdbcUrl.schema(baseUrl() + "/APP?schema=QUERY", properties));
    }

    private static DamengDialect dialect(String schema) {
        return new DamengDialect(config(schema, baseUrl(), null, "SYSDBA"));
    }

    private static JdbcConnectionConfig config(
            String schema,
            String url,
            Map<String, String> properties,
            String username) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "dm.jdbc.driver.DmDriver");
        values.put("username", username);
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:dm://127.0.0.1:5236";
    }
}
