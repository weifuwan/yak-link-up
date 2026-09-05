package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.dameng.DamengDialect;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcDamengTargetPathTest {

    @Test
    public void foreignSourceDatabaseAndSchemaDoNotLeakIntoDamengTarget() {
        JdbcConnectionConfig config = config("TARGET", baseUrl(), null, "SYSDBA");
        assertEquals(
                TablePath.of(null, "TARGET", "orders"),
                JdbcCreateTableSqlResolver.resolveTargetPath(
                        config,
                        TablePath.of("source_db", "public", "orders")));
    }

    @Test
    public void explicitTargetSchemaTableIsPreserved() {
        JdbcConnectionConfig config = config("TARGET", baseUrl(), null, "SYSDBA");
        assertEquals(
                TablePath.of(null, "APP", "ORDERS"),
                JdbcCreateTableSqlResolver.resolveTargetPath(
                        config,
                        TablePath.of(null, "APP", "ORDERS")));
    }

    @Test
    public void urlSchemaIsUsedWhenConnectorSchemaIsAbsent() {
        JdbcConnectionConfig config = config(null, baseUrl() + "/APP", null, "SYSDBA");
        assertEquals(
                TablePath.of(null, "APP", "ORDERS"),
                JdbcCreateTableSqlResolver.resolveTargetPath(
                        config,
                        TablePath.of("mysql_db", "source", "ORDERS")));
    }

    @Test
    public void resolverBuildsDamengTargetDdl() {
        JdbcConnectionConfig config = config("APP", baseUrl(), null, "SYSDBA");
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("ID", BasicType.LONG_TYPE).nullable(false).build())
                .build();
        CatalogTable source = CatalogTable.builder(
                TablePath.of("source_db", "public", "USERS"),
                schema).build();

        String ddl = JdbcCreateTableSqlResolver.resolve(
                new DamengDialect(config),
                config,
                source);
        assertTrue(ddl.contains("CREATE TABLE \"APP\".\"USERS\""));
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
