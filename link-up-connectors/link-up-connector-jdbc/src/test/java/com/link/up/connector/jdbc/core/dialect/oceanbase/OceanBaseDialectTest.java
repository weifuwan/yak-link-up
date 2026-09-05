package com.link.up.connector.jdbc.core.dialect.oceanbase;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.mysql.MySqlTypeMapper;
import com.link.up.connector.jdbc.core.dialect.oracle.OracleTypeMapper;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OceanBaseDialectTest {

    @Test
    public void mysqlModeUsesOceanBaseIdentityAndMySqlSql() {
        OceanBaseDialect dialect =
                new OceanBaseDialect(
                        config(
                                "mysql",
                                "APP"));

        assertEquals(
                DatabaseIdentifier.OCEANBASE,
                dialect.name());

        assertTrue(
                dialect.typeMapper()
                        instanceof MySqlTypeMapper);

        assertEquals(
                "`target_db`.`orders`",
                dialect.tableIdentifier(
                        TablePath.of(
                                "source_db",
                                "orders")));

        String upsert =
                dialect.buildUpsertSql(
                                TablePath.of(
                                        "source_db",
                                        "orders"),
                                Arrays.asList(
                                        "id",
                                        "name"),
                                Arrays.asList(
                                        "id"))
                        .get();

        assertTrue(
                upsert.contains(
                        "ON DUPLICATE KEY UPDATE"));

        assertTrue(
                upsert.contains(
                        "VALUES(`name`)"));

        assertEquals(
                "oceanbase",
                dialect.rowConverter()
                        .name());
    }

    @Test
    public void oracleModeUsesOracleSqlAndConfiguredSchema() {
        OceanBaseDialect dialect =
                new OceanBaseDialect(
                        config(
                                "oracle",
                                "APP"));

        assertTrue(
                dialect.typeMapper()
                        instanceof OracleTypeMapper);

        assertEquals(
                "\"APP\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of(
                                "source_db",
                                "OTHER",
                                "orders")));

        String upsert =
                dialect.buildUpsertSql(
                                TablePath.of(
                                        "source_db",
                                        "OTHER",
                                        "orders"),
                                Arrays.asList(
                                        "id",
                                        "name"),
                                Arrays.asList(
                                        "id"))
                        .get();

        assertTrue(
                upsert.startsWith(
                        "MERGE INTO \"APP\".\"orders\""));

        assertTrue(
                upsert.contains(
                        "FROM DUAL"));

        assertTrue(
                upsert.contains(
                        "WHEN MATCHED THEN UPDATE SET"));
    }

    @Test
    public void hashSplitFollowsCompatibleMode() {
        Column column =
                Column.builder(
                                "id",
                                BasicType.INT_TYPE)
                        .build();

        String mysql =
                new OceanBaseDialect(
                        config(
                                "mysql",
                                "APP"))
                        .buildHashPartitionPredicate(
                                column,
                                1,
                                4)
                        .get();

        assertEquals(
                "MOD(CRC32(CAST(`id` AS CHAR)), 4) = 1",
                mysql);

        String oracle =
                new OceanBaseDialect(
                        config(
                                "oracle",
                                "APP"))
                        .buildHashPartitionPredicate(
                                column,
                                1,
                                4)
                        .get();

        assertEquals(
                "MOD(ORA_HASH(\"id\"), 4) = 1",
                oracle);
    }

    @Test
    public void compatibleModeIsRequired() {
        try {
            new OceanBaseDialect(
                    config(
                            null,
                            "APP"));
            fail(
                    "missing compatible mode must fail");
        } catch (IllegalArgumentException e) {
            assertTrue(
                    e.getMessage()
                            .contains(
                                    "compatible_mode"));
        }
    }

    @Test
    public void urlParserKeepsDatabaseAndCanTranslateMysqlScheme() {
        String url =
                "jdbc:oceanbase://127.0.0.1:2881/app"
                        + "?useUnicode=true";

        assertEquals(
                "app",
                OceanBaseJdbcUrl.databaseName(
                        url));

        assertEquals(
                "jdbc:mysql://127.0.0.1:2881/app"
                        + "?useUnicode=true",
                OceanBaseJdbcUrl.toMySqlUrl(
                        url));
    }

    private static JdbcConnectionConfig config(
            String compatibleMode,
            String schema) {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put(
                "url",
                "jdbc:oceanbase://127.0.0.1:2881/target_db");

        values.put(
                "driver",
                "com.oceanbase.jdbc.Driver");

        values.put(
                "username",
                "app");

        values.put(
                "schema",
                schema);

        if (compatibleMode != null) {
            values.put(
                    "compatible_mode",
                    compatibleMode);
        }

        return JdbcConnectionConfig.of(
                ReadonlyConfig.fromMap(
                        values));
    }
}
