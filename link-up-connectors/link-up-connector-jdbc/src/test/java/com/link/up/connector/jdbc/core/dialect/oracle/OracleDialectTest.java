package com.link.up.connector.jdbc.core.dialect.oracle;

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

public class OracleDialectTest {

    @Test
    public void parsesTwoPartPathAsSchemaTable() {
        OracleDialect dialect =
                dialect(
                        "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1",
                        "app",
                        null);

        TablePath path =
                dialect.parseTablePath(
                        "HR.EMPLOYEES");

        assertEquals(
                "HR",
                path.getSchemaName());

        assertEquals(
                "EMPLOYEES",
                path.getTableName());
    }

    @Test
    public void usesConfiguredSchemaForForeignSourcePath() {
        OracleDialect dialect =
                dialect(
                        "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1",
                        "app",
                        "TARGET");

        assertEquals(
                "\"TARGET\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of(
                                "source_db",
                                "source_schema",
                                "orders")));
    }

    @Test
    public void defaultsSchemaToUppercaseUsername() {
        OracleDialect dialect =
                dialect(
                        "jdbc:oracle:thin:@127.0.0.1:1521:ORCL",
                        "app",
                        null);

        assertEquals(
                "\"APP\".\"orders\"",
                dialect.tableIdentifier(
                        TablePath.of(
                                "orders")));
    }

    @Test
    public void explicitOracleSchemaWins() {
        OracleDialect dialect =
                dialect(
                        "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1",
                        "app",
                        "TARGET");

        assertEquals(
                "\"HR\".\"EMPLOYEES\"",
                dialect.tableIdentifier(
                        TablePath.of(
                                null,
                                "HR",
                                "EMPLOYEES")));
    }

    @Test
    public void buildsMergeUpsertWithPositionalParameters() {
        OracleDialect dialect =
                dialect(
                        "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1",
                        "app",
                        "APP");

        String sql =
                dialect.buildUpsertSql(
                        TablePath.of(
                                null,
                                "APP",
                                "users"),
                        Arrays.asList(
                                "id",
                                "name"),
                        Arrays.asList(
                                "id"))
                        .get();

        assertTrue(
                sql.startsWith(
                        "MERGE INTO \"APP\".\"users\" TARGET"));

        assertTrue(
                sql.contains(
                        "SELECT ? AS \"id\", ? AS \"name\" FROM DUAL"));

        assertTrue(
                sql.contains(
                        "WHEN MATCHED THEN UPDATE SET"));

        assertTrue(
                sql.contains(
                        "WHEN NOT MATCHED THEN INSERT"));
    }

    @Test
    public void allPrimaryKeyMergeOmitsUpdateClause() {
        OracleDialect dialect =
                dialect(
                        "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1",
                        "app",
                        "APP");

        String sql =
                dialect.buildUpsertSql(
                        TablePath.of(
                                null,
                                "APP",
                                "users"),
                        Arrays.asList(
                                "id"),
                        Arrays.asList(
                                "id"))
                        .get();

        assertFalse(
                sql.contains(
                        "WHEN MATCHED THEN UPDATE"));

        assertTrue(
                sql.contains(
                        "WHEN NOT MATCHED THEN INSERT"));
    }

    @Test
    public void buildsOracleHashPredicate() {
        OracleDialect dialect =
                dialect(
                        "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1",
                        "app",
                        "APP");

        assertEquals(
                "MOD(ORA_HASH(\"id\"), 4) = 2",
                dialect.buildHashPartitionPredicate(
                        Column.builder(
                                "id",
                                BasicType.INT_TYPE)
                                .build(),
                        2,
                        4)
                        .get());
    }

    @Test
    public void parsesOracleServiceAndSidUrls() {
        assertEquals(
                "FREEPDB1",
                OracleJdbcUrl.databaseName(
                        "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1"));

        assertEquals(
                "ORCL",
                OracleJdbcUrl.databaseName(
                        "jdbc:oracle:thin:@127.0.0.1:1521:ORCL"));
    }

    private static OracleDialect dialect(
            String url,
            String username,
            String schema) {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put(
                "url",
                url);

        values.put(
                "driver",
                "oracle.jdbc.OracleDriver");

        values.put(
                "username",
                username);

        if (schema != null) {
            values.put(
                    "schema",
                    schema);
        }

        JdbcConnectionConfig config =
                JdbcConnectionConfig.of(
                        ReadonlyConfig.fromMap(
                                values));

        return new OracleDialect(
                config);
    }
}
