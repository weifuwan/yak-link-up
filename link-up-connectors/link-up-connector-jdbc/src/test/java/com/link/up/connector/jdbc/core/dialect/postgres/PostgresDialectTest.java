package com.link.up.connector.jdbc.core.dialect.postgres;

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
import static org.junit.Assert.assertTrue;

public class PostgresDialectTest {

    @Test
    public void usesPublicSchemaForUnqualifiedTable() {
        PostgresDialect dialect =
                dialect(
                        "jdbc:postgresql://127.0.0.1:5432/app",
                        null);

        assertEquals(
                "\"public\".\"users\"",
                dialect.tableIdentifier(
                        TablePath.of("users")));
    }

    @Test
    public void configuredSchemaTakesPrecedence() {
        PostgresDialect dialect =
                dialect(
                        "jdbc:postgresql://127.0.0.1:5432/app",
                        "analytics");

        assertEquals(
                "\"analytics\".\"events\"",
                dialect.tableIdentifier(
                        TablePath.of("events")));
    }

    @Test
    public void twoPartTablePathMeansSchemaAndTable() {
        PostgresDialect dialect =
                dialect(
                        "jdbc:postgresql://127.0.0.1:5432/app",
                        null);

        assertEquals(
                TablePath.of(
                        null,
                        "sales",
                        "orders"),
                dialect.parseTablePath(
                        "sales.orders"));
    }

    @Test
    public void buildsPostgresUpsert() {
        PostgresDialect dialect =
                dialect(
                        "jdbc:postgresql://127.0.0.1:5432/app",
                        null);

        assertEquals(
                "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES (?, ?) "
                        + "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"",
                dialect.buildUpsertSql(
                        TablePath.of("users"),
                        Arrays.asList(
                                "id",
                                "name"),
                        Arrays.asList(
                                "id"))
                        .get());
    }

    @Test
    public void allPrimaryKeyFieldsUseDoNothing() {
        PostgresDialect dialect =
                dialect(
                        "jdbc:postgresql://127.0.0.1:5432/app",
                        null);

        assertEquals(
                "INSERT INTO \"public\".\"users\" (\"id\") VALUES (?) "
                        + "ON CONFLICT (\"id\") DO NOTHING",
                dialect.buildUpsertSql(
                        TablePath.of("users"),
                        Arrays.asList(
                                "id"),
                        Arrays.asList(
                                "id"))
                        .get());
    }

    @Test
    public void buildsHashPartitionPredicate() {
        PostgresDialect dialect =
                dialect(
                        "jdbc:postgresql://127.0.0.1:5432/app",
                        null);

        Column column =
                Column.builder(
                        "id",
                        BasicType.LONG_TYPE)
                        .build();

        assertEquals(
                "MOD(ABS(HASHTEXT(CAST(\"id\" AS TEXT))::BIGINT), 4) = 2",
                dialect.buildHashPartitionPredicate(
                        column,
                        2,
                        4)
                        .get());
    }

    @Test
    public void loaderDiscoversPostgresDialectByUrl() {
        JdbcConnectionConfig config =
                connectionConfig(
                        "jdbc:postgresql://127.0.0.1:5432/app",
                        null);

        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        config);

        assertEquals(
                DatabaseIdentifier.POSTGRESQL,
                dialect.name());

        assertTrue(
                dialect.defaultConnectionProperties()
                        .containsKey(
                                "reWriteBatchedInserts"));
    }

    private static PostgresDialect dialect(
            String url,
            String schema) {

        return new PostgresDialect(
                connectionConfig(
                        url,
                        schema));
    }

    private static JdbcConnectionConfig connectionConfig(
            String url,
            String schema) {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put(
                "url",
                url);

        values.put(
                "driver",
                "org.postgresql.Driver");

        if (schema != null) {
            values.put(
                    "schema",
                    schema);
        }

        return JdbcConnectionConfig.of(
                ReadonlyConfig.fromMap(
                        values));
    }
}
