package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8cDialectTest {

    @Test
    public void loadsDedicatedGBase8cDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(null, baseUrl(), null, null));
        assertEquals(DatabaseIdentifier.GBASE8C, dialect.name());
    }

    @Test
    public void factoryOnlyAcceptsDedicatedGBase8cProtocol() {
        GBase8cDialectFactory factory = new GBase8cDialectFactory();
        assertTrue(factory.acceptsUrl(baseUrl()));
        assertFalse(factory.acceptsUrl("jdbc:postgresql://127.0.0.1:5432/app"));
        assertFalse(factory.acceptsUrl("jdbc:gbase://127.0.0.1:5258/app"));
        assertFalse(factory.acceptsUrl("jdbc:mysql://127.0.0.1:3306/app"));
    }

    @Test
    public void explicitDialectLoadsDedicatedGBase8cIdentity() {
        JdbcDialect dialect = JdbcDialectLoader.load(
                config(null, baseUrl(), null, DatabaseIdentifier.GBASE8C));
        assertEquals(DatabaseIdentifier.GBASE8C, dialect.name());
    }

    @Test
    public void jdbcUrlHelperExtractsDatabaseWithoutQueryProperties() {
        assertEquals("app", GBase8cJdbcUrl.databaseName(baseUrl()));
        assertEquals(
                "archive",
                GBase8cJdbcUrl.databaseName(
                        "jdbc:gbase8c://127.0.0.1:5432/archive?loggerLevel=warning"));
    }

    @Test
    public void parsesPgCompatibleSchemaTablePaths() {
        GBase8cDialect dialect = dialect(null);
        assertEquals(
                TablePath.of(null, "sales", "orders"),
                dialect.parseTablePath("sales.orders"));
        assertEquals(
                TablePath.of("app", "sales", "orders"),
                dialect.parseTablePath("app.sales.orders"));
    }

    @Test
    public void rejectsCrossDatabaseTablePaths() {
        GBase8cDialect dialect = dialect(null);
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> dialect.parseTablePath("archive.sales.orders"));
        assertTrue(error.getMessage().contains("不支持跨 database"));

        assertThrows(
                IllegalArgumentException.class,
                () -> dialect.tableIdentifier(TablePath.of("archive", "sales", "orders")));
    }

    @Test
    public void configuredSchemaAndPublicFallbackQualifyTables() {
        assertEquals(
                "\"sales\".\"orders\"",
                dialect("sales").tableIdentifier(TablePath.of("orders")));
        assertEquals(
                "\"public\".\"orders\"",
                dialect(null).tableIdentifier(TablePath.of("orders")));
        assertEquals(
                "\"archive\".\"orders\"",
                dialect("sales").tableIdentifier(TablePath.of(null, "archive", "orders")));
    }

    @Test
    public void stageOneCatalogIsReadOnlyAndUpsertIsNotAdvertised() {
        GBase8cDialect dialect = dialect("public");
        Catalog catalog = dialect.createCatalog(config("public", baseUrl(), null, null));
        assertFalse(catalog instanceof WritableCatalog);
        assertFalse(dialect.buildUpsertSql(
                TablePath.of(null, "public", "orders"),
                Arrays.asList("id", "name"),
                Collections.singletonList("id")).isPresent());
    }

    @Test
    public void stageOneTypeMapperDoesNotGenerateSinkTypes() {
        Column column = Column.builder("name", BasicType.STRING_TYPE).build();
        assertThrows(
                UnsupportedOperationException.class,
                () -> dialect("public").typeMapper().toDatabaseType(column));
    }

    @Test
    public void stageOneAdvertisesBestEffortReadConsistencyOnly() {
        assertEquals(
                Collections.singleton(ReadConsistency.BEST_EFFORT),
                dialect("public").supportedReadConsistencies());
    }

    @Test
    public void rowConverterKeepsFirstClassDatabaseIdentity() {
        assertEquals(
                DatabaseIdentifier.GBASE8C,
                dialect("public").rowConverter().name());
    }

    @Test
    public void requiresDatabaseInDedicatedJdbcUrl() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GBase8cDialect(
                        config(null, "jdbc:gbase8c://127.0.0.1:5432", null, null)));
    }

    private static GBase8cDialect dialect(String schema) {
        return new GBase8cDialect(config(schema, baseUrl(), null, null));
    }

    private static JdbcConnectionConfig config(
            String schema,
            String url,
            Map<String, String> properties,
            String dialect) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.gbase8c.Driver");
        values.put("username", "gbase");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        if (dialect != null) {
            values.put("dialect", dialect);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:gbase8c://127.0.0.1:5432/app?loggerLevel=warning";
    }
}
