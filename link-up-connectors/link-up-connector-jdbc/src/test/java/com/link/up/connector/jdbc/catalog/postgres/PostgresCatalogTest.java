package com.link.up.connector.jdbc.catalog.postgres;

import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PostgresCatalogTest {

    @Test
    public void parsesDefaultDatabaseFromPostgresUrl() {
        PostgresCatalog catalog =
                new PostgresCatalog(
                        "postgresql",
                        config(),
                        "analytics");

        assertEquals(
                "app",
                catalog.getDefaultDatabase()
                        .get());
    }

    @Test
    public void postgresPropertiesAreNotPollutedByMysqlOptions() {
        Properties properties =
                config()
                        .toConnectionProperties();

        assertFalse(
                properties.containsKey(
                        "tinyInt1isBit"));
    }

    private static JdbcCatalogConfig config() {
        return new JdbcCatalogConfig(
                "jdbc:postgresql://127.0.0.1:5432/app?sslmode=disable",
                "test",
                "test",
                "org.postgresql.Driver",
                Collections.<String, String>emptyMap(),
                false);
    }
}
