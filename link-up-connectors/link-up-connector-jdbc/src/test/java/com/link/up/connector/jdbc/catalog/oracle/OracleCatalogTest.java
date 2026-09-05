package com.link.up.connector.jdbc.catalog.oracle;

import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OracleCatalogTest {

    @Test
    public void exposesLogicalDatabaseFromEasyConnectUrl() {
        OracleCatalog catalog =
                new OracleCatalog(
                        "oracle",
                        config(
                                "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1"),
                        "APP");

        assertEquals(
                "FREEPDB1",
                catalog.getDefaultDatabase()
                        .get());
    }

    @Test
    public void oraclePropertiesAreNotPollutedByMysqlOptions() {
        Properties properties =
                config(
                        "jdbc:oracle:thin:@127.0.0.1:1521:ORCL")
                        .toConnectionProperties();

        assertFalse(
                properties.containsKey(
                        "tinyInt1isBit"));
    }

    private static JdbcCatalogConfig config(
            String url) {

        return new JdbcCatalogConfig(
                url,
                "app",
                "secret",
                "oracle.jdbc.OracleDriver",
                Collections.<String, String>emptyMap(),
                false);
    }
}
