package com.link.up.connector.jdbc.catalog.sqlserver;

import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerJdbcUrl;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SqlServerCatalogTest {

    @Test
    public void parsesAndRewritesDatabaseName() {
        String url = "jdbc:sqlserver://127.0.0.1:1433;databaseName=app;encrypt=false";
        assertEquals("app", SqlServerJdbcUrl.databaseName(url));
        assertEquals(
                "jdbc:sqlserver://127.0.0.1:1433;databaseName=master;encrypt=false",
                SqlServerJdbcUrl.withDatabase(url, "master"));
    }

    @Test
    public void catalogExposesDefaultDatabaseBeforeOpen() {
        SqlServerCatalog catalog = new SqlServerCatalog("sqlserver", config(), "sales");
        assertEquals("app", catalog.getDefaultDatabase().get());
    }

    @Test
    public void sqlServerPropertiesAreNotPollutedByMysqlOptions() {
        Properties properties = config().toConnectionProperties();
        assertFalse(properties.containsKey("tinyInt1isBit"));
    }

    private static JdbcCatalogConfig config() {
        return new JdbcCatalogConfig(
                "jdbc:sqlserver://127.0.0.1:1433;databaseName=app;encrypt=false",
                "sa", "test", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                Collections.<String, String>emptyMap(), false);
    }
}
