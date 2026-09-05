package com.link.up.connector.jdbc.catalog.oceanbase;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Catalog;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OceanBaseCatalogFactoryTest {

    @Test
    public void createsMysqlCompatibleCatalog() {
        Catalog catalog =
                new OceanBaseCatalogFactory()
                        .createCatalog(
                                "oceanbase",
                                config("mysql"));

        assertTrue(
                catalog
                        instanceof OceanBaseCatalog);

        assertEquals(
                "app",
                catalog.getDefaultDatabase()
                        .get());
    }

    @Test
    public void createsOracleCompatibleCatalog() {
        Catalog catalog =
                new OceanBaseCatalogFactory()
                        .createCatalog(
                                "oceanbase",
                                config("oracle"));

        assertTrue(
                catalog
                        instanceof OceanBaseCatalog);

        assertEquals(
                "app",
                catalog.getDefaultDatabase()
                        .get());
    }

    @Test
    public void rejectsMissingCompatibleMode() {
        Map<String, Object> values =
                baseValues();

        try {
            new OceanBaseCatalogFactory()
                    .createCatalog(
                            "oceanbase",
                            ReadonlyConfig.fromMap(
                                    values));

            fail(
                    "missing compatible mode must fail");
        } catch (IllegalArgumentException e) {
            assertTrue(
                    e.getMessage()
                            .contains(
                                    "compatible_mode"));
        }
    }

    private static ReadonlyConfig config(
            String mode) {

        Map<String, Object> values =
                baseValues();

        values.put(
                "compatible_mode",
                mode);

        return ReadonlyConfig.fromMap(
                values);
    }

    private static Map<String, Object>
    baseValues() {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put(
                "url",
                "jdbc:oceanbase://127.0.0.1:2881/app");

        values.put(
                "driver",
                "com.oceanbase.jdbc.Driver");

        values.put(
                "username",
                "app");

        values.put(
                "password",
                "secret");

        values.put(
                "schema",
                "APP");

        return values;
    }
}
