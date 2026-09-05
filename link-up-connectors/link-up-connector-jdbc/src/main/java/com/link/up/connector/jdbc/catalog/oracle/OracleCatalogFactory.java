package com.link.up.connector.jdbc.catalog.oracle;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;

import java.util.Collections;
import java.util.Map;

/**
 * Oracle Catalog factory.
 */
@AutoService(Factory.class)
public final class OracleCatalogFactory
        implements CatalogFactory {

    private static final String DEFAULT_DRIVER =
            "oracle.jdbc.OracleDriver";

    @Override
    public String factoryIdentifier() {
        return OracleCatalog.DIALECT;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            ReadonlyConfig options) {

        String url =
                options.get(
                        JdbcCommonOptions.URL);

        String username =
                options.getOptional(
                        JdbcCommonOptions.USERNAME)
                        .orElse(null);

        String password =
                options.getOptional(
                        JdbcCommonOptions.PASSWORD)
                        .orElse(null);

        String driver =
                options.getOptional(
                        JdbcCommonOptions.DRIVER)
                        .orElse(
                                DEFAULT_DRIVER);

        Map<String, String> properties =
                options.getOptional(
                        JdbcCommonOptions.PROPERTIES)
                        .orElse(
                                Collections.emptyMap());

        String schema =
                options.getOptional(
                        JdbcCommonOptions.SCHEMA)
                        .orElse(null);

        JdbcCatalogConfig config =
                new JdbcCatalogConfig(
                        url,
                        username,
                        password,
                        driver,
                        properties,
                        false);

        return new OracleCatalog(
                catalogName,
                config,
                schema);
    }
}
