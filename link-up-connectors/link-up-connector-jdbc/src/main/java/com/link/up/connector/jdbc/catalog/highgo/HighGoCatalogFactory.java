package com.link.up.connector.jdbc.catalog.highgo;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;

import java.util.Collections;
import java.util.Map;

/** HighGo Catalog factory. */
@AutoService(Factory.class)
public final class HighGoCatalogFactory implements CatalogFactory {

    private static final String DEFAULT_DRIVER = "com.highgo.jdbc.Driver";

    @Override
    public String factoryIdentifier() {
        return HighGoCatalog.DIALECT;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String url = options.get(JdbcCommonOptions.URL);
        String username = options.getOptional(JdbcCommonOptions.USERNAME).orElse(null);
        String password = options.getOptional(JdbcCommonOptions.PASSWORD).orElse(null);
        String driver = options.getOptional(JdbcCommonOptions.DRIVER).orElse(DEFAULT_DRIVER);
        String schema = options.getOptional(JdbcCommonOptions.SCHEMA).orElse(null);
        Map<String, String> properties = options.getOptional(JdbcCommonOptions.PROPERTIES)
                .orElse(Collections.<String, String>emptyMap());

        return new HighGoCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        url,
                        username,
                        password,
                        driver,
                        properties,
                        false),
                schema);
    }
}
