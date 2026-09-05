package com.link.up.connector.jdbc.catalog.iris;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;

import java.util.Collections;
import java.util.Map;

/** InterSystems IRIS Catalog factory. */
@AutoService(Factory.class)
public final class IrisCatalogFactory implements CatalogFactory {

    private static final String DEFAULT_DRIVER = "com.intersystems.jdbc.IRISDriver";

    @Override
    public String factoryIdentifier() {
        return IrisCatalog.DIALECT;
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

        return new IrisCatalog(
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
