package com.link.up.connector.jdbc.catalog.oceanbase;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;
import com.link.up.connector.jdbc.core.dialect.oceanbase.OceanBaseCompatibleMode;

import java.util.Collections;
import java.util.Map;

/**
 * OceanBase Catalog factory.
 */
@AutoService(Factory.class)
public final class OceanBaseCatalogFactory
        implements CatalogFactory {

    private static final String DEFAULT_DRIVER =
            "com.oceanbase.jdbc.Driver";

    @Override
    public String factoryIdentifier() {
        return OceanBaseCatalog.DIALECT;
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

        boolean intTypeNarrowing =
                options.getOptional(
                        JdbcCommonOptions
                                .INT_TYPE_NARROWING)
                        .orElse(false);

        String schema =
                options.getOptional(
                        JdbcCommonOptions.SCHEMA)
                        .orElse(null);

        OceanBaseCompatibleMode mode =
                OceanBaseCompatibleMode.from(
                        options.getOptional(
                                JdbcCommonOptions
                                        .COMPATIBLE_MODE)
                                .orElse(null));

        JdbcCatalogConfig config =
                new JdbcCatalogConfig(
                        url,
                        username,
                        password,
                        driver,
                        properties,
                        intTypeNarrowing);

        return new OceanBaseCatalog(
                catalogName,
                config,
                mode,
                schema);
    }
}
