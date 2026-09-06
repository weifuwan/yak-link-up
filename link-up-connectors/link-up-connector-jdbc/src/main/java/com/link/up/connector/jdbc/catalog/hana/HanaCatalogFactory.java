package com.link.up.connector.jdbc.catalog.hana;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;
import com.link.up.connector.jdbc.core.dialect.hana.HanaJdbcUrl;

import java.util.Collections;
import java.util.Map;

/** SAP HANA Catalog factory for bounded Source and offline Sink jobs. */
@AutoService(Factory.class)
public final class HanaCatalogFactory implements CatalogFactory {

    private static final String DEFAULT_DRIVER = "com.sap.db.jdbc.Driver";

    @Override
    public String factoryIdentifier() {
        return HanaCatalog.DIALECT;
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

        JdbcCatalogConfig config = new JdbcCatalogConfig(
                url,
                username,
                password,
                driver,
                properties,
                false);
        return new HanaCatalog(
                catalogName,
                config,
                HanaJdbcUrl.currentSchema(url, properties, schema),
                HanaJdbcUrl.databaseName(url, properties));
    }
}
