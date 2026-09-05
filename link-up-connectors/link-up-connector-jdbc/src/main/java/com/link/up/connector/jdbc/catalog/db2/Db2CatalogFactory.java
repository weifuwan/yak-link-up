package com.link.up.connector.jdbc.catalog.db2;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;

import java.util.Collections;
import java.util.Map;

/** DB2 LUW Catalog factory. */
@AutoService(Factory.class)
public final class Db2CatalogFactory implements CatalogFactory {

    private static final String DEFAULT_DRIVER = "com.ibm.db2.jcc.DB2Driver";

    @Override
    public String factoryIdentifier() {
        return Db2Catalog.DIALECT;
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
        return new Db2Catalog(catalogName, config, schema);
    }
}
