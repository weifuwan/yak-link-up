package com.link.up.connector.jdbc.catalog.xugu;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;
import com.link.up.connector.jdbc.core.dialect.xugu.XuguJdbcUrl;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** XuguDB Catalog factory. */
@AutoService(Factory.class)
public final class XuguCatalogFactory implements CatalogFactory {

    private static final String DEFAULT_DRIVER = "com.xugu.cloudjdbc.Driver";

    @Override
    public String factoryIdentifier() {
        return XuguCatalog.DIALECT;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String url = options.get(JdbcCommonOptions.URL);
        String username = options.getOptional(JdbcCommonOptions.USERNAME).orElse(null);
        String password = options.getOptional(JdbcCommonOptions.PASSWORD).orElse(null);
        String driver = options.getOptional(JdbcCommonOptions.DRIVER).orElse(DEFAULT_DRIVER);
        String schema = options.getOptional(JdbcCommonOptions.SCHEMA).orElse(null);
        String compatibleMode = options.getOptional(JdbcCommonOptions.COMPATIBLE_MODE).orElse(null);
        Map<String, String> configured = options.getOptional(JdbcCommonOptions.PROPERTIES)
                .orElse(Collections.<String, String>emptyMap());
        Map<String, String> properties = new LinkedHashMap<String, String>(configured);
        if (!containsIgnoreCase(properties, "useLike")) {
            properties.put("useLike", "true");
        }
        if (hasText(compatibleMode)
                && !hasText(XuguJdbcUrl.compatibleMode(url, properties))) {
            properties.put("compatiblemode", compatibleMode.trim());
        }

        return new XuguCatalog(
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

    private static boolean containsIgnoreCase(Map<String, String> properties, String key) {
        for (String propertyKey : properties.keySet()) {
            if (propertyKey != null && key.equalsIgnoreCase(propertyKey.trim())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
