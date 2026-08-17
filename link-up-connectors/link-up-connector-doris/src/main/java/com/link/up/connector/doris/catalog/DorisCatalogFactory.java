package com.link.up.connector.doris.catalog;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.doris.config.DorisSinkOptions;

/**
 * Doris Catalog SPI 工厂。
 *
 * <p>通过 {@code factoryIdentifier = "doris"} 注册，
 * 由 SPI 机制自动发现和加载。
 */
@AutoService(Factory.class)
public final class DorisCatalogFactory
        implements CatalogFactory {

    private static final String IDENTIFIER = "doris";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            ReadonlyConfig options) {

        String fenodes =
                options.get(DorisSinkOptions.FENODES);

        if (fenodes == null
                || fenodes.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "Doris Catalog 'fenodes' must not be blank");
        }

        int queryPort =
                options.getOptional(
                                DorisSinkOptions.QUERY_PORT)
                        .orElse(9030);

        String username =
                options.get(DorisSinkOptions.USERNAME);

        if (username == null
                || username.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "Doris Catalog 'username' must not be blank");
        }

        String password =
                options.getOptional(
                                DorisSinkOptions.PASSWORD)
                        .orElse(null);

        String database =
                options.getOptional(
                                DorisSinkOptions.DATABASE)
                        .orElse(null);

        DorisCatalogConfig config =
                new DorisCatalogConfig(
                        fenodes.trim(),
                        queryPort,
                        username.trim(),
                        password,
                        database);

        return new DorisCatalog(
                catalogName,
                config);
    }
}
