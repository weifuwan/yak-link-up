package com.link.up.connector.jdbc.catalog.duckdb;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.factory.Factory;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.config.JdbcCommonOptions;
import com.link.up.connector.jdbc.core.dialect.duckdb.DuckDbJdbcUrl;

import java.util.Collections;
import java.util.Map;

/** DuckDB Catalog factory. */
@AutoService(Factory.class)
public final class DuckDbCatalogFactory implements CatalogFactory {

    private static final String DEFAULT_DRIVER = "org.duckdb.DuckDBDriver";

    @Override
    public String factoryIdentifier() {
        return DuckDbCatalog.DIALECT;
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

        if (DuckDbJdbcUrl.isConnectionPrivateMemory(url)) {
            throw new IllegalArgumentException(
                    "DuckDB 匿名内存库无法跨 Catalog/Reader/Writer 连接共享");
        }
        if (DuckDbJdbcUrl.isDuckLake(url)) {
            throw new IllegalArgumentException(
                    "DuckLake 暂不属于 DuckDB Offline JDBC 首阶段范围");
        }
        if (DuckDbJdbcUrl.isInstanceCacheDisabled(url, properties)) {
            throw new IllegalArgumentException(
                    "DuckDB Offline JDBC 需要 jdbc_instance_cache=true");
        }
        if (DuckDbJdbcUrl.isUnpinnedNamedMemory(url, properties)) {
            throw new IllegalArgumentException(
                    "DuckDB 命名内存库必须配置 jdbc_pin_db=true");
        }

        return new DuckDbCatalog(
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
