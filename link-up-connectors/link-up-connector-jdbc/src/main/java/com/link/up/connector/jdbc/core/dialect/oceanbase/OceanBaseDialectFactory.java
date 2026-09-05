package com.link.up.connector.jdbc.core.dialect.oceanbase;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/**
 * OceanBase dialect SPI.
 */
@AutoService(JdbcDialectFactory.class)
public final class OceanBaseDialectFactory
        implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.OCEANBASE;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return OceanBaseJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(
            JdbcConnectionConfig connectionConfig) {

        return new OceanBaseDialect(
                connectionConfig);
    }
}
