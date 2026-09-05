package com.link.up.connector.jdbc.core.dialect.yashandb;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** YashanDB offline JDBC dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class YashanDbDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.YASHANDB;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return YashanDbJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new YashanDbDialect(connectionConfig);
    }
}
