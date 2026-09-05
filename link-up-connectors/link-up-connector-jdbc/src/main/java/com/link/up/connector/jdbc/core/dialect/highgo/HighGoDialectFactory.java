package com.link.up.connector.jdbc.core.dialect.highgo;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** HighGo offline JDBC dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class HighGoDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.HIGHGO;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return HighGoJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new HighGoDialect(connectionConfig);
    }
}
