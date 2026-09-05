package com.link.up.connector.jdbc.core.dialect.xugu;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** XuguDB offline JDBC dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class XuguDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.XUGU;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return XuguJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new XuguDialect(connectionConfig);
    }
}
