package com.link.up.connector.jdbc.core.dialect.dameng;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** Dameng DM8 dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class DamengDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return DamengJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new DamengDialect(connectionConfig);
    }
}
