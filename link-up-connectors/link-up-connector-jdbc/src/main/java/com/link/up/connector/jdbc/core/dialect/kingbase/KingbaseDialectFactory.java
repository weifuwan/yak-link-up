package com.link.up.connector.jdbc.core.dialect.kingbase;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** KingbaseES dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class KingbaseDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return KingbaseJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new KingbaseDialect(connectionConfig);
    }
}
