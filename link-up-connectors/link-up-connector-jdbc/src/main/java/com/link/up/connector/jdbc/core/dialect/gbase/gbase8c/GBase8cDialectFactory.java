package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** GBase 8c bounded Source and existing-table Sink dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class GBase8cDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.GBASE8C;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return GBase8cJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new GBase8cDialect(connectionConfig);
    }
}
