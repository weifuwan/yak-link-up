package com.link.up.connector.jdbc.core.dialect.iris;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** InterSystems IRIS offline JDBC dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class IrisDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.IRIS;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return IrisJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new IrisDialect(connectionConfig);
    }
}
