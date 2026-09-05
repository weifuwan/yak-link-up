package com.link.up.connector.jdbc.core.dialect.db2;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** DB2 LUW dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class Db2DialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.DB2;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return Db2JdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new Db2Dialect(connectionConfig);
    }
}
