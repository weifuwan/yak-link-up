package com.link.up.connector.jdbc.core.dialect.oracle;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/**
 * Oracle dialect factory.
 */
@AutoService(JdbcDialectFactory.class)
public final class OracleDialectFactory
        implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    public boolean acceptsUrl(
            String url) {

        return OracleJdbcUrl.accepts(
                url);
    }

    @Override
    public JdbcDialect create(
            JdbcConnectionConfig connectionConfig) {

        return new OracleDialect(
                connectionConfig);
    }
}
