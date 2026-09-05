package com.link.up.connector.jdbc.core.dialect.opengauss;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** openGauss dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class OpenGaussDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.OPENGAUSS;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return OpenGaussJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new OpenGaussDialect(connectionConfig);
    }
}
