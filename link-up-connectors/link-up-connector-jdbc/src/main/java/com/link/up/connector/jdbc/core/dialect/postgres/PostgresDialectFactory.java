package com.link.up.connector.jdbc.core.dialect.postgres;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/**
 * PostgreSQL JDBC 方言工厂。
 */
@AutoService(JdbcDialectFactory.class)
public final class PostgresDialectFactory
        implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null
                && url.trim()
                .toLowerCase()
                .startsWith("jdbc:postgresql:");
    }

    @Override
    public JdbcDialect create(
            JdbcConnectionConfig connectionConfig) {

        return new PostgresDialect(connectionConfig);
    }
}
