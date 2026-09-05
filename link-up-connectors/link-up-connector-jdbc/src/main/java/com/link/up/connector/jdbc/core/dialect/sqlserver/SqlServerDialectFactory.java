package com.link.up.connector.jdbc.core.dialect.sqlserver;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** Factory for the bounded SQL Server JDBC dialect. */
@AutoService(JdbcDialectFactory.class)
public final class SqlServerDialectFactory
        implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return SqlServerJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(
            JdbcConnectionConfig connectionConfig) {
        return new SqlServerDialect(connectionConfig);
    }
}
