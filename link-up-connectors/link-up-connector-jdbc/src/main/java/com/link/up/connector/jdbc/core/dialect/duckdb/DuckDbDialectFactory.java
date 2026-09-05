package com.link.up.connector.jdbc.core.dialect.duckdb;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** DuckDB offline JDBC dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class DuckDbDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.DUCKDB;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return DuckDbJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new DuckDbDialect(connectionConfig);
    }
}
