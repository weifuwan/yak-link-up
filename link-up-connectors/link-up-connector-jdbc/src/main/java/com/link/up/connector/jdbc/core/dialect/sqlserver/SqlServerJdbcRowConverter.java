package com.link.up.connector.jdbc.core.dialect.sqlserver;

import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

/** SQL Server JDBC row converter. */
public final class SqlServerJdbcRowConverter
        extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.SQLSERVER;
    }
}
