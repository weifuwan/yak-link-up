package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.api.table.catalog.Column;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;
import com.link.up.connector.jdbc.core.dialect.postgres.PostgresTypeMapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * GBase 8c type mapper for bounded Source and existing-table Sink jobs.
 *
 * <p>The adapter reuses the mature PostgreSQL-compatible read-side type contract. The current Sink
 * writes only to pre-created tables, so target DDL type generation stays disabled until a later
 * GBase 8c table-creation stage models distribution and compatibility-mode semantics.</p>
 */
public final class GBase8cTypeMapper implements JdbcTypeMapper {

    private final PostgresTypeMapper delegate = new PostgresTypeMapper();

    @Override
    public Column map(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        return delegate.map(metadata, columnIndex);
    }

    /** Maps one information_schema.columns row using the PG-compatible read contract. */
    public Column toColumn(ResultSet row) throws SQLException {
        return delegate.toColumn(row);
    }

    @Override
    public String toDatabaseType(Column column) {
        throw new UnsupportedOperationException(
                "GBase 8c existing-table Sink does not generate target DDL types");
    }
}
