package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.api.table.catalog.Column;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;
import com.link.up.connector.jdbc.core.dialect.postgres.PostgresTypeMapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * GBase 8c Stage 1 type mapper.
 *
 * <p>The bounded Source targets PG-compatible databases and reuses the mature PostgreSQL read-side
 * type contract. Sink DDL mapping stays disabled until the dedicated GBase 8c Sink stage.</p>
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
                "GBase 8c Stage 1 is source-only; target type generation belongs to the Sink stage");
    }
}
