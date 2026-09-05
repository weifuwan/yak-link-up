package com.link.up.connector.jdbc.core.dialect.oceanbase;

import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Keeps the connector identity as OceanBase while delegating concrete value
 * handling to the selected MySQL/Oracle row converter.
 */
public final class OceanBaseJdbcRowConverter
        implements JdbcRowConverter {

    private final JdbcRowConverter delegate;

    public OceanBaseJdbcRowConverter(
            JdbcRowConverter delegate) {

        this.delegate =
                Objects.requireNonNull(
                        delegate,
                        "delegate must not be null");
    }

    @Override
    public String name() {
        return DatabaseIdentifier.OCEANBASE;
    }

    @Override
    public FluxRow read(
            ResultSet resultSet,
            TableSchema tableSchema)
            throws SQLException {

        return delegate.read(
                resultSet,
                tableSchema);
    }

    @Override
    public void write(
            PreparedStatement statement,
            FluxRow row,
            TableSchema tableSchema)
            throws SQLException {

        delegate.write(
                statement,
                row,
                tableSchema);
    }
}
