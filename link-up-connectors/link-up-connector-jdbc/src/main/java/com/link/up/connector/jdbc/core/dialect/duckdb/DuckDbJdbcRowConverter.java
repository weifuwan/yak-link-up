package com.link.up.connector.jdbc.core.dialect.duckdb;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.LocalTime;
import java.util.Locale;

/** DuckDB row converter with nanosecond-safe TIME and exact scalar binding. */
public final class DuckDbJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.DUCKDB;
    }

    @Override
    protected void writeValue(
            PreparedStatement statement,
            int index,
            Object value,
            Column column) throws SQLException {

        SqlType sqlType = column.getDataType().getSqlType();
        String base = baseType(column.getSourceType());

        if (sqlType == SqlType.TIME && value instanceof LocalTime) {
            try {
                statement.setObject(index, value);
            } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
                // String keeps fractional seconds whereas java.sql.Time does not.
                statement.setString(index, value.toString());
            }
            return;
        }

        if (sqlType == SqlType.STRING && requiresNativeScalarCast(base)) {
            statement.setString(index, asString(value));
            return;
        }

        super.writeValue(statement, index, value, column);
    }

    private static boolean requiresNativeScalarCast(String base) {
        return "HUGEINT".equals(base)
                || "INT128".equals(base)
                || "UHUGEINT".equals(base)
                || "UINT128".equals(base)
                || "BIGNUM".equals(base)
                || "BIT".equals(base)
                || "BITSTRING".equals(base)
                || "UUID".equals(base)
                || "JSON".equals(base)
                || "INTERVAL".equals(base)
                || "TIMETZ".equals(base)
                || "TIME WITH TIME ZONE".equals(base);
    }

    private static String baseType(String value) {
        if (value == null) {
            return "";
        }
        String type = value.trim()
                .toUpperCase(Locale.ROOT)
                .replaceAll("\\s+", " ");
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0
                ? type.substring(0, parenthesis).trim()
                : type;
    }
}
