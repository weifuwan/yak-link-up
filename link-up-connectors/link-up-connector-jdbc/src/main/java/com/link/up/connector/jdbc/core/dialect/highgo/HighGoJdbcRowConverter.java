package com.link.up.connector.jdbc.core.dialect.highgo;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Locale;

/** HighGo row converter with native extension, TIME precision and LOB-safe writes. */
public final class HighGoJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String NATIVE_ATTRIBUTE = HighGoTypeMapper.NATIVE_ATTRIBUTE;
    private static final long STREAM_STRING_THRESHOLD = 1_048_576L;
    private static final long STREAM_BYTES_THRESHOLD = 1_048_576L;

    @Override
    public String name() {
        return DatabaseIdentifier.HIGHGO;
    }

    /**
     * java.sql.Time drops fractional seconds. HighGo TIME supports microseconds,
     * so bind the Java 8 time value directly through the PG-compatible driver.
     */
    @Override
    protected void writeTime(
            PreparedStatement statement,
            int index,
            LocalTime value) throws SQLException {
        statement.setObject(index, value);
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        if (isNative(column)
                && column.getDataType().getSqlType() == SqlType.STRING
                && requiresOtherBinding(column)) {
            statement.setNull(index, Types.OTHER);
            return;
        }
        if (column.getDataType().getSqlType() == SqlType.BYTES
                && isLargeBinary(column)) {
            statement.setNull(index, Types.VARBINARY);
            return;
        }
        if (column.getDataType().getSqlType() == SqlType.STRING
                && isLargeText(column)) {
            statement.setNull(index, Types.LONGVARCHAR);
            return;
        }
        super.writeNull(statement, index, column);
    }

    @Override
    protected void writeValue(
            PreparedStatement statement,
            int index,
            Object value,
            Column column) throws SQLException {

        if (isNative(column)
                && column.getDataType().getSqlType() == SqlType.STRING
                && requiresOtherBinding(column)) {
            statement.setObject(index, String.valueOf(value), Types.OTHER);
            return;
        }

        if (column.getDataType().getSqlType() == SqlType.STRING
                && isLargeText(column)) {
            String text = asString(value);
            statement.setCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());
            return;
        }

        if (column.getDataType().getSqlType() == SqlType.BYTES
                && isLargeBinary(column)) {
            byte[] bytes = asBytes(value);
            statement.setBinaryStream(
                    index,
                    new ByteArrayInputStream(bytes),
                    bytes.length);
            return;
        }

        super.writeValue(statement, index, value, column);
    }

    private static boolean isNative(Column column) {
        return "true".equalsIgnoreCase(
                column.getAttributes().get(NATIVE_ATTRIBUTE));
    }

    private static boolean requiresOtherBinding(Column column) {
        String type = normalize(column.getSourceType());
        String base = baseType(type);
        return !isOrdinaryText(base)
                && !"clob".equals(base)
                && !"nclob".equals(base);
    }

    private static boolean isOrdinaryText(String base) {
        return base.contains("char")
                || "text".equals(base)
                || "tinytext".equals(base)
                || "mediumtext".equals(base)
                || "longtext".equals(base)
                || "name".equals(base)
                || "citext".equals(base)
                || "long".equals(base);
    }

    private static boolean isLargeText(Column column) {
        String base = baseType(normalize(column.getSourceType()));
        return "clob".equals(base)
                || "nclob".equals(base)
                || "longtext".equals(base)
                || column.getLength() == null
                || column.getLength() > STREAM_STRING_THRESHOLD;
    }

    private static boolean isLargeBinary(Column column) {
        String base = baseType(normalize(column.getSourceType()));
        return "blob".equals(base)
                || "mediumblob".equals(base)
                || "longblob".equals(base)
                || column.getLength() == null
                || column.getLength() > STREAM_BYTES_THRESHOLD;
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0
                ? type.substring(0, parenthesis).trim()
                : type;
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }
}
