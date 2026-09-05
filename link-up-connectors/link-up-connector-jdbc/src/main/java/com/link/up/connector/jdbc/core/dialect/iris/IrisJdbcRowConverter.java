package com.link.up.connector.jdbc.core.dialect.iris;

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

/** IRIS row converter with stream-safe LOB and precise temporal writes. */
public final class IrisJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String NATIVE_ATTRIBUTE = IrisTypeMapper.NATIVE_ATTRIBUTE;

    @Override
    public String name() {
        return DatabaseIdentifier.IRIS;
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        SqlType sqlType = column.getDataType().getSqlType();
        if (sqlType == SqlType.TIMESTAMP_TZ) {
            statement.setNull(index, Types.VARCHAR);
            return;
        }
        if (sqlType == SqlType.TIME) {
            statement.setNull(index, Types.TIME);
            return;
        }
        if (isNative(column)
                && sqlType == SqlType.STRING
                && isExactNumeric(column.getSourceType())) {
            // Exact numeric values are carried as strings. VARCHAR null typing
            // works both for an existing NUMERIC target and our LONGVARCHAR
            // fallback used when the source numeric shape is not representable.
            statement.setNull(index, Types.VARCHAR);
            return;
        }
        if (sqlType == SqlType.STRING && isLongText(column)) {
            statement.setNull(index, Types.LONGVARCHAR);
            return;
        }
        if (sqlType == SqlType.BYTES && isLongBinary(column)) {
            statement.setNull(index, Types.LONGVARBINARY);
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

        SqlType sqlType = column.getDataType().getSqlType();
        if (sqlType == SqlType.TIMESTAMP_TZ) {
            // IRIS core SQL has no offset-aware timestamp type; the type mapper
            // creates VARCHAR(64) so the offset is preserved exactly.
            statement.setString(index, String.valueOf(value));
            return;
        }
        if (sqlType == SqlType.TIME) {
            LocalTime time = value instanceof LocalTime
                    ? (LocalTime) value
                    : LocalTime.parse(String.valueOf(value));
            statement.setObject(index, time);
            return;
        }
        if (isNative(column)
                && sqlType == SqlType.STRING
                && isExactNumeric(column.getSourceType())) {
            statement.setString(index, String.valueOf(value));
            return;
        }
        if (sqlType == SqlType.STRING && isLongText(column)) {
            String text = asString(value);
            statement.setCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());
            return;
        }
        if (sqlType == SqlType.BYTES && isLongBinary(column)) {
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

    private static boolean isExactNumeric(String sourceType) {
        String base = baseType(sourceType);
        return "numeric".equals(base)
                || "decimal".equals(base)
                || "dec".equals(base)
                || "number".equals(base)
                || "money".equals(base)
                || "smallmoney".equals(base);
    }

    private static boolean isLongText(Column column) {
        String base = baseType(column.getSourceType());
        return "longvarchar".equals(base)
                || "long varchar".equals(base)
                || "clob".equals(base)
                || "ntext".equals(base)
                || "text".equals(base)
                || "longtext".equals(base)
                || "mediumtext".equals(base)
                || column.getLength() == null
                || column.getLength() > IrisTypeMapper.MAX_BOUNDED_VARCHAR_LENGTH;
    }

    private static boolean isLongBinary(Column column) {
        String base = baseType(column.getSourceType());
        return "longvarbinary".equals(base)
                || "blob".equals(base)
                || "image".equals(base)
                || "long binary".equals(base)
                || "long raw".equals(base)
                || column.getLength() == null
                || column.getLength() > IrisTypeMapper.MAX_BOUNDED_BINARY_LENGTH;
    }

    private static String baseType(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim()
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ");
        int parenthesis = normalized.indexOf('(');
        return parenthesis >= 0
                ? normalized.substring(0, parenthesis).trim()
                : normalized;
    }
}
