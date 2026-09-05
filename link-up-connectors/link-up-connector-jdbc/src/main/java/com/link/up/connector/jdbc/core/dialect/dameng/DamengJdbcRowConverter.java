package com.link.up.connector.jdbc.core.dialect.dameng;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

/** Dameng row converter with LOB-safe writes. */
public final class DamengJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final long INLINE_LIMIT = DamengTypeMapper.MAX_INLINE_BYTES;

    @Override
    public String name() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        String sourceType = baseType(column.getSourceType());
        SqlType sqlType = column.getDataType().getSqlType();

        if (isCharacterLob(sourceType)
                || (sqlType == SqlType.STRING
                && (column.getLength() == null || exceedsInlineStringLength(column.getLength()))
                && !isTimeWithTimezone(column.getSourceType()))) {
            statement.setNull(index, Types.CLOB);
            return;
        }

        if (isBinaryLob(sourceType)
                || (sqlType == SqlType.BYTES
                && (column.getLength() == null || column.getLength() > INLINE_LIMIT))) {
            statement.setNull(index, Types.BLOB);
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

        String sourceType = baseType(column.getSourceType());
        SqlType sqlType = column.getDataType().getSqlType();

        if (isTimeWithTimezone(column.getSourceType()) && sqlType == SqlType.STRING) {
            // Exact text is intentionally used because Flux has no TIME_TZ primitive.
            statement.setString(index, asString(value));
            return;
        }

        if (isCharacterLob(sourceType)
                || (sqlType == SqlType.STRING
                && (column.getLength() == null || exceedsInlineStringLength(column.getLength())))) {
            String text = asString(value);
            statement.setCharacterStream(index, new StringReader(text), text.length());
            return;
        }

        if (isBinaryLob(sourceType)
                || (sqlType == SqlType.BYTES
                && (column.getLength() == null || column.getLength() > INLINE_LIMIT))) {
            byte[] bytes = asBytes(value);
            statement.setBinaryStream(index, new ByteArrayInputStream(bytes), bytes.length);
            return;
        }

        super.writeValue(statement, index, value, column);
    }

    private static boolean exceedsInlineStringLength(long length) {
        return length <= 0 || length > INLINE_LIMIT / 4L;
    }

    private static boolean isCharacterLob(String type) {
        return "CLOB".equals(type)
                || "TEXT".equals(type)
                || "LONG".equals(type)
                || "LONGVARCHAR".equals(type);
    }

    private static boolean isBinaryLob(String type) {
        return "BLOB".equals(type)
                || "IMAGE".equals(type)
                || "LONGVARBINARY".equals(type);
    }

    private static boolean isTimeWithTimezone(String value) {
        String type = normalize(value);
        return type.startsWith("TIME")
                && !type.startsWith("TIMESTAMP")
                && type.contains("WITH TIME ZONE");
    }

    private static String baseType(String value) {
        String type = normalize(value);
        int parenthesis = type.indexOf('(');
        int space = type.indexOf(' ');
        int end = type.length();
        if (parenthesis >= 0) {
            end = Math.min(end, parenthesis);
        }
        if (space >= 0) {
            end = Math.min(end, space);
        }
        return type.substring(0, end).trim();
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
    }
}
