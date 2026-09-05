package com.link.up.connector.jdbc.core.dialect.xugu;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/** XuguDB row converter with lossless LOB/bit/timezone handling. */
public final class XuguJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String NATIVE_ATTRIBUTE = XuguTypeMapper.NATIVE_ATTRIBUTE;

    @Override
    public String name() {
        return DatabaseIdentifier.XUGU;
    }

    @Override
    protected LocalTime readTime(ResultSet resultSet, int index) throws SQLException {
        // Xugu's java.sql.Time path does not expose fractional TIME precision.
        // Reading textual TIME first keeps milliseconds when the driver returns them.
        String text = resultSet.getString(index);
        if (text == null) {
            return null;
        }
        try {
            return LocalTime.parse(text.trim());
        } catch (DateTimeParseException ignored) {
            return super.readTime(resultSet, index);
        }
    }

    @Override
    protected void writeTime(
            PreparedStatement statement,
            int index,
            LocalTime value) throws SQLException {
        // Xugu JDBC documents a precision-losing java.sql.Time output path.
        // Bind ISO local-time text and let the target TIME column perform the
        // database-native conversion so up to millisecond precision survives.
        statement.setString(index, value.toString());
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        SqlType sqlType = column.getDataType().getSqlType();
        if (sqlType == SqlType.TIMESTAMP_TZ) {
            // Target mapping is VARCHAR(64) to avoid the Xugu JDBC offset-loss
            // batch path documented by the upstream connector.
            statement.setNull(index, Types.VARCHAR);
            return;
        }
        if (isNative(column)
                && sqlType == SqlType.STRING
                && isExactNativeString(column.getSourceType())) {
            // Exact-value textual transport (NUMERIC overflow, BIT strings,
            // TIME WITH TIME ZONE, etc.) must not become a CLOB-typed null
            // merely because the source metadata has no bounded length.
            statement.setNull(index, Types.VARCHAR);
            return;
        }
        if (sqlType == SqlType.STRING && isLongText(column)) {
            statement.setNull(index, Types.CLOB);
            return;
        }
        if (sqlType == SqlType.BYTES && isLongBinary(column)) {
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

        SqlType sqlType = column.getDataType().getSqlType();
        if (sqlType == SqlType.TIMESTAMP_TZ) {
            OffsetDateTime offsetDateTime = value instanceof OffsetDateTime
                    ? (OffsetDateTime) value
                    : OffsetDateTime.parse(String.valueOf(value));
            statement.setString(index, offsetDateTime.toString());
            return;
        }
        if (isNative(column)
                && sqlType == SqlType.STRING
                && isExactNativeString(column.getSourceType())) {
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

    private static boolean isExactNativeString(String sourceType) {
        String type = normalize(sourceType);
        String base = baseType(type);
        return "numeric".equals(base)
                || "decimal".equals(base)
                || "number".equals(base)
                || "bit".equals(base)
                || "varbit".equals(base)
                || "bit varying".equals(base)
                || "guid".equals(base)
                || "uuid".equals(base)
                || "json".equals(base)
                || "xml".equals(base)
                || "rowid".equals(base)
                || "interval".equals(base)
                || base.startsWith("interval ")
                || isTimeWithTimeZone(type, base);
    }

    private static boolean isTimeWithTimeZone(String type, String base) {
        return ("time".equals(base) || base.startsWith("time "))
                && !base.startsWith("timestamp")
                && type.contains("with time zone")
                && !type.contains("without time zone");
    }

    private static boolean isLongText(Column column) {
        String base = baseType(normalize(column.getSourceType()));
        return "clob".equals(base)
                || "nclob".equals(base)
                || column.getLength() == null
                || column.getLength() > XuguTypeMapper.MAX_VARCHAR_LENGTH;
    }

    private static boolean isLongBinary(Column column) {
        String base = baseType(normalize(column.getSourceType()));
        return "blob".equals(base)
                || "longvarbinary".equals(base)
                || "long varbinary".equals(base)
                || column.getLength() == null
                || column.getLength() > XuguTypeMapper.MAX_BINARY_LENGTH;
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        if (parenthesis < 0) {
            return type;
        }
        String before = type.substring(0, parenthesis).trim();
        int close = type.indexOf(')', parenthesis + 1);
        if (close >= 0 && close < type.length() - 1) {
            String suffix = type.substring(close + 1).trim();
            return suffix.isEmpty() ? before : before + " " + suffix;
        }
        return before;
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }
}
