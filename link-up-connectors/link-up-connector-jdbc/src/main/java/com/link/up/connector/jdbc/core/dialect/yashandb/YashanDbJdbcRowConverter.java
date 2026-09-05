package com.link.up.connector.jdbc.core.dialect.yashandb;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Locale;

/** YashanDB row converter with lossless NUMBER/TIME and LOB handling. */
public final class YashanDbJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.YASHANDB;
    }

    @Override
    protected Object readValue(
            ResultSet resultSet,
            int index,
            Column column) throws SQLException {

        if (column.getDataType().getSqlType() == SqlType.TIMESTAMP
                && "DATE".equals(baseType(column.getSourceType()))) {
            Timestamp timestamp = resultSet.getTimestamp(index);
            return timestamp == null ? null : timestamp.toLocalDateTime();
        }

        return super.readValue(resultSet, index, column);
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        String base = baseType(column.getSourceType());

        if (isNumberBase(base)
                && column.getDataType().getSqlType() == SqlType.STRING) {
            statement.setNull(index, Types.NUMERIC);
            return;
        }

        if ("TIME".equals(base)
                && column.getDataType().getSqlType() == SqlType.STRING) {
            statement.setNull(index, Types.TIME);
            return;
        }

        if ("CLOB".equals(base)) {
            statement.setNull(index, Types.CLOB);
            return;
        }
        if ("NCLOB".equals(base)) {
            statement.setNull(index, Types.NCLOB);
            return;
        }

        if (isBlobBase(base)
                || (column.getDataType().getSqlType() == SqlType.BYTES
                && (column.getLength() == null
                || column.getLength() > YashanDbTypeMapper.MAX_RAW_COLUMN_LENGTH))) {
            statement.setNull(index, Types.BLOB);
            return;
        }

        if (column.getDataType().getSqlType() == SqlType.STRING
                && !isNativeTextValue(base)
                && (column.getLength() == null
                || column.getLength() > YashanDbTypeMapper.MAX_SAFE_VARCHAR_CHARS)) {
            statement.setNull(index, Types.CLOB);
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

        String base = baseType(column.getSourceType());

        if (isNumberBase(base)
                && column.getDataType().getSqlType() == SqlType.STRING) {
            statement.setBigDecimal(index, new BigDecimal(asString(value).trim()));
            return;
        }

        if ("TIME".equals(base)
                && column.getDataType().getSqlType() == SqlType.STRING) {
            // YashanDB TIME can represent +/-838 hours, which java.time.LocalTime
            // cannot. String binding keeps the native value intact.
            statement.setString(index, asString(value));
            return;
        }

        if ("CLOB".equals(base)) {
            String text = asString(value);
            statement.setCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());
            return;
        }

        if ("NCLOB".equals(base)) {
            String text = asString(value);
            statement.setNCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());
            return;
        }

        if (isBlobBase(base)
                || (column.getDataType().getSqlType() == SqlType.BYTES
                && (column.getLength() == null
                || column.getLength() > YashanDbTypeMapper.MAX_RAW_COLUMN_LENGTH))) {
            byte[] bytes = asBytes(value);
            statement.setBinaryStream(
                    index,
                    new ByteArrayInputStream(bytes),
                    bytes.length);
            return;
        }

        if (column.getDataType().getSqlType() == SqlType.STRING
                && !isNativeTextValue(base)
                && (column.getLength() == null
                || column.getLength() > YashanDbTypeMapper.MAX_SAFE_VARCHAR_CHARS)) {
            String text = asString(value);
            statement.setCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());
            return;
        }

        super.writeValue(statement, index, value, column);
    }

    private static boolean isNumberBase(String base) {
        return "NUMBER".equals(base)
                || "NUMERIC".equals(base)
                || "DECIMAL".equals(base)
                || "DEC".equals(base)
                || "FIXED".equals(base);
    }

    private static boolean isBlobBase(String base) {
        return "BLOB".equals(base)
                || "TINYBLOB".equals(base)
                || "MEDIUMBLOB".equals(base)
                || "LONGBLOB".equals(base)
                || "IMAGE".equals(base);
    }

    private static boolean isNativeTextValue(String base) {
        return isNumberBase(base)
                || "TIME".equals(base)
                || (base.startsWith("TIME")
                && !base.startsWith("TIMESTAMP")
                && base.contains("TIME ZONE"))
                || "JSON".equals(base)
                || "JSONB".equals(base)
                || "XMLTYPE".equals(base)
                || "SYS.XMLTYPE".equals(base)
                || "ROWID".equals(base)
                || "UROWID".equals(base)
                || "INTERVAL YEAR TO MONTH".equals(base)
                || "INTERVAL DAY TO SECOND".equals(base)
                || "VECTOR".equals(base);
    }

    private static String baseType(String value) {
        if (value == null) {
            return "";
        }
        return value.trim()
                .toUpperCase(Locale.ROOT)
                .replaceFirst("\\([^)]*\\)", "")
                .replaceAll("\\s+", " ")
                .trim();
    }
}
