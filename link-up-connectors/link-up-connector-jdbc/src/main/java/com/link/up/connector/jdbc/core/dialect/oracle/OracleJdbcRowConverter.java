package com.link.up.connector.jdbc.core.dialect.oracle;

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

/**
 * Oracle JDBC row converter.
 *
 * <p>The common converter handles ordinary primitives. This class only adds
 * Oracle LOB binding and NUMBER(1)-compatible Boolean writes.</p>
 */
public final class OracleJdbcRowConverter
        extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column)
            throws SQLException {

        String sourceType =
                normalize(
                        column.getSourceType());

        if ("CLOB".equals(sourceType)) {
            statement.setNull(
                    index,
                    Types.CLOB);
            return;
        }

        if ("NCLOB".equals(sourceType)) {
            statement.setNull(
                    index,
                    Types.NCLOB);
            return;
        }

        if ("BLOB".equals(sourceType)
                || "BFILE".equals(sourceType)
                || (column.getDataType()
                .getSqlType()
                == SqlType.BYTES
                && (column.getLength() == null
                || column.getLength() > 2000L))) {

            statement.setNull(
                    index,
                    Types.BLOB);
            return;
        }

        if (sourceType.startsWith("RAW")) {
            statement.setNull(
                    index,
                    Types.VARBINARY);
            return;
        }

        if (column.getDataType()
                .getSqlType()
                == SqlType.STRING
                && (column.getLength() == null
                || column.getLength() > 4000L)) {

            statement.setNull(
                    index,
                    Types.CLOB);
            return;
        }

        if (column.getDataType()
                .getSqlType()
                == SqlType.BOOLEAN) {

            statement.setNull(
                    index,
                    Types.NUMERIC);
            return;
        }

        super.writeNull(
                statement,
                index,
                column);
    }

    @Override
    protected void writeValue(
            PreparedStatement statement,
            int index,
            Object value,
            Column column)
            throws SQLException {

        String sourceType =
                normalize(
                        column.getSourceType());

        if ("CLOB".equals(sourceType)) {
            String text =
                    asString(value);

            statement.setCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());

            return;
        }

        if ("NCLOB".equals(sourceType)) {
            String text =
                    asString(value);

            statement.setNCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());

            return;
        }

        if ("BLOB".equals(sourceType)
                || "BFILE".equals(sourceType)
                || (column.getDataType()
                .getSqlType()
                == SqlType.BYTES
                && (column.getLength() == null
                || column.getLength() > 2000L))) {

            byte[] bytes =
                    asBytes(value);

            statement.setBinaryStream(
                    index,
                    new ByteArrayInputStream(
                            bytes),
                    bytes.length);

            return;
        }

        if (column.getDataType()
                .getSqlType()
                == SqlType.STRING
                && (column.getLength() == null
                || column.getLength() > 4000L)) {

            String text =
                    asString(value);

            statement.setCharacterStream(
                    index,
                    new StringReader(text),
                    text.length());

            return;
        }

        if (column.getDataType()
                .getSqlType()
                == SqlType.BOOLEAN) {

            statement.setInt(
                    index,
                    asBoolean(value)
                            ? 1
                            : 0);

            return;
        }

        super.writeValue(
                statement,
                index,
                value,
                column);
    }

    private static String normalize(
            String value) {

        if (value == null) {
            return "";
        }

        String normalized =
                value.trim()
                        .toUpperCase(
                                Locale.ROOT);

        int parenthesis =
                normalized.indexOf('(');

        return parenthesis >= 0
                ? normalized.substring(
                        0,
                        parenthesis)
                        .trim()
                : normalized;
    }
}
