package com.link.up.connector.jdbc.core.dialect.db2;

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

/** DB2 row converter with LOB-safe writes. */
public final class Db2JdbcRowConverter extends AbstractJdbcRowConverter {

    private static final long INLINE_LIMIT = 32672L;

    @Override
    public String name() {
        return DatabaseIdentifier.DB2;
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        String sourceType = normalize(column.getSourceType());
        SqlType sqlType = column.getDataType().getSqlType();

        if (isCharacterLob(sourceType)
                || (sqlType == SqlType.STRING
                && (column.getLength() == null || column.getLength() > INLINE_LIMIT)
                && !"DECFLOAT".equals(sourceType))) {
            statement.setNull(index, Types.CLOB);
            return;
        }

        if ("BLOB".equals(sourceType)
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

        String sourceType = normalize(column.getSourceType());
        SqlType sqlType = column.getDataType().getSqlType();

        if (isCharacterLob(sourceType)
                || (sqlType == SqlType.STRING
                && (column.getLength() == null || column.getLength() > INLINE_LIMIT)
                && !"DECFLOAT".equals(sourceType))) {
            String text = asString(value);
            statement.setCharacterStream(index, new StringReader(text), text.length());
            return;
        }

        if ("BLOB".equals(sourceType)
                || (sqlType == SqlType.BYTES
                && (column.getLength() == null || column.getLength() > INLINE_LIMIT))) {
            byte[] bytes = asBytes(value);
            statement.setBinaryStream(index, new ByteArrayInputStream(bytes), bytes.length);
            return;
        }

        // DECFLOAT is intentionally carried as exact text. Binding it as a
        // String lets DB2 convert into a DECFLOAT target without an IEEE-754
        // intermediate representation.
        if ("DECFLOAT".equals(sourceType) && sqlType == SqlType.STRING) {
            statement.setString(index, asString(value));
            return;
        }

        super.writeValue(statement, index, value, column);
    }

    private static boolean isCharacterLob(String type) {
        return "CLOB".equals(type) || "DBCLOB".equals(type);
    }

    private static String normalize(String value) {
        if (value == null) {
            return "";
        }
        String type = value.trim().toUpperCase(Locale.ROOT);
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0 ? type.substring(0, parenthesis).trim() : type;
    }
}
