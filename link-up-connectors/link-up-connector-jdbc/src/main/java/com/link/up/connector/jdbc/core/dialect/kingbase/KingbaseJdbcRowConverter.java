package com.link.up.connector.jdbc.core.dialect.kingbase;

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

/** KingbaseES row converter with native-type and LOB-safe writes. */
public final class KingbaseJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String NATIVE_ATTRIBUTE = "kingbase_native";

    @Override
    public String name() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        String type = baseType(normalize(column.getSourceType()));
        if (isNative(column) && isCharacterLob(type)) {
            statement.setNull(index, Types.CLOB);
            return;
        }
        if (isNative(column) && isBinaryLob(type)) {
            statement.setNull(index, Types.BLOB);
            return;
        }
        if (isNative(column)
                && column.getDataType().getSqlType() == SqlType.STRING
                && requiresOtherBinding(column.getSourceType())) {
            statement.setNull(index, Types.OTHER);
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

        String type = baseType(normalize(column.getSourceType()));
        if (isNative(column) && isCharacterLob(type)) {
            String text = asString(value);
            statement.setCharacterStream(index, new StringReader(text), text.length());
            return;
        }
        if (isNative(column) && isBinaryLob(type)) {
            byte[] bytes = asBytes(value);
            statement.setBinaryStream(index, new ByteArrayInputStream(bytes), bytes.length);
            return;
        }
        if (isNative(column)
                && column.getDataType().getSqlType() == SqlType.STRING
                && requiresOtherBinding(column.getSourceType())) {
            statement.setObject(index, String.valueOf(value), Types.OTHER);
            return;
        }
        super.writeValue(statement, index, value, column);
    }

    private static boolean isNative(Column column) {
        return "true".equalsIgnoreCase(column.getAttributes().get(NATIVE_ATTRIBUTE));
    }

    private static boolean requiresOtherBinding(String sourceType) {
        String type = normalize(sourceType);
        String base = baseType(type);
        return "uuid".equals(base)
                || "json".equals(base)
                || "jsonb".equals(base)
                || "xml".equals(base)
                || "inet".equals(base)
                || "cidr".equals(base)
                || base.startsWith("macaddr")
                || "timetz".equals(base)
                || (type.startsWith("time")
                    && !type.startsWith("timestamp")
                    && type.contains("time zone"))
                || "numeric".equals(base)
                || "decimal".equals(base)
                || "number".equals(base)
                || "fixed".equals(base)
                || "money".equals(base)
                || "bit".equals(base)
                || "varbit".equals(base)
                || "bit varying".equals(base)
                || "interval".equals(base);
    }

    private static boolean isCharacterLob(String type) {
        return "clob".equals(type) || "nclob".equals(type);
    }

    private static boolean isBinaryLob(String type) {
        return "blob".equals(type);
    }

    private static String baseType(String type) {
        int parenthesis = type.indexOf('(');
        return parenthesis >= 0 ? type.substring(0, parenthesis).trim() : type;
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }
}
