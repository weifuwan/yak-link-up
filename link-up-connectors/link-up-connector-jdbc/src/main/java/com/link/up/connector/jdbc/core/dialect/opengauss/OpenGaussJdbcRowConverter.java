package com.link.up.connector.jdbc.core.dialect.opengauss;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

/** openGauss row converter with safe binding for server-resolved native types. */
public final class OpenGaussJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.OPENGAUSS;
    }

    @Override
    protected void writeNull(
            PreparedStatement statement,
            int index,
            Column column) throws SQLException {

        if (column.getDataType().getSqlType() == SqlType.STRING
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

        if (column.getDataType().getSqlType() == SqlType.STRING
                && requiresOtherBinding(column.getSourceType())) {
            statement.setObject(index, String.valueOf(value), Types.OTHER);
            return;
        }

        super.writeValue(statement, index, value, column);
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
                || (type.startsWith("time") && type.contains("time zone"))
                || "numeric".equals(base)
                || "decimal".equals(base)
                || "dec".equals(base)
                || "number".equals(base)
                || "fixed".equals(base);
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
