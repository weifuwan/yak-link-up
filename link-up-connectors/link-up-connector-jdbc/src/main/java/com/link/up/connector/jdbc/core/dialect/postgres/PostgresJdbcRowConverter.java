package com.link.up.connector.jdbc.core.dialect.postgres;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

/**
 * PostgreSQL JDBC 行转换器。
 *
 * <p>UUID、JSON/JSONB 等 PostgreSQL OTHER 类型以 unknown 参数绑定，
 * 由服务端根据目标字段类型完成转换；普通关系型类型继续复用公共转换器。
 */
public final class PostgresJdbcRowConverter
        extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    protected void writeValue(
            PreparedStatement statement,
            int index,
            Object value,
            Column column)
            throws SQLException {

        if (column.getDataType().getSqlType() == SqlType.STRING
                && requiresOtherBinding(column.getSourceType())) {

            statement.setObject(
                    index,
                    String.valueOf(value),
                    Types.OTHER);
            return;
        }

        super.writeValue(
                statement,
                index,
                value,
                column);
    }

    private static boolean requiresOtherBinding(
            String sourceType) {

        if (sourceType == null) {
            return false;
        }

        String normalized =
                sourceType.trim()
                        .toLowerCase(Locale.ROOT);

        return "uuid".equals(normalized)
                || "json".equals(normalized)
                || "jsonb".equals(normalized)
                || "xml".equals(normalized)
                || "inet".equals(normalized)
                || "cidr".equals(normalized)
                || "macaddr".equals(normalized)
                || "macaddr8".equals(normalized);
    }
}
