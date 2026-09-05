package com.link.up.connector.jdbc.core.dialect.oceanbase;

import java.util.Locale;

/**
 * OceanBase compatibility mode.
 *
 * <p>The mode is explicit because MySQL and Oracle modes differ in quoting,
 * table paths, type mapping and UPSERT syntax. Guessing from data or SQL can
 * silently generate incorrect statements.</p>
 */
public enum OceanBaseCompatibleMode {
    MYSQL,
    ORACLE;

    public static OceanBaseCompatibleMode from(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "OceanBase 必须配置 compatible_mode=mysql|oracle");
        }

        String normalized =
                value.trim().toUpperCase(Locale.ROOT);

        if ("MYSQL".equals(normalized)) {
            return MYSQL;
        }

        if ("ORACLE".equals(normalized)) {
            return ORACLE;
        }

        throw new IllegalArgumentException(
                "不支持的 OceanBase compatible_mode："
                        + value
                        + "，仅支持 mysql、oracle");
    }

    public boolean isMySql() {
        return this == MYSQL;
    }

    public boolean isOracle() {
        return this == ORACLE;
    }
}
