package com.link.up.connector.jdbc.core.dialect;

/**
 * JDBC 数据库方言标识。
 * <p>
 * 标识统一使用小写，便于配置和 SPI 匹配。
 */
public final class DatabaseIdentifier {

    public static final String MYSQL = "mysql";
    public static final String POSTGRESQL = "postgresql";
    public static final String ORACLE = "oracle";
    public static final String SQLSERVER = "sqlserver";
    public static final String OCEANBASE = "oceanbase";
    public static final String DB2 = "db2";

    private DatabaseIdentifier() {
    }
}
