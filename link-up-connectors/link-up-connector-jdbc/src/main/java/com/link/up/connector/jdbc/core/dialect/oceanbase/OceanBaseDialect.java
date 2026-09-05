package com.link.up.connector.jdbc.core.dialect.oceanbase;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.oceanbase.OceanBaseCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;
import com.link.up.connector.jdbc.core.dialect.mysql.MySqlJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.mysql.MySqlTypeMapper;
import com.link.up.connector.jdbc.core.dialect.oracle.OracleJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.oracle.OracleTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * OceanBase bounded/offline JDBC dialect.
 *
 * <p>The connector stays OceanBase-aware at the SPI boundary while reusing
 * the already-tested MySQL or Oracle value/type/SQL semantics selected by
 * {@code compatible_mode}. CDC, LogProxy/CLog and streaming state are
 * intentionally outside this dialect.</p>
 */
public final class OceanBaseDialect
        implements JdbcDialect {

    private static final int ORACLE_DEFAULT_FETCH_SIZE = 128;

    private final JdbcConnectionConfig connectionConfig;
    private final OceanBaseCompatibleMode mode;
    private final JdbcTypeMapper typeMapper;
    private final String databaseName;

    public OceanBaseDialect(
            JdbcConnectionConfig connectionConfig) {

        if (connectionConfig == null) {
            throw new IllegalArgumentException(
                    "connectionConfig must not be null");
        }

        if (!OceanBaseJdbcUrl.accepts(
                connectionConfig.getUrl())) {

            throw new IllegalArgumentException(
                    "非法 OceanBase JDBC URL："
                            + connectionConfig.getUrl());
        }

        this.connectionConfig =
                connectionConfig;

        this.mode =
                OceanBaseCompatibleMode.from(
                        connectionConfig
                                .getCompatibleMode());

        this.databaseName =
                OceanBaseJdbcUrl.databaseName(
                        connectionConfig.getUrl());

        this.typeMapper =
                mode.isMySql()
                        ? new MySqlTypeMapper(false)
                        : new OracleTypeMapper();
    }

    public OceanBaseCompatibleMode compatibleMode() {
        return mode;
    }

    @Override
    public String name() {
        return DatabaseIdentifier.OCEANBASE;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        OceanBaseCompatibleMode catalogMode =
                OceanBaseCompatibleMode.from(
                        connectionConfig
                                .getCompatibleMode());

        return new OceanBaseCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        connectionConfig.getUrl(),
                        connectionConfig.getUsername(),
                        connectionConfig.getPassword(),
                        connectionConfig.getDriverName(),
                        connectionConfig.getProperties(),
                        false),
                catalogMode,
                connectionConfig.getSchema());
    }

    @Override
    public JdbcTypeMapper typeMapper() {
        return typeMapper;
    }

    @Override
    public JdbcRowConverter rowConverter() {
        JdbcRowConverter delegate =
                mode.isMySql()
                        ? new MySqlJdbcRowConverter()
                        : new OracleJdbcRowConverter();

        return new OceanBaseJdbcRowConverter(
                delegate);
    }

    @Override
    public Set<ReadConsistency>
    supportedReadConsistencies() {

        if (mode.isMySql()) {
            return EnumSet.of(
                    ReadConsistency.BEST_EFFORT,
                    ReadConsistency
                            .SINGLE_CONNECTION_SNAPSHOT);
        }

        return Collections.singleton(
                ReadConsistency.BEST_EFFORT);
    }

    @Override
    public TablePath parseTablePath(
            String tablePath) {

        if (mode.isMySql()) {
            return TablePath.parse(tablePath);
        }

        if (!JdbcDialect.hasText(
                tablePath)) {

            throw new IllegalArgumentException(
                    "tablePath must not be empty");
        }

        String[] parts =
                tablePath.trim()
                        .split("\\.");

        switch (parts.length) {
            case 1:
                return TablePath.of(
                        parts[0]);

            case 2:
                return TablePath.of(
                        null,
                        parts[0],
                        parts[1]);

            case 3:
                return TablePath.of(
                        parts[0],
                        parts[1],
                        parts[2]);

            default:
                throw new IllegalArgumentException(
                        "非法 OceanBase Oracle 表路径："
                                + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(
            String identifier) {

        if (!JdbcDialect.hasText(
                identifier)) {

            throw new IllegalArgumentException(
                    "identifier must not be empty");
        }

        String value =
                identifier.trim();

        if (mode.isMySql()) {
            return "`"
                    + value.replace(
                    "`",
                    "``")
                    + "`";
        }

        return "\""
                + value.replace(
                "\"",
                "\"\"")
                + "\"";
    }

    @Override
    public String tableIdentifier(
            TablePath tablePath) {

        if (tablePath == null) {
            throw new IllegalArgumentException(
                    "tablePath must not be null");
        }

        if (mode.isMySql()) {
            String database =
                    resolveMySqlDatabase(
                            tablePath);

            return quoteIdentifier(
                    database)
                    + "."
                    + quoteIdentifier(
                    tablePath.getTableName());
        }

        return quoteIdentifier(
                resolveOracleSchema(
                        tablePath))
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    @Override
    public Optional<String> buildUpsertSql(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {

        return mode.isMySql()
                ? buildMySqlUpsert(
                tablePath,
                fieldNames,
                primaryKeys)
                : buildOracleUpsert(
                tablePath,
                fieldNames,
                primaryKeys);
    }

    private Optional<String> buildMySqlUpsert(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {

        JdbcDialect.validateFields(
                fieldNames);

        Set<String> primaryKeySet =
                JdbcDialect.normalizeFields(
                        primaryKeys);

        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException(
                    "OceanBase MySQL UPSERT 必须配置主键字段");
        }

        validatePrimaryKeys(
                fieldNames,
                primaryKeys,
                "OceanBase MySQL");

        List<String> updateFields =
                fieldNames.stream()
                        .filter(
                                field ->
                                        !primaryKeySet.contains(
                                                field))
                        .collect(
                                Collectors.toList());

        if (updateFields.isEmpty()) {
            updateFields =
                    Collections.singletonList(
                            primaryKeys.get(0));
        }

        String updateClause =
                updateFields.stream()
                        .map(
                                field ->
                                        quoteIdentifier(
                                                field)
                                                + " = VALUES("
                                                + quoteIdentifier(
                                                field)
                                                + ")")
                        .collect(
                                Collectors.joining(
                                        ", "));

        return Optional.of(
                buildInsertSql(
                        tablePath,
                        fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause);
    }

    private Optional<String> buildOracleUpsert(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {

        JdbcDialect.validateFields(
                fieldNames);

        Set<String> primaryKeySet =
                JdbcDialect.normalizeFields(
                        primaryKeys);

        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException(
                    "OceanBase Oracle UPSERT 必须配置主键字段");
        }

        validatePrimaryKeys(
                fieldNames,
                primaryKeys,
                "OceanBase Oracle");

        String sourceProjection =
                fieldNames.stream()
                        .map(
                                field ->
                                        "? AS "
                                                + quoteIdentifier(
                                                field))
                        .collect(
                                Collectors.joining(
                                        ", "));

        String onClause =
                primaryKeys.stream()
                        .map(
                                field ->
                                        "TARGET."
                                                + quoteIdentifier(
                                                field)
                                                + " = SOURCE."
                                                + quoteIdentifier(
                                                field))
                        .collect(
                                Collectors.joining(
                                        " AND "));

        List<String> updateFields =
                fieldNames.stream()
                        .filter(
                                field ->
                                        !primaryKeySet.contains(
                                                field))
                        .collect(
                                Collectors.toList());

        StringBuilder sql =
                new StringBuilder()
                        .append("MERGE INTO ")
                        .append(
                                tableIdentifier(
                                        tablePath))
                        .append(
                                " TARGET USING (SELECT ")
                        .append(
                                sourceProjection)
                        .append(
                                " FROM DUAL) SOURCE ON (")
                        .append(
                                onClause)
                        .append(")");

        if (!updateFields.isEmpty()) {
            String updateClause =
                    updateFields.stream()
                            .map(
                                    field ->
                                            "TARGET."
                                                    + quoteIdentifier(
                                                    field)
                                                    + " = SOURCE."
                                                    + quoteIdentifier(
                                                    field))
                            .collect(
                                    Collectors.joining(
                                            ", "));

            sql.append(
                    " WHEN MATCHED THEN UPDATE SET ")
                    .append(
                            updateClause);
        }

        String insertFields =
                fieldNames.stream()
                        .map(
                                this::quoteIdentifier)
                        .collect(
                                Collectors.joining(
                                        ", "));

        String insertValues =
                fieldNames.stream()
                        .map(
                                field ->
                                        "SOURCE."
                                                + quoteIdentifier(
                                                field))
                        .collect(
                                Collectors.joining(
                                        ", "));

        sql.append(
                " WHEN NOT MATCHED THEN INSERT (")
                .append(
                        insertFields)
                .append(
                        ") VALUES (")
                .append(
                        insertValues)
                .append(")");

        return Optional.of(
                sql.toString());
    }

    @Override
    public PreparedStatement prepareReadStatement(
            Connection connection,
            String sql,
            int fetchSize)
            throws SQLException {

        PreparedStatement statement =
                connection.prepareStatement(
                        sql,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);

        if (mode.isMySql()) {
            /*
             * OceanBase's MySQL-compatible JDBC path follows the MySQL
             * streaming convention used by SeaTunnel's OceanBase dialect.
             */
            statement.setFetchSize(
                    Integer.MIN_VALUE);
            return statement;
        }

        statement.setFetchSize(
                fetchSize > 0
                        ? fetchSize
                        : ORACLE_DEFAULT_FETCH_SIZE);

        return statement;
    }

    @Override
    public Map<String, String>
    defaultConnectionProperties() {

        if (mode.isOracle()) {
            return Collections.emptyMap();
        }

        Map<String, String> result =
                new LinkedHashMap<String, String>();

        result.put(
                "rewriteBatchedStatements",
                "true");

        result.put(
                "allowMultiQueries",
                "true");

        return Collections.unmodifiableMap(
                result);
    }

    @Override
    public Optional<String>
    buildHashPartitionPredicate(
            Column column,
            int bucket,
            int bucketCount) {

        if (column == null) {
            throw new IllegalArgumentException(
                    "column must not be null");
        }

        if (bucketCount <= 0) {
            throw new IllegalArgumentException(
                    "bucketCount must be greater than 0");
        }

        if (bucket < 0
                || bucket >= bucketCount) {

            throw new IllegalArgumentException(
                    "bucket must be between 0 and bucketCount - 1");
        }

        String field =
                quoteIdentifier(
                        column.getName());

        if (mode.isMySql()) {
            return Optional.of(
                    "MOD(CRC32(CAST("
                            + field
                            + " AS CHAR)), "
                            + bucketCount
                            + ") = "
                            + bucket);
        }

        return Optional.of(
                "MOD(ORA_HASH("
                        + field
                        + "), "
                        + bucketCount
                        + ") = "
                        + bucket);
    }

    private String resolveMySqlDatabase(
            TablePath tablePath) {

        if (JdbcDialect.hasText(
                databaseName)) {

            return databaseName.trim();
        }

        if (JdbcDialect.hasText(
                connectionConfig.getSchema())) {

            return connectionConfig
                    .getSchema()
                    .trim();
        }

        if (JdbcDialect.hasText(
                tablePath.getDatabaseName())) {

            return tablePath
                    .getDatabaseName()
                    .trim();
        }

        if (JdbcDialect.hasText(
                tablePath.getSchemaName())) {

            return tablePath
                    .getSchemaName()
                    .trim();
        }

        throw new IllegalArgumentException(
                "OceanBase MySQL 表路径缺少 database，"
                        + "且 JDBC URL 未指定默认 database："
                        + tablePath);
    }

    private String resolveOracleSchema(
            TablePath tablePath) {

        String pathDatabase =
                tablePath.getDatabaseName();

        String pathSchema =
                tablePath.getSchemaName();

        if (JdbcDialect.hasText(
                pathSchema)
                && (!JdbcDialect.hasText(
                pathDatabase)
                || (JdbcDialect.hasText(
                databaseName)
                && databaseName
                .equalsIgnoreCase(
                        pathDatabase)))) {

            return pathSchema.trim();
        }

        if (JdbcDialect.hasText(
                connectionConfig.getSchema())) {

            return connectionConfig
                    .getSchema()
                    .trim();
        }

        if (JdbcDialect.hasText(
                connectionConfig
                        .getUsername())) {

            return connectionConfig
                    .getUsername()
                    .trim()
                    .toUpperCase(
                            Locale.ROOT);
        }

        if (JdbcDialect.hasText(
                pathSchema)) {

            return pathSchema.trim();
        }

        throw new IllegalArgumentException(
                "OceanBase Oracle 表路径缺少 schema，"
                        + "且 connection schema/username 均未配置："
                        + tablePath);
    }

    private static void validatePrimaryKeys(
            List<String> fieldNames,
            List<String> primaryKeys,
            String database) {

        Set<String> fields =
                new HashSet<String>(
                        fieldNames);

        for (String primaryKey :
                primaryKeys) {

            if (!fields.contains(
                    primaryKey)) {

                throw new IllegalArgumentException(
                        database
                                + " UPSERT 主键字段不存在："
                                + primaryKey);
            }
        }
    }
}
