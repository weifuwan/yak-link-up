package com.link.up.connector.jdbc.catalog.oceanbase;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.DatabaseAlreadyExistsException;
import com.link.up.api.table.catalog.exception.DatabaseNotFoundException;
import com.link.up.api.table.catalog.exception.TableAlreadyExistsException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCatalog;
import com.link.up.connector.jdbc.core.dialect.oceanbase.OceanBaseCompatibleMode;
import com.link.up.connector.jdbc.core.dialect.oceanbase.OceanBaseJdbcUrl;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * OceanBase mode-aware Catalog facade.
 *
 * <p>MySQL-compatible mode reuses the mature MySQL Catalog through the MySQL
 * wire protocol. Oracle-compatible mode uses the Oracle metadata/DDL rules
 * with the OceanBase JDBC driver.</p>
 */
public final class OceanBaseCatalog
        implements WritableCatalog {

    public static final String DIALECT = "oceanbase";

    private static final String MYSQL_DRIVER =
            "com.mysql.cj.jdbc.Driver";

    private final String catalogName;
    private final OceanBaseCompatibleMode mode;
    private final String defaultDatabase;
    private final WritableCatalog delegate;

    public OceanBaseCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            OceanBaseCompatibleMode mode,
            String defaultSchema) {

        if (catalogName == null
                || catalogName.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "catalogName must not be empty");
        }

        this.catalogName =
                catalogName.trim();

        this.mode =
                Objects.requireNonNull(
                        mode,
                        "mode must not be null");

        Objects.requireNonNull(
                config,
                "config must not be null");

        this.defaultDatabase =
                OceanBaseJdbcUrl.databaseName(
                        config.getUrl());

        if (mode.isMySql()) {
            JdbcCatalogConfig mysqlConfig =
                    new JdbcCatalogConfig(
                            OceanBaseJdbcUrl.toMySqlUrl(
                                    config.getUrl()),
                            config.getUsername(),
                            config.getPassword(),
                            MYSQL_DRIVER,
                            config.getProperties(),
                            config.isIntTypeNarrowing());

            this.delegate =
                    new MySqlCatalog(
                            catalogName,
                            mysqlConfig);
        } else {
            this.delegate =
                    new OceanBaseOracleCatalog(
                            catalogName,
                            config,
                            defaultSchema);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public void open()
            throws CatalogException {

        delegate.open();
    }

    @Override
    public void close()
            throws CatalogException {

        delegate.close();
    }

    @Override
    public Optional<String> getDefaultDatabase()
            throws CatalogException {

        return defaultDatabase == null
                ? delegate.getDefaultDatabase()
                : Optional.of(defaultDatabase);
    }

    @Override
    public List<String> listDatabases()
            throws CatalogException {

        return delegate.listDatabases();
    }

    @Override
    public List<String> listSchemas(
            String databaseName)
            throws CatalogException {

        return delegate.listSchemas(
                normalizeDatabase(
                        databaseName));
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName)
            throws CatalogException {

        if (mode.isMySql()) {
            return delegate.listTables(
                    normalizeDatabase(
                            databaseName),
                    null);
        }

        return delegate.listTables(
                normalizeDatabase(
                        databaseName),
                schemaName);
    }

    @Override
    public boolean tableExists(
            TablePath tablePath)
            throws CatalogException {

        return delegate.tableExists(
                normalizePath(
                        tablePath));
    }

    @Override
    public CatalogTable getTable(
            TablePath tablePath)
            throws CatalogException,
            TableNotFoundException {

        return delegate.getTable(
                normalizePath(
                        tablePath));
    }

    @Override
    public void createDatabase(
            String databaseName,
            boolean ignoreIfExists)
            throws CatalogException,
            DatabaseAlreadyExistsException {

        delegate.createDatabase(
                databaseName,
                ignoreIfExists);
    }

    @Override
    public void dropDatabase(
            String databaseName,
            boolean ignoreIfNotExists)
            throws CatalogException,
            DatabaseNotFoundException {

        delegate.dropDatabase(
                databaseName,
                ignoreIfNotExists);
    }

    @Override
    public void createTable(
            CatalogTable table,
            boolean ignoreIfExists)
            throws CatalogException,
            DatabaseNotFoundException,
            TableAlreadyExistsException {

        Objects.requireNonNull(
                table,
                "table must not be null");

        TablePath targetPath =
                normalizePath(
                        table.getTablePath());

        delegate.createTable(
                table.getTablePath()
                        .equals(targetPath)
                        ? table
                        : table.withPath(
                                targetPath),
                ignoreIfExists);
    }

    @Override
    public void addColumn(
            TablePath tablePath,
            Column column)
            throws CatalogException,
            TableNotFoundException {

        delegate.addColumn(
                normalizePath(
                        tablePath),
                column);
    }

    @Override
    public void dropTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException,
            TableNotFoundException {

        delegate.dropTable(
                normalizePath(
                        tablePath),
                ignoreIfNotExists);
    }

    @Override
    public void truncateTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException,
            TableNotFoundException {

        delegate.truncateTable(
                normalizePath(
                        tablePath),
                ignoreIfNotExists);
    }

    private TablePath normalizePath(
            TablePath tablePath) {

        Objects.requireNonNull(
                tablePath,
                "tablePath must not be null");

        if (mode.isOracle()) {
            /*
             * Oracle-compatible delegate resolves schema and guarantees that
             * a foreign source database is not leaked into target SQL.
             */
            return tablePath;
        }

        String database =
                defaultDatabase;

        if (database == null
                || database.trim().isEmpty()) {

            database =
                    tablePath.getDatabaseName();
        }

        if ((database == null
                || database.trim().isEmpty())
                && tablePath.getSchemaName()
                != null) {

            database =
                    tablePath.getSchemaName();
        }

        if (database == null
                || database.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "OceanBase MySQL 模式缺少目标 database，"
                            + "请在 JDBC URL 或 table path 中指定");
        }

        return TablePath.of(
                database.trim(),
                tablePath.getTableName());
    }

    private String normalizeDatabase(
            String requested) {

        if (defaultDatabase == null
                || defaultDatabase.trim().isEmpty()) {

            return requested;
        }

        /*
         * A configured URL database is the target/source database for this
         * bounded connector instance. It also prevents a foreign source path
         * from accidentally routing writes into another OceanBase database.
         */
        return defaultDatabase;
    }
}
