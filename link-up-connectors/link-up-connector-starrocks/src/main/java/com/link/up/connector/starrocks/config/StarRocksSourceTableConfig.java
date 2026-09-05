package com.link.up.connector.starrocks.config;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;

import java.io.Serializable;
import java.util.Objects;

/** Per-table configuration for StarRocks Native Source. */
public final class StarRocksSourceTableConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;
    private final String scanFilter;
    private final CatalogTable catalogTable;

    public StarRocksSourceTableConfig(
            String database,
            String table,
            String scanFilter,
            TableSchema schema) {
        this.database = requireText(database, "database");
        this.table = requireText(table, "table");
        this.scanFilter = scanFilter == null ? "" : scanFilter.trim();
        Objects.requireNonNull(schema, "schema must not be null");
        this.catalogTable =
                CatalogTable.builder(TablePath.of(this.database, this.table), schema)
                        .option("connector", "starrocks")
                        .option("read_mode", "native")
                        .build();
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getScanFilter() {
        return scanFilter;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    public TablePath getTablePath() {
        return catalogTable.getTablePath();
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }
}
