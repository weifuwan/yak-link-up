package com.link.up.connector.starrocks.client.source.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One native scan partition bound to a StarRocks BE and a group of tablets. */
public final class StarRocksQueryPartition implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;
    private final String beAddress;
    private final List<Long> tabletIds;
    private final String opaquedQueryPlan;

    public StarRocksQueryPartition(
            String database,
            String table,
            String beAddress,
            List<Long> tabletIds,
            String opaquedQueryPlan) {
        this.database = Objects.requireNonNull(database, "database must not be null");
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.beAddress = Objects.requireNonNull(beAddress, "beAddress must not be null");
        if (tabletIds == null || tabletIds.isEmpty()) {
            throw new IllegalArgumentException("tabletIds must not be empty");
        }
        this.tabletIds =
                Collections.unmodifiableList(new ArrayList<Long>(tabletIds));
        this.opaquedQueryPlan =
                Objects.requireNonNull(opaquedQueryPlan, "opaquedQueryPlan must not be null");
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getBeAddress() {
        return beAddress;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public String getOpaquedQueryPlan() {
        return opaquedQueryPlan;
    }
}
