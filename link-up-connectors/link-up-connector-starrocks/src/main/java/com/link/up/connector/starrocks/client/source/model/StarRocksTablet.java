package com.link.up.connector.starrocks.client.source.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Tablet routing returned by the StarRocks FE query-plan endpoint. */
public final class StarRocksTablet implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<String> routings = new ArrayList<String>();
    private int version;
    private long versionHash;
    private long schemaHash;

    public List<String> getRoutings() {
        return routings == null
                ? Collections.<String>emptyList()
                : routings;
    }

    public void setRoutings(List<String> routings) {
        this.routings = routings;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getVersionHash() {
        return versionHash;
    }

    public void setVersionHash(long versionHash) {
        this.versionHash = versionHash;
    }

    public long getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(long schemaHash) {
        this.schemaHash = schemaHash;
    }
}
