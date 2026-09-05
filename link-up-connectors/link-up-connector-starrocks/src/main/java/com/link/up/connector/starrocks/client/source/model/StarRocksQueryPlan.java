package com.link.up.connector.starrocks.client.source.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** Query plan returned by the StarRocks FE native scan endpoint. */
public final class StarRocksQueryPlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private int status;

    @JsonProperty("opaqued_query_plan")
    private String opaquedQueryPlan;

    private Map<String, StarRocksTablet> partitions =
            new LinkedHashMap<String, StarRocksTablet>();

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getOpaquedQueryPlan() {
        return opaquedQueryPlan;
    }

    public void setOpaquedQueryPlan(String opaquedQueryPlan) {
        this.opaquedQueryPlan = opaquedQueryPlan;
    }

    public Map<String, StarRocksTablet> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, StarRocksTablet> partitions) {
        this.partitions = partitions;
    }
}
