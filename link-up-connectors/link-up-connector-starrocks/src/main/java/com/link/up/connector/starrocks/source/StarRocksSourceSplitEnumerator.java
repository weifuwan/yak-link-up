package com.link.up.connector.starrocks.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.connector.starrocks.client.source.StarRocksQueryPlanClient;
import com.link.up.connector.starrocks.client.source.StarRocksSplitPlanner;
import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPartition;
import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPlan;
import com.link.up.connector.starrocks.config.StarRocksSourceConfig;
import com.link.up.connector.starrocks.config.StarRocksSourceTableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Bounded native split enumerator backed by StarRocks FE query plans. */
public final class StarRocksSourceSplitEnumerator
        implements SourceSplitEnumerator<StarRocksSourceSplit> {

    private final StarRocksSourceConfig config;
    private final StarRocksQueryPlanClient queryPlanClient;

    public StarRocksSourceSplitEnumerator(StarRocksSourceConfig config) {
        this(config, new StarRocksQueryPlanClient(config));
    }

    StarRocksSourceSplitEnumerator(
            StarRocksSourceConfig config,
            StarRocksQueryPlanClient queryPlanClient) {
        this.config = config;
        this.queryPlanClient = queryPlanClient;
    }

    @Override
    public List<StarRocksSourceSplit> enumerateSplits() throws Exception {
        List<StarRocksSourceSplit> result = new ArrayList<StarRocksSourceSplit>();
        for (StarRocksSourceTableConfig table : config.getTableConfigs()) {
            StarRocksQueryPlan queryPlan = queryPlanClient.fetchQueryPlan(table);
            List<StarRocksQueryPartition> partitions =
                    StarRocksSplitPlanner.plan(
                            table.getDatabase(),
                            table.getTable(),
                            queryPlan,
                            config.getRequestTabletSize());
            int partitionIndex = 0;
            for (StarRocksQueryPartition partition : partitions) {
                String splitId =
                        table.getDatabase()
                                + "."
                                + table.getTable()
                                + "@"
                                + partition.getBeAddress()
                                + "#"
                                + partitionIndex++;
                result.add(
                        new StarRocksSourceSplit(
                                splitId,
                                table.getTablePath(),
                                partition));
            }
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public void close() {
        queryPlanClient.close();
    }
}
