package com.link.up.connector.starrocks.client.source;

import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPartition;
import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPlan;
import com.link.up.connector.starrocks.client.source.model.StarRocksTablet;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StarRocksSplitPlannerTest {

    @Test
    public void plansTabletsAcrossCandidateBackendsDeterministically() {
        StarRocksQueryPlan plan = new StarRocksQueryPlan();
        plan.setOpaquedQueryPlan("opaque-plan");

        Map<String, StarRocksTablet> tablets =
                new LinkedHashMap<String, StarRocksTablet>();
        tablets.put("3", tablet("be-b:9060", "be-a:9060"));
        tablets.put("1", tablet("be-a:9060", "be-b:9060"));
        tablets.put("2", tablet("be-a:9060", "be-b:9060"));
        tablets.put("4", tablet("be-a:9060", "be-b:9060"));
        plan.setPartitions(tablets);

        List<StarRocksQueryPartition> partitions =
                StarRocksSplitPlanner.plan("demo", "orders", plan, 1);

        assertEquals(4, partitions.size());
        assertEquals("be-a:9060", partitions.get(0).getBeAddress());
        assertEquals(Arrays.asList(1L), partitions.get(0).getTabletIds());
        assertEquals("be-a:9060", partitions.get(1).getBeAddress());
        assertEquals(Arrays.asList(3L), partitions.get(1).getTabletIds());
        assertEquals("be-b:9060", partitions.get(2).getBeAddress());
        assertEquals(Arrays.asList(2L), partitions.get(2).getTabletIds());
        assertEquals("be-b:9060", partitions.get(3).getBeAddress());
        assertEquals(Arrays.asList(4L), partitions.get(3).getTabletIds());
    }

    @Test
    public void chunksTabletGroupsByRequestSize() {
        StarRocksQueryPlan plan = new StarRocksQueryPlan();
        plan.setOpaquedQueryPlan("opaque-plan");
        Map<String, StarRocksTablet> tablets =
                new LinkedHashMap<String, StarRocksTablet>();
        for (int i = 1; i <= 5; i++) {
            tablets.put(String.valueOf(i), tablet("be-a:9060"));
        }
        plan.setPartitions(tablets);

        List<StarRocksQueryPartition> partitions =
                StarRocksSplitPlanner.plan("demo", "orders", plan, 2);

        assertEquals(3, partitions.size());
        assertEquals(Arrays.asList(1L, 2L), partitions.get(0).getTabletIds());
        assertEquals(Arrays.asList(3L, 4L), partitions.get(1).getTabletIds());
        assertEquals(Arrays.asList(5L), partitions.get(2).getTabletIds());
    }

    private static StarRocksTablet tablet(String... routings) {
        StarRocksTablet tablet = new StarRocksTablet();
        tablet.setRoutings(Arrays.asList(routings));
        return tablet;
    }
}
