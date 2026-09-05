package com.link.up.connector.starrocks.client.source;

import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPartition;
import com.link.up.connector.starrocks.client.source.model.StarRocksQueryPlan;
import com.link.up.connector.starrocks.client.source.model.StarRocksTablet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Converts FE tablet routing into deterministic BE-bound native scan partitions. */
public final class StarRocksSplitPlanner {

    private StarRocksSplitPlanner() {
    }

    public static List<StarRocksQueryPartition> plan(
            String database,
            String table,
            StarRocksQueryPlan queryPlan,
            int requestTabletSize) {

        if (queryPlan == null) {
            throw new IllegalArgumentException("queryPlan must not be null");
        }
        if (requestTabletSize <= 0) {
            throw new IllegalArgumentException("requestTabletSize must be greater than 0");
        }
        if (!hasText(queryPlan.getOpaquedQueryPlan())) {
            throw new IllegalArgumentException("StarRocks query plan does not contain opaqued_query_plan");
        }

        Map<String, StarRocksTablet> tablets = queryPlan.getPartitions();
        if (tablets == null || tablets.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map.Entry<String, StarRocksTablet>> orderedTablets =
                new ArrayList<Map.Entry<String, StarRocksTablet>>(tablets.entrySet());
        Collections.sort(
                orderedTablets,
                new Comparator<Map.Entry<String, StarRocksTablet>>() {
                    @Override
                    public int compare(
                            Map.Entry<String, StarRocksTablet> left,
                            Map.Entry<String, StarRocksTablet> right) {
                        return compareTabletId(left.getKey(), right.getKey());
                    }
                });

        Map<String, List<Long>> beToTablets = new LinkedHashMap<String, List<Long>>();
        for (Map.Entry<String, StarRocksTablet> entry : orderedTablets) {
            long tabletId = parseTabletId(entry.getKey());
            String be = selectBackend(entry.getValue(), beToTablets);
            List<Long> assigned = beToTablets.get(be);
            if (assigned == null) {
                assigned = new ArrayList<Long>();
                beToTablets.put(be, assigned);
            }
            assigned.add(tabletId);
        }

        List<String> backends = new ArrayList<String>(beToTablets.keySet());
        Collections.sort(backends);

        List<StarRocksQueryPartition> result = new ArrayList<StarRocksQueryPartition>();
        for (String backend : backends) {
            List<Long> tabletIds =
                    new ArrayList<Long>(new LinkedHashSet<Long>(beToTablets.get(backend)));
            Collections.sort(tabletIds);
            int offset = 0;
            while (offset < tabletIds.size()) {
                int end = Math.min(tabletIds.size(), offset + requestTabletSize);
                result.add(
                        new StarRocksQueryPartition(
                                database,
                                table,
                                backend,
                                tabletIds.subList(offset, end),
                                queryPlan.getOpaquedQueryPlan()));
                offset = end;
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static String selectBackend(
            StarRocksTablet tablet,
            Map<String, List<Long>> assigned) {
        if (tablet == null || tablet.getRoutings() == null || tablet.getRoutings().isEmpty()) {
            throw new IllegalArgumentException("StarRocks tablet has no BE routing");
        }

        List<String> candidates = new ArrayList<String>();
        for (String routing : tablet.getRoutings()) {
            if (hasText(routing)) {
                candidates.add(routing.trim());
            }
        }
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("StarRocks tablet has no valid BE routing");
        }
        Collections.sort(candidates);

        String selected = null;
        int selectedLoad = Integer.MAX_VALUE;
        for (String candidate : candidates) {
            List<Long> current = assigned.get(candidate);
            int load = current == null ? 0 : current.size();
            if (selected == null || load < selectedLoad) {
                selected = candidate;
                selectedLoad = load;
            }
        }
        return selected;
    }

    private static int compareTabletId(String left, String right) {
        try {
            return Long.compare(Long.parseLong(left), Long.parseLong(right));
        } catch (NumberFormatException ignored) {
            return String.valueOf(left).compareTo(String.valueOf(right));
        }
    }

    private static long parseTabletId(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid StarRocks tablet id: " + value, e);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
