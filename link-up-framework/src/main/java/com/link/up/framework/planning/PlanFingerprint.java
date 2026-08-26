package com.link.up.framework.planning;

import com.link.up.framework.job.ColumnMapping;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobDefinition;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Creates a deterministic digest without exposing connector option values. */
public final class PlanFingerprint {

    private static final String FORMAT_VERSION =
            "link-up-plan-fingerprint/v1";

    private PlanFingerprint() {
    }

    public static String create(JobDefinition definition) {
        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");

        StringBuilder canonical = new StringBuilder();
        appendToken(canonical, FORMAT_VERSION);
        appendToken(canonical, job.getName());
        appendToken(canonical, job.getSource().getType());
        appendValue(
                canonical,
                job.getSource().getOptions().getSourceMap());
        appendToken(canonical, job.getSink().getType());
        appendValue(
                canonical,
                job.getSink().getOptions().getSourceMap());
        appendExecution(canonical, job.getExecutionConfig());
        appendMapping(canonical, job.getColumnMapping());

        return "sha256:" + sha256(canonical.toString());
    }

    private static void appendExecution(
            StringBuilder canonical,
            ExecutionConfig config) {

        appendToken(canonical, Integer.toString(config.getBatchSize()));
        appendToken(canonical, Integer.toString(config.getSourceParallelism()));
        appendToken(canonical, Integer.toString(config.getSinkParallelism()));
        appendToken(canonical, Integer.toString(config.getPipelineParallelism()));
        appendToken(canonical, Integer.toString(config.getMaxBufferedBatches()));
        appendToken(canonical, Long.toString(config.getMaxBufferedRecords()));
        appendToken(canonical, Long.toString(config.getMaxBufferedBytes()));
        appendToken(canonical, Long.toString(config.getMaxRecordsPerSecond()));
        appendToken(canonical, Long.toString(config.getMaxBytesPerSecond()));
        appendToken(canonical, config.getSinkPartitionStrategy().name());
        appendToken(canonical, config.getSplitAssignmentMode().name());
    }

    private static void appendMapping(
            StringBuilder canonical,
            ColumnMapping mapping) {

        ColumnMapping safe = mapping == null
                ? ColumnMapping.empty()
                : mapping;

        appendToken(canonical, Integer.toString(safe.getColumns().size()));
        for (ColumnMapping.Item item : safe.getColumns()) {
            appendToken(canonical, item.getSource());
            appendToken(canonical, item.getTarget());
        }
    }

    private static void appendValue(
            StringBuilder canonical,
            Object value) {

        if (value == null) {
            appendToken(canonical, "null");
            return;
        }

        if (value instanceof Map) {
            appendMap(canonical, (Map<?, ?>) value);
            return;
        }

        if (value instanceof Set) {
            appendSet(canonical, (Set<?>) value);
            return;
        }

        if (value instanceof Collection) {
            Collection<?> values = (Collection<?>) value;
            appendToken(canonical, "collection");
            appendToken(canonical, Integer.toString(values.size()));
            for (Object item : values) {
                appendValue(canonical, item);
            }
            return;
        }

        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            appendToken(canonical, "array");
            appendToken(canonical, Integer.toString(length));
            for (int index = 0; index < length; index++) {
                appendValue(canonical, Array.get(value, index));
            }
            return;
        }

        appendToken(canonical, value.getClass().getName());
        appendToken(canonical, String.valueOf(value));
    }

    private static void appendMap(
            StringBuilder canonical,
            Map<?, ?> values) {

        List<Map.Entry<?, ?>> entries =
                new ArrayList<Map.Entry<?, ?>>(values.entrySet());
        Collections.sort(
                entries,
                new Comparator<Map.Entry<?, ?>>() {
                    @Override
                    public int compare(
                            Map.Entry<?, ?> left,
                            Map.Entry<?, ?> right) {
                        return String.valueOf(left.getKey())
                                .compareTo(String.valueOf(right.getKey()));
                    }
                });

        appendToken(canonical, "map");
        appendToken(canonical, Integer.toString(entries.size()));
        for (Map.Entry<?, ?> entry : entries) {
            appendToken(canonical, String.valueOf(entry.getKey()));
            appendValue(canonical, entry.getValue());
        }
    }

    private static void appendSet(
            StringBuilder canonical,
            Set<?> values) {

        List<String> items = new ArrayList<String>();
        for (Object value : values) {
            StringBuilder item = new StringBuilder();
            appendValue(item, value);
            items.add(item.toString());
        }
        Collections.sort(items);

        appendToken(canonical, "set");
        appendToken(canonical, Integer.toString(items.size()));
        for (String item : items) {
            appendToken(canonical, item);
        }
    }

    private static void appendToken(
            StringBuilder canonical,
            String value) {

        String safe = value == null ? "null" : value;
        canonical.append(safe.length())
                .append(':')
                .append(safe)
                .append(';');
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte current : bytes) {
                result.append(
                        String.format(
                                Locale.ROOT,
                                "%02x",
                                current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    failure);
        }
    }
}
