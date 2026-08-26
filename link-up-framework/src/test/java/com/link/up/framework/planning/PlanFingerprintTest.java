package com.link.up.framework.planning;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.framework.job.ColumnMapping;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class PlanFingerprintTest {

    @Test
    public void fingerprintShouldIgnoreMapInsertionOrder() {
        Map<String, Object> first = new LinkedHashMap<String, Object>();
        first.put("url", "jdbc:mysql://localhost/demo");
        first.put("username", "demo");
        first.put("password", "TEST_ONLY_SECRET");

        Map<String, Object> second = new LinkedHashMap<String, Object>();
        second.put("password", "TEST_ONLY_SECRET");
        second.put("username", "demo");
        second.put("url", "jdbc:mysql://localhost/demo");

        assertEquals(
                PlanFingerprint.create(definition(first)),
                PlanFingerprint.create(definition(second)));
    }

    @Test
    public void fingerprintShouldDetectSecretChangesWithoutExposingThem() {
        Map<String, Object> first = options("TEST_ONLY_SECRET_A");
        Map<String, Object> second = options("TEST_ONLY_SECRET_B");

        String firstFingerprint =
                PlanFingerprint.create(definition(first));
        String secondFingerprint =
                PlanFingerprint.create(definition(second));

        assertNotEquals(firstFingerprint, secondFingerprint);
        assertFalse(firstFingerprint.contains("TEST_ONLY_SECRET_A"));
        assertFalse(secondFingerprint.contains("TEST_ONLY_SECRET_B"));
    }

    @Test
    public void logicalPlanMustNotRetainConnectorConfiguration() {
        LogicalJobPlan plan =
                LogicalJobPlan.from(
                        definition(options("TEST_ONLY_SECRET")));

        for (Field field : LogicalJobPlan.class.getDeclaredFields()) {
            String typeName = field.getGenericType().getTypeName();
            assertFalse(typeName.contains("ReadonlyConfig"));
            assertFalse(typeName.contains("Map"));
        }
        assertFalse(
                plan.getFingerprint()
                        .contains("TEST_ONLY_SECRET"));
    }

    private static JobDefinition definition(
            Map<String, Object> sourceOptions) {

        Map<String, Object> sinkOptions =
                new LinkedHashMap<String, Object>();
        sinkOptions.put("table_path", "archive.orders");

        return new JobDefinition(
                "plan-fingerprint-test",
                new SourceDefinition(
                        "jdbc",
                        ReadonlyConfig.fromMap(sourceOptions)),
                new SinkDefinition(
                        "jdbc",
                        ReadonlyConfig.fromMap(sinkOptions)),
                new ExecutionConfig(
                        100,
                        2,
                        2,
                        32),
                new ColumnMapping(
                        Arrays.asList(
                                new ColumnMapping.Item(
                                        "id",
                                        "order_id"))));
    }

    private static Map<String, Object> options(String secret) {
        Map<String, Object> options =
                new LinkedHashMap<String, Object>();
        options.put("url", "jdbc:mysql://localhost/demo");
        options.put("username", "demo");
        options.put("password", secret);
        return options;
    }
}
