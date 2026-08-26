package com.link.up.framework.planning;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.framework.job.ExecutionConfig;
import com.link.up.framework.job.JobCapabilityRequirements;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.SinkDefinition;
import com.link.up.framework.job.SourceDefinition;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class PlanFingerprintCapabilityTest {

    @Test
    public void capabilityIntentMustContributeToFingerprint() {
        JobDefinition baseline = definition(
                JobCapabilityRequirements.empty());
        JobDefinition required = definition(
                new JobCapabilityRequirements(
                        Collections.<ConnectorCapability>emptySet(),
                        Collections.<ConnectorCapability>emptySet(),
                        Collections.singleton(
                                ConnectorCapability.TWO_PHASE_COMMIT),
                        Collections.<ConnectorCapability>emptySet()));

        assertNotEquals(
                PlanFingerprint.create(baseline),
                PlanFingerprint.create(required));
    }

    @Test
    public void logicalPlanShouldExposeIntentWithoutConnectorOptions() {
        JobDefinition definition = definition(
                new JobCapabilityRequirements(
                        Collections.singleton(
                                ConnectorCapability.MULTI_TABLE),
                        Collections.singleton(
                                ConnectorCapability.PARTITION_SPLIT),
                        Collections.<ConnectorCapability>emptySet(),
                        Collections.<ConnectorCapability>emptySet()));

        LogicalJobPlan plan =
                LogicalJobPlan.from(definition);

        assertTrue(
                plan.getCapabilities()
                        .getSource()
                        .getRequired()
                        .contains(
                                ConnectorCapability.MULTI_TABLE));
        assertFalse(
                plan.getFingerprint().contains(
                        "TEST_ONLY_SECRET"));
    }

    private JobDefinition definition(
            JobCapabilityRequirements requirements) {

        java.util.Map<String, Object> sourceOptions =
                new java.util.LinkedHashMap<String, Object>();
        sourceOptions.put(
                "password",
                "TEST_ONLY_SECRET");

        return new JobDefinition(
                "fingerprint-capability",
                new SourceDefinition(
                        "jdbc",
                        ReadonlyConfig.fromMap(sourceOptions)),
                new SinkDefinition(
                        "doris",
                        ReadonlyConfig.fromMap(
                                Collections.<String, Object>emptyMap())),
                new ExecutionConfig(100, 1, 1, 32),
                null,
                requirements);
    }
}
