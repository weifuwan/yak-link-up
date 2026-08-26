package com.link.up.framework.job;

import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.job.JobSpec;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobCapabilityCompilerTest {

    @Test
    public void shouldCompileRequiredAndPreferredCapabilities() {
        JobSpec spec = spec();
        JobSpec.CapabilityRequirements requirements =
                new JobSpec.CapabilityRequirements();

        JobSpec.Endpoint source = new JobSpec.Endpoint();
        source.setRequired(
                Arrays.asList(
                        "table_schema_discovery",
                        "MULTI_TABLE"));
        source.setPreferred(
                Collections.singletonList(
                        "partition_split"));
        requirements.setSource(source);

        JobSpec.Endpoint sink = new JobSpec.Endpoint();
        sink.setRequired(
                Collections.singletonList(
                        "two_phase_commit"));
        requirements.setSink(sink);
        spec.setCapabilities(requirements);

        JobDefinition definition =
                new JobSpecCompiler().compile(spec);
        JobCapabilityRequirements compiled =
                definition.getCapabilityRequirements();

        assertTrue(
                compiled.getSourceRequired().contains(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY));
        assertTrue(
                compiled.getSourceRequired().contains(
                        ConnectorCapability.MULTI_TABLE));
        assertEquals(
                Collections.singleton(
                        ConnectorCapability.PARTITION_SPLIT),
                compiled.getSourcePreferred());
        assertEquals(
                Collections.singleton(
                        ConnectorCapability.TWO_PHASE_COMMIT),
                compiled.getSinkRequired());
    }

    @Test
    public void shouldRejectUnknownCapability() {
        JobSpec spec = spec();
        JobSpec.CapabilityRequirements requirements =
                new JobSpec.CapabilityRequirements();
        JobSpec.Endpoint source = new JobSpec.Endpoint();
        source.setRequired(
                Collections.singletonList(
                        "NOT_A_CAPABILITY"));
        requirements.setSource(source);
        spec.setCapabilities(requirements);

        try {
            new JobSpecCompiler().compile(spec);
            fail("Expected unknown capability rejection");
        } catch (IllegalArgumentException expected) {
            assertTrue(
                    expected.getMessage().contains(
                            "Unknown capabilities.source.required capability"));
        }
    }

    @Test
    public void shouldRejectRequiredPreferredOverlap() {
        JobSpec spec = spec();
        JobSpec.CapabilityRequirements requirements =
                new JobSpec.CapabilityRequirements();
        JobSpec.Endpoint sink = new JobSpec.Endpoint();
        sink.setRequired(
                Collections.singletonList("UPSERT"));
        sink.setPreferred(
                Collections.singletonList("upsert"));
        requirements.setSink(sink);
        spec.setCapabilities(requirements);

        try {
            new JobSpecCompiler().compile(spec);
            fail("Expected overlap rejection");
        } catch (IllegalArgumentException expected) {
            assertTrue(
                    expected.getMessage().contains(
                            "sink capabilities must not be both required and preferred"));
        }
    }

    @Test
    public void shouldParseHoconCapabilityIntent() {
        String hocon =
                "source { type = \"test-source\" }\n"
                        + "sink { type = \"test-sink\" }\n"
                        + "capabilities {\n"
                        + "  source { required = [\"MULTI_TABLE\"] }\n"
                        + "  sink { preferred = [\"TWO_PHASE_COMMIT\"] }\n"
                        + "}\n";

        JobDefinition definition =
                new JobConfigParser().parse(hocon);

        assertEquals(
                Collections.singleton(
                        ConnectorCapability.MULTI_TABLE),
                definition.getCapabilityRequirements()
                        .getSourceRequired());
        assertEquals(
                Collections.singleton(
                        ConnectorCapability.TWO_PHASE_COMMIT),
                definition.getCapabilityRequirements()
                        .getSinkPreferred());
    }

    private JobSpec spec() {
        JobSpec spec = new JobSpec();
        spec.setName("capability-job");
        spec.setSource(connector("test-source"));
        spec.setSink(connector("test-sink"));
        return spec;
    }

    private JobSpec.Connector connector(String id) {
        JobSpec.Connector connector =
                new JobSpec.Connector();
        connector.setConnectorId(id);
        return connector;
    }
}
