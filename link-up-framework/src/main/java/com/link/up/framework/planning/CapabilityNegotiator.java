package com.link.up.framework.planning;

import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.connector.schema.ConnectorRole;
import com.link.up.framework.connector.ConnectorException;
import com.link.up.framework.connector.ConnectorPreparer;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.JobCapabilityRequirements;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.PipelineGraph;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** Matches explicit and plan-derived requirements with Connector declarations. */
public final class CapabilityNegotiator {

    private final ConnectorPreparer connectorPreparer;

    public CapabilityNegotiator(
            ConnectorPreparer connectorPreparer) {
        this.connectorPreparer = Objects.requireNonNull(
                connectorPreparer,
                "connectorPreparer must not be null");
    }

    public CapabilityNegotiation negotiate(
            JobDefinition definition) {
        return negotiate(
                definition,
                TopologyFacts.empty());
    }

    public CapabilityNegotiation negotiate(
            JobDefinition definition,
            PreparedSource<?> preparedSource) {

        PreparedSource<?> source = Objects.requireNonNull(
                preparedSource,
                "preparedSource must not be null");

        TopologyFacts facts = TopologyFacts.empty();
        facts.sourceObserved.add(
                ConnectorCapability.TABLE_SCHEMA_DISCOVERY);

        if (source.getOutputTables().size() > 1) {
            facts.requireMultiTable();
        }

        return negotiate(definition, facts);
    }

    public CapabilityNegotiation negotiate(
            JobDefinition definition,
            JobGraph jobGraph) {

        JobGraph graph = Objects.requireNonNull(
                jobGraph,
                "jobGraph must not be null");
        TopologyFacts facts = TopologyFacts.empty();
        facts.sourceObserved.add(
                ConnectorCapability.TABLE_SCHEMA_DISCOVERY);

        if (graph.getPipelineGraphs().size() > 1) {
            facts.requireMultiTable();
        }

        for (PipelineGraph<?> pipeline :
                graph.getPipelineGraphs()) {
            if (pipeline.getSourceSplits().size() > 1) {
                facts.sourceObserved.add(
                        ConnectorCapability.PARTITION_SPLIT);
                break;
            }
        }

        return negotiate(definition, facts);
    }

    public void requireSatisfied(
            CapabilityNegotiation negotiation) {

        CapabilityNegotiation result =
                Objects.requireNonNull(
                        negotiation,
                        "negotiation must not be null");

        if (!result.getSource()
                .getMissingRequired()
                .isEmpty()) {
            throw PlanningException.requiredCapabilityMissing(
                    ConnectorRole.SOURCE,
                    result.getSource().getConnectorId(),
                    result.getSource()
                            .getMissingRequired()
                            .get(0));
        }

        if (!result.getSink()
                .getMissingRequired()
                .isEmpty()) {
            throw PlanningException.requiredCapabilityMissing(
                    ConnectorRole.SINK,
                    result.getSink().getConnectorId(),
                    result.getSink()
                            .getMissingRequired()
                            .get(0));
        }
    }

    private CapabilityNegotiation negotiate(
            JobDefinition definition,
            TopologyFacts facts) {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        JobCapabilityRequirements requirements =
                job.getCapabilityRequirements();

        CapabilityNegotiation.Endpoint source = endpoint(
                ConnectorRole.SOURCE,
                job.getSource().getType(),
                sourceCapabilities(job.getSource().getType()),
                requirements.getSourceRequired(),
                requirements.getSourcePreferred(),
                facts.sourceDerivedRequired,
                facts.sourceObserved);
        CapabilityNegotiation.Endpoint sink = endpoint(
                ConnectorRole.SINK,
                job.getSink().getType(),
                sinkCapabilities(job.getSink().getType()),
                requirements.getSinkRequired(),
                requirements.getSinkPreferred(),
                facts.sinkDerivedRequired,
                facts.sinkObserved);

        return new CapabilityNegotiation(
                source,
                sink);
    }

    private Set<ConnectorCapability> sourceCapabilities(
            String connectorId) {
        try {
            return connectorPreparer.sourceCapabilities(
                    connectorId);
        } catch (ConnectorException failure) {
            throw PlanningException.connectorNotFound(
                    ConnectorRole.SOURCE,
                    connectorId,
                    failure);
        }
    }

    private Set<ConnectorCapability> sinkCapabilities(
            String connectorId) {
        try {
            return connectorPreparer.sinkCapabilities(
                    connectorId);
        } catch (ConnectorException failure) {
            throw PlanningException.connectorNotFound(
                    ConnectorRole.SINK,
                    connectorId,
                    failure);
        }
    }

    private CapabilityNegotiation.Endpoint endpoint(
            ConnectorRole role,
            String connectorId,
            Set<ConnectorCapability> supported,
            Set<ConnectorCapability> required,
            Set<ConnectorCapability> preferred,
            Set<ConnectorCapability> derivedRequired,
            Set<ConnectorCapability> observed) {

        return new CapabilityNegotiation.Endpoint(
                role,
                connectorId,
                supported,
                required,
                preferred,
                derivedRequired,
                observed);
    }

    private static final class TopologyFacts {

        private final EnumSet<ConnectorCapability>
                sourceDerivedRequired =
                EnumSet.noneOf(ConnectorCapability.class);
        private final EnumSet<ConnectorCapability>
                sinkDerivedRequired =
                EnumSet.noneOf(ConnectorCapability.class);
        private final EnumSet<ConnectorCapability>
                sourceObserved =
                EnumSet.noneOf(ConnectorCapability.class);
        private final EnumSet<ConnectorCapability>
                sinkObserved =
                EnumSet.noneOf(ConnectorCapability.class);

        private static TopologyFacts empty() {
            return new TopologyFacts();
        }

        private void requireMultiTable() {
            sourceDerivedRequired.add(
                    ConnectorCapability.MULTI_TABLE);
            sinkDerivedRequired.add(
                    ConnectorCapability.MULTI_TABLE);
            sourceObserved.add(
                    ConnectorCapability.MULTI_TABLE);
            sinkObserved.add(
                    ConnectorCapability.MULTI_TABLE);
        }
    }
}
