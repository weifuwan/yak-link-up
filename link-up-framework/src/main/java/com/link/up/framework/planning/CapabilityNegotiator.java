package com.link.up.framework.planning;

import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.connector.schema.ConnectorRole;
import com.link.up.framework.connector.ConnectorException;
import com.link.up.framework.connector.ConnectorPreparer;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.JobCapabilityRequirements;
import com.link.up.framework.job.JobDefinition;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Performs the small capability check required by the offline runtime.
 *
 * <p>Only explicit Job requirements and mandatory topology-derived
 * requirements participate. The runtime does not maintain a second
 * "observed capability" model or a generic capability rule engine.</p>
 */
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
                DerivedRequirements.empty());
    }

    public CapabilityNegotiation negotiate(
            JobDefinition definition,
            PreparedSource<?> preparedSource) {

        PreparedSource<?> source = Objects.requireNonNull(
                preparedSource,
                "preparedSource must not be null");

        DerivedRequirements derived =
                DerivedRequirements.empty();

        if (source.getOutputTables().size() > 1) {
            derived.requireMultiTable();
        }

        return negotiate(definition, derived);
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
            DerivedRequirements derived) {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        JobCapabilityRequirements requirements =
                job.getCapabilityRequirements();

        CapabilityNegotiation.Endpoint source =
                new CapabilityNegotiation.Endpoint(
                        ConnectorRole.SOURCE,
                        job.getSource().getType(),
                        sourceCapabilities(
                                job.getSource().getType()),
                        requirements.getSourceRequired(),
                        requirements.getSourcePreferred(),
                        derived.sourceRequired);
        CapabilityNegotiation.Endpoint sink =
                new CapabilityNegotiation.Endpoint(
                        ConnectorRole.SINK,
                        job.getSink().getType(),
                        sinkCapabilities(
                                job.getSink().getType()),
                        requirements.getSinkRequired(),
                        requirements.getSinkPreferred(),
                        derived.sinkRequired);

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

    private static final class DerivedRequirements {

        private final EnumSet<ConnectorCapability> sourceRequired =
                EnumSet.noneOf(ConnectorCapability.class);
        private final EnumSet<ConnectorCapability> sinkRequired =
                EnumSet.noneOf(ConnectorCapability.class);

        private static DerivedRequirements empty() {
            return new DerivedRequirements();
        }

        private void requireMultiTable() {
            sourceRequired.add(
                    ConnectorCapability.MULTI_TABLE);
            sinkRequired.add(
                    ConnectorCapability.MULTI_TABLE);
        }
    }
}
