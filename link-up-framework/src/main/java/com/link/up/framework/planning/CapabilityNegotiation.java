package com.link.up.framework.planning;

import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.connector.schema.ConnectorRole;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Secret-safe result of checking offline Job requirements against Connector
 * capabilities.
 *
 * <p>The contract intentionally stays small: supported, required, preferred
 * and the corresponding missing sets. Topology-derived requirements are folded
 * into required instead of creating a second rules/observation model.</p>
 */
public final class CapabilityNegotiation {

    public static final String CURRENT_API_VERSION =
            "link-up-capability-negotiation/v2";

    public enum Status {
        SATISFIED,
        DEGRADED,
        REJECTED
    }

    private final String apiVersion;
    private final Status status;
    private final Endpoint source;
    private final Endpoint sink;

    CapabilityNegotiation(
            Endpoint source,
            Endpoint sink) {

        this.apiVersion = CURRENT_API_VERSION;
        this.source = Objects.requireNonNull(
                source,
                "source must not be null");
        this.sink = Objects.requireNonNull(
                sink,
                "sink must not be null");
        this.status = status(source, sink);
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public Status getStatus() {
        return status;
    }

    public Endpoint getSource() {
        return source;
    }

    public Endpoint getSink() {
        return sink;
    }

    public boolean isRejected() {
        return status == Status.REJECTED;
    }

    public List<PlanningDiagnostic> diagnostics() {
        List<PlanningDiagnostic> result =
                new ArrayList<PlanningDiagnostic>();

        diagnostics(source, result);
        diagnostics(sink, result);

        if (result.isEmpty()) {
            result.add(
                    new PlanningDiagnostic(
                            "CAPABILITY_CHECKED",
                            PlanningDiagnostic.Severity.INFO,
                            "CAPABILITY_NEGOTIATION",
                            "Required Connector capabilities are satisfied"));
        }

        return Collections.unmodifiableList(result);
    }

    private static void diagnostics(
            Endpoint endpoint,
            List<PlanningDiagnostic> result) {

        for (ConnectorCapability capability :
                endpoint.getMissingRequired()) {
            result.add(
                    new PlanningDiagnostic(
                            "REQUIRED_CAPABILITY_MISSING",
                            PlanningDiagnostic.Severity.ERROR,
                            "CAPABILITY_NEGOTIATION",
                            endpoint.getRole().name()
                                    + " connector '"
                                    + endpoint.getConnectorId()
                                    + "' is missing required capability "
                                    + capability.name()));
        }

        for (ConnectorCapability capability :
                endpoint.getMissingPreferred()) {
            result.add(
                    new PlanningDiagnostic(
                            "PREFERRED_CAPABILITY_MISSING",
                            PlanningDiagnostic.Severity.WARNING,
                            "CAPABILITY_NEGOTIATION",
                            endpoint.getRole().name()
                                    + " connector '"
                                    + endpoint.getConnectorId()
                                    + "' does not provide preferred capability "
                                    + capability.name()));
        }
    }

    private static Status status(
            Endpoint source,
            Endpoint sink) {

        if (!source.getMissingRequired().isEmpty()
                || !sink.getMissingRequired().isEmpty()) {
            return Status.REJECTED;
        }
        if (!source.getMissingPreferred().isEmpty()
                || !sink.getMissingPreferred().isEmpty()) {
            return Status.DEGRADED;
        }
        return Status.SATISFIED;
    }

    public static final class Endpoint {

        private final ConnectorRole role;
        private final String connectorId;
        private final List<ConnectorCapability> supported;
        private final List<ConnectorCapability> required;
        private final List<ConnectorCapability> preferred;
        private final List<ConnectorCapability> missingRequired;
        private final List<ConnectorCapability> missingPreferred;
        private final Status status;

        Endpoint(
                ConnectorRole role,
                String connectorId,
                Collection<ConnectorCapability> supported,
                Collection<ConnectorCapability> explicitRequired,
                Collection<ConnectorCapability> preferred,
                Collection<ConnectorCapability> derivedRequired) {

            this.role = Objects.requireNonNull(
                    role,
                    "role must not be null");
            this.connectorId = requireText(
                    connectorId,
                    "connectorId");
            this.supported = immutable(supported);

            EnumSet<ConnectorCapability> allRequired =
                    copy(explicitRequired);
            allRequired.addAll(copy(derivedRequired));

            this.required = immutable(allRequired);
            this.preferred = immutable(preferred);
            this.missingRequired = immutable(
                    difference(
                            allRequired,
                            supported));
            this.missingPreferred = immutable(
                    difference(
                            preferred,
                            supported));

            if (!missingRequired.isEmpty()) {
                this.status = Status.REJECTED;
            } else if (!missingPreferred.isEmpty()) {
                this.status = Status.DEGRADED;
            } else {
                this.status = Status.SATISFIED;
            }
        }

        public ConnectorRole getRole() {
            return role;
        }

        public String getConnectorId() {
            return connectorId;
        }

        public List<ConnectorCapability> getSupported() {
            return supported;
        }

        public List<ConnectorCapability> getRequired() {
            return required;
        }

        public List<ConnectorCapability> getPreferred() {
            return preferred;
        }

        public List<ConnectorCapability> getMissingRequired() {
            return missingRequired;
        }

        public List<ConnectorCapability> getMissingPreferred() {
            return missingPreferred;
        }

        public Status getStatus() {
            return status;
        }

        private static List<ConnectorCapability> immutable(
                Collection<ConnectorCapability> values) {

            EnumSet<ConnectorCapability> copy = copy(values);
            return Collections.unmodifiableList(
                    new ArrayList<ConnectorCapability>(copy));
        }

        private static EnumSet<ConnectorCapability> copy(
                Collection<ConnectorCapability> values) {

            EnumSet<ConnectorCapability> result =
                    EnumSet.noneOf(ConnectorCapability.class);

            if (values != null) {
                for (ConnectorCapability value : values) {
                    result.add(
                            Objects.requireNonNull(
                                    value,
                                    "capabilities must not contain null"));
                }
            }

            return result;
        }

        private static EnumSet<ConnectorCapability> difference(
                Collection<ConnectorCapability> left,
                Collection<ConnectorCapability> right) {

            EnumSet<ConnectorCapability> result = copy(left);
            result.removeAll(copy(right));
            return result;
        }

        private static String requireText(
                String value,
                String name) {

            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        name + " must not be blank");
            }
            return value.trim();
        }
    }
}
