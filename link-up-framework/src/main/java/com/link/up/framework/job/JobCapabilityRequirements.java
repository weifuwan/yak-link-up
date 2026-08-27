package com.link.up.framework.job;

import com.link.up.api.connector.schema.ConnectorCapability;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/** Immutable required/preferred capability intent for Source and Sink. */
public final class JobCapabilityRequirements {

    private static final JobCapabilityRequirements EMPTY =
            new JobCapabilityRequirements(
                    Collections.<ConnectorCapability>emptySet(),
                    Collections.<ConnectorCapability>emptySet(),
                    Collections.<ConnectorCapability>emptySet(),
                    Collections.<ConnectorCapability>emptySet());

    private final Set<ConnectorCapability> sourceRequired;
    private final Set<ConnectorCapability> sourcePreferred;
    private final Set<ConnectorCapability> sinkRequired;
    private final Set<ConnectorCapability> sinkPreferred;

    public JobCapabilityRequirements(
            Collection<ConnectorCapability> sourceRequired,
            Collection<ConnectorCapability> sourcePreferred,
            Collection<ConnectorCapability> sinkRequired,
            Collection<ConnectorCapability> sinkPreferred) {

        this.sourceRequired = immutable(sourceRequired);
        this.sourcePreferred = immutable(sourcePreferred);
        this.sinkRequired = immutable(sinkRequired);
        this.sinkPreferred = immutable(sinkPreferred);

        requireDisjoint(
                this.sourceRequired,
                this.sourcePreferred,
                "source");
        requireDisjoint(
                this.sinkRequired,
                this.sinkPreferred,
                "sink");
    }

    public static JobCapabilityRequirements empty() {
        return EMPTY;
    }

    public boolean isEmpty() {
        return sourceRequired.isEmpty()
                && sourcePreferred.isEmpty()
                && sinkRequired.isEmpty()
                && sinkPreferred.isEmpty();
    }

    public Set<ConnectorCapability> getSourceRequired() {
        return sourceRequired;
    }

    public Set<ConnectorCapability> getSourcePreferred() {
        return sourcePreferred;
    }

    public Set<ConnectorCapability> getSinkRequired() {
        return sinkRequired;
    }

    public Set<ConnectorCapability> getSinkPreferred() {
        return sinkPreferred;
    }

    private static Set<ConnectorCapability> immutable(
            Collection<ConnectorCapability> values) {

        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }

        EnumSet<ConnectorCapability> copy =
                EnumSet.noneOf(ConnectorCapability.class);

        for (ConnectorCapability value : values) {
            if (value == null) {
                throw new IllegalArgumentException(
                        "capability collections must not contain null");
            }
            copy.add(value);
        }

        return Collections.unmodifiableSet(copy);
    }

    private static void requireDisjoint(
            Set<ConnectorCapability> required,
            Set<ConnectorCapability> preferred,
            String role) {

        EnumSet<ConnectorCapability> overlap =
                EnumSet.noneOf(ConnectorCapability.class);
        overlap.addAll(required);
        overlap.retainAll(preferred);

        if (!overlap.isEmpty()) {
            throw new IllegalArgumentException(
                    role
                            + " capabilities must not be both required and preferred: "
                            + overlap);
        }
    }
}
