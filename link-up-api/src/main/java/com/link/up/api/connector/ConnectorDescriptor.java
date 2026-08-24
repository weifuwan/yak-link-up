package com.link.up.api.connector;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Small, transport-neutral description of a connector artifact.
 *
 * <p>Capability strings deliberately remain opaque: the framework does not
 * register or interpret capabilities that it does not consume.</p>
 */
public final class ConnectorDescriptor {

    private final String identifier;
    private final String version;
    private final String apiVersion;
    private final Set<ConnectorType> types;
    private final Set<String> requiredCapabilities;
    private final String pluginPath;

    public ConnectorDescriptor(
            String identifier,
            String version,
            String apiVersion,
            Set<ConnectorType> types,
            Set<String> requiredCapabilities,
            String pluginPath) {

        this.identifier = required(identifier, "identifier");
        this.version = required(version, "version");
        this.apiVersion = required(apiVersion, "apiVersion");
        this.types = immutableTypes(types);
        this.requiredCapabilities =
                immutableCapabilities(requiredCapabilities);
        this.pluginPath = pluginPath;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getVersion() {
        return version;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public Set<ConnectorType> getTypes() {
        return types;
    }

    public Set<String> getRequiredCapabilities() {
        return requiredCapabilities;
    }

    public String getPluginPath() {
        return pluginPath;
    }

    private static Set<ConnectorType> immutableTypes(
            Set<ConnectorType> types) {

        EnumSet<ConnectorType> copy =
                types == null || types.isEmpty()
                        ? EnumSet.noneOf(ConnectorType.class)
                        : EnumSet.copyOf(types);

        return Collections.unmodifiableSet(copy);
    }

    private static Set<String> immutableCapabilities(
            Set<String> capabilities) {

        Set<String> copy =
                capabilities == null
                        ? new LinkedHashSet<String>()
                        : new LinkedHashSet<String>(capabilities);

        return Collections.unmodifiableSet(copy);
    }

    private static String required(
            String value,
            String name) {

        Objects.requireNonNull(
                value,
                name + " must not be null");

        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }

        return value;
    }
}
