package com.link.up.api.connector.schema;

import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.factory.Factory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Exports a connector's runtime option contract as the stable control-plane
 * {@link ConnectorSchema}.
 */
public final class ConnectorSchemaExporter {

    private final ConnectorOptionSchemaExporter optionExporter =
            new ConnectorOptionSchemaExporter();

    private final ConnectorRuleSchemaExporter ruleExporter =
            new ConnectorRuleSchemaExporter();

    public ConnectorSchema export(
            Factory factory,
            ConnectorRole role,
            OptionRule optionRule) {

        if (factory == null) {
            throw new IllegalArgumentException(
                    "factory must not be null");
        }
        if (role == null) {
            throw new IllegalArgumentException(
                    "role must not be null");
        }
        if (optionRule == null) {
            throw new IllegalArgumentException(
                    "optionRule must not be null");
        }

        List<ConnectorOptionSchema> options =
                optionExporter.export(optionRule);
        List<ConnectorRuleSchema> rules =
                ruleExporter.export(optionRule);
        List<ConnectorCapability> capabilities =
                sortedCapabilities(factory);

        String fingerprint =
                ConnectorSchemaFingerprint.create(
                        factory.factoryIdentifier(),
                        role,
                        factory.schemaVersion(),
                        options,
                        rules,
                        capabilities);

        return new ConnectorSchema(
                factory.factoryIdentifier(),
                role,
                factory.schemaVersion(),
                fingerprint,
                factory.getClass().getName(),
                implementationVersion(factory.getClass()),
                options,
                rules,
                capabilities);
    }

    private static List<ConnectorCapability> sortedCapabilities(
            Factory factory) {

        List<ConnectorCapability> capabilities =
                new ArrayList<ConnectorCapability>(
                        factory.capabilities());

        Collections.sort(
                capabilities,
                new Comparator<ConnectorCapability>() {
                    @Override
                    public int compare(
                            ConnectorCapability first,
                            ConnectorCapability second) {
                        return first.name()
                                .compareTo(second.name());
                    }
                });

        return capabilities;
    }

    private static String implementationVersion(
            Class<?> implementationClass) {

        Package implementationPackage =
                implementationClass.getPackage();

        String version =
                implementationPackage == null
                        ? null
                        : implementationPackage
                                .getImplementationVersion();

        return version == null
                || version.trim().isEmpty()
                ? "unknown"
                : version;
    }
}
