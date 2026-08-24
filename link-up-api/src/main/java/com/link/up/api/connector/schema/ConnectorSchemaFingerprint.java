package com.link.up.api.connector.schema;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Builds the stable SHA-256 fingerprint for an exported connector schema. */
final class ConnectorSchemaFingerprint {

    private ConnectorSchemaFingerprint() {
    }

    static String create(
            String connectorId,
            ConnectorRole role,
            String schemaVersion,
            List<ConnectorOptionSchema> options,
            List<ConnectorRuleSchema> rules,
            List<ConnectorCapability> capabilities) {

        StringBuilder canonical = new StringBuilder();
        canonical.append(connectorId)
                .append('|')
                .append(role.name())
                .append('|')
                .append(schemaVersion);

        for (ConnectorOptionSchema option : options) {
            appendOption(canonical, option);
        }

        for (ConnectorRuleSchema rule : rules) {
            appendRule(canonical, rule);
        }

        for (ConnectorCapability capability : capabilities) {
            canonical.append("\nC|")
                    .append(capability.name());
        }

        return "sha256:" + sha256(canonical.toString());
    }

    private static void appendOption(
            StringBuilder canonical,
            ConnectorOptionSchema option) {

        canonical.append("\nO|")
                .append(option.getKey())
                .append('|')
                .append(option.getValueType())
                .append('|')
                .append(option.getJavaType())
                .append('|')
                .append(option.getElementJavaType())
                .append('|')
                .append(canonicalValue(
                        option.getDefaultValue()))
                .append('|')
                .append(canonicalValue(
                        option.getAllowedValues()))
                .append('|')
                .append(option.isRequired())
                .append('|')
                .append(option.isSensitive())
                .append('|')
                .append(option.getSemanticType())
                .append('|')
                .append(option.getScope())
                .append('|')
                .append(option.getDescription())
                .append('|')
                .append(option.getFallbackKeys());
    }

    private static void appendRule(
            StringBuilder canonical,
            ConnectorRuleSchema rule) {

        canonical.append("\nR|")
                .append(rule.getType())
                .append('|')
                .append(rule.getOptionKeys());

        appendCondition(
                canonical,
                rule.getCondition());

        for (ConnectorRuleSchema nested :
                rule.getNestedRules()) {
            appendRule(canonical, nested);
        }
    }

    private static void appendCondition(
            StringBuilder canonical,
            ConnectorConditionSchema condition) {

        ConnectorConditionSchema current = condition;

        while (current != null) {
            canonical.append("|Q:")
                    .append(current.getOptionKey())
                    .append(':')
                    .append(current.getOperator())
                    .append(':')
                    .append(canonicalValue(
                            current.getExpectedValue()))
                    .append(':')
                    .append(current.getCompareOptionKey())
                    .append(':')
                    .append(current.getExtensionDescription())
                    .append(':')
                    .append(current.getLogicalOperator());

            current = current.getNext();
        }
    }

    private static String canonicalValue(Object value) {
        if (value == null) {
            return "null";
        }

        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            List<String> entries = new ArrayList<String>();

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                entries.add(
                        canonicalValue(entry.getKey())
                                + "="
                                + canonicalValue(
                                        entry.getValue()));
            }

            Collections.sort(entries);
            return entries.toString();
        }

        if (value instanceof Collection) {
            List<String> values = new ArrayList<String>();
            for (Object item : (Collection<?>) value) {
                values.add(canonicalValue(item));
            }
            return values.toString();
        }

        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<String> values =
                    new ArrayList<String>(length);

            for (int index = 0; index < length; index++) {
                values.add(
                        canonicalValue(
                                Array.get(value, index)));
            }
            return values.toString();
        }

        return String.valueOf(
                ConnectorSchemaValues.jsonValue(value));
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");

            byte[] bytes =
                    digest.digest(
                            value.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexadecimal =
                    new StringBuilder(bytes.length * 2);

            for (byte current : bytes) {
                hexadecimal.append(
                        String.format(
                                "%02x",
                                current & 0xff));
            }

            return hexadecimal.toString();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    failure);
        }
    }
}
