package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.ReadonlyConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates configured paths against the keys declared by an {@link OptionRule}. */
final class OptionKeyValidator {

    private OptionKeyValidator() {
    }

    static void validate(
            ReadonlyConfig config,
            OptionRule rule,
            String connectorName,
            Option<?>... commonOptions) {

        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }

        Set<String> declaredKeys = collectDeclaredKeys(rule);
        if (commonOptions != null) {
            collectKeys(declaredKeys, Arrays.asList(commonOptions));
        }

        List<String> unknownKeys = new ArrayList<String>();
        validatePaths(
                config.getSourceMap(),
                "",
                declaredKeys,
                unknownKeys);

        if (unknownKeys.isEmpty()) {
            return;
        }

        List<String> sortedDeclaredKeys =
                new ArrayList<String>(declaredKeys);
        Collections.sort(sortedDeclaredKeys);

        throw new OptionValidationException(
                "Connector '%s' has unknown option keys: %s. Declared options are: %s",
                connectorName,
                unknownKeys,
                sortedDeclaredKeys);
    }

    private static Set<String> collectDeclaredKeys(OptionRule rule) {
        Set<String> keys = new HashSet<String>();
        if (rule == null) {
            return keys;
        }

        collectKeys(keys, rule.getOptionalOptions());

        for (RequiredOption required : rule.getRequiredOptions()) {
            collectKeys(keys, required.getOptions());

            if (required
                    instanceof RequiredOption.ConditionalRequiredOptions) {
                RequiredOption.ConditionalRequiredOptions conditional =
                        (RequiredOption.ConditionalRequiredOptions) required;
                collectConditionKeys(keys, conditional.getCondition());
            }
        }

        for (ConditionRule conditionRule : rule.getConditionRules()) {
            collectConditionKeys(keys, conditionRule.getCondition());
            keys.addAll(
                    collectDeclaredKeys(conditionRule.getOptionRule()));
        }

        for (Condition<?> constraint : rule.getValueConstraints()) {
            collectConditionKeys(keys, constraint);
        }

        return keys;
    }

    private static void collectConditionKeys(
            Set<String> keys,
            Condition<?> condition) {

        Condition<?> current = condition;
        while (current != null) {
            addOptionKeys(keys, current.getOption());
            if (current.getCompareOption() != null) {
                addOptionKeys(keys, current.getCompareOption());
            }
            current = current.getNext();
        }
    }

    private static void collectKeys(
            Set<String> keys,
            List<? extends Option<?>> options) {

        for (Option<?> option : options) {
            addOptionKeys(keys, option);
        }
    }

    private static void addOptionKeys(
            Set<String> keys,
            Option<?> option) {

        keys.add(option.key());
        keys.addAll(option.getFallbackKeys());
    }

    @SuppressWarnings("unchecked")
    private static void validatePaths(
            Map<String, Object> values,
            String prefix,
            Set<String> declaredKeys,
            List<String> unknownKeys) {

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String fullKey = prefix.isEmpty()
                    ? entry.getKey()
                    : prefix + "." + entry.getKey();

            if (!isDeclaredPath(fullKey, declaredKeys)) {
                unknownKeys.add(fullKey);
                continue;
            }

            Object value = entry.getValue();
            if (value instanceof Map
                    && !declaredKeys.contains(fullKey)) {
                validatePaths(
                        (Map<String, Object>) value,
                        fullKey,
                        declaredKeys,
                        unknownKeys);
            }
        }
    }

    private static boolean isDeclaredPath(
            String path,
            Set<String> declaredKeys) {

        if (declaredKeys.contains(path)) {
            return true;
        }

        String prefix = path + ".";
        for (String key : declaredKeys) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
