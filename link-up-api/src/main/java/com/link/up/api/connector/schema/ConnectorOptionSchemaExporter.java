package com.link.up.api.connector.schema;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.SingleChoiceOption;
import com.link.up.api.configuration.util.Condition;
import com.link.up.api.configuration.util.ConditionRule;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.configuration.util.RequiredOption;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Exports connector options while preserving stable schema ordering. */
final class ConnectorOptionSchemaExporter {

    List<ConnectorOptionSchema> export(OptionRule rule) {
        Map<String, Option<?>> optionIndex =
                new LinkedHashMap<String, Option<?>>();

        collectOptions(
                rule,
                optionIndex,
                Collections.newSetFromMap(
                        new IdentityHashMap<OptionRule, Boolean>()));

        Set<String> requiredKeys =
                absoluteRequiredKeys(rule);

        List<ConnectorOptionSchema> options =
                new ArrayList<ConnectorOptionSchema>();

        for (Option<?> option : optionIndex.values()) {
            options.add(
                    exportOption(
                            option,
                            requiredKeys.contains(option.key())));
        }

        Collections.sort(
                options,
                new Comparator<ConnectorOptionSchema>() {
                    @Override
                    public int compare(
                            ConnectorOptionSchema first,
                            ConnectorOptionSchema second) {
                        return first.getKey()
                                .compareTo(second.getKey());
                    }
                });

        return options;
    }

    private static Set<String> absoluteRequiredKeys(
            OptionRule rule) {

        Set<String> result =
                new LinkedHashSet<String>();

        for (RequiredOption required :
                rule.getRequiredOptions()) {
            if (required
                    instanceof RequiredOption.AbsolutelyRequiredOptions) {
                result.addAll(
                        ConnectorSchemaValues.optionKeys(
                                required.getOptions()));
            }
        }

        return result;
    }

    private static void collectOptions(
            OptionRule rule,
            Map<String, Option<?>> optionIndex,
            Set<OptionRule> visited) {

        if (!visited.add(rule)) {
            throw new IllegalArgumentException(
                    "Circular OptionRule graph detected");
        }

        for (Option<?> option : rule.getOptionalOptions()) {
            putOption(optionIndex, option);
        }

        for (RequiredOption required :
                rule.getRequiredOptions()) {
            for (Option<?> option : required.getOptions()) {
                putOption(optionIndex, option);
            }

            if (required
                    instanceof RequiredOption.ConditionalRequiredOptions) {
                RequiredOption.ConditionalRequiredOptions conditional =
                        (RequiredOption.ConditionalRequiredOptions)
                                required;
                collectConditionOptions(
                        conditional.getCondition(),
                        optionIndex);
            }
        }

        for (Condition<?> constraint :
                rule.getValueConstraints()) {
            collectConditionOptions(
                    constraint,
                    optionIndex);
        }

        for (ConditionRule conditionRule :
                rule.getConditionRules()) {
            collectConditionOptions(
                    conditionRule.getCondition(),
                    optionIndex);
            collectOptions(
                    conditionRule.getOptionRule(),
                    optionIndex,
                    visited);
        }

        visited.remove(rule);
    }

    private static void collectConditionOptions(
            Condition<?> condition,
            Map<String, Option<?>> optionIndex) {

        Set<Condition<?>> visited =
                Collections.newSetFromMap(
                        new IdentityHashMap<Condition<?>, Boolean>());

        Condition<?> current = condition;
        while (current != null) {
            if (!visited.add(current)) {
                throw new IllegalArgumentException(
                        "Circular condition chain detected");
            }

            putOption(optionIndex, current.getOption());

            if (current.getCompareOption() != null) {
                putOption(
                        optionIndex,
                        current.getCompareOption());
            }

            current = current.getNext();
        }
    }

    private static void putOption(
            Map<String, Option<?>> optionIndex,
            Option<?> option) {

        Option<?> existing =
                optionIndex.get(option.key());

        if (existing == null) {
            optionIndex.put(option.key(), option);
            return;
        }

        if (!existing.typeReference()
                .getType()
                .equals(option.typeReference().getType())) {
            throw new IllegalArgumentException(
                    "Option key '"
                            + option.key()
                            + "' is declared with different types");
        }
    }

    private static ConnectorOptionSchema exportOption(
            Option<?> option,
            boolean required) {

        Type type = option.typeReference().getType();

        return new ConnectorOptionSchema(
                option.key(),
                ConnectorSchemaValues.valueType(type),
                type.getTypeName(),
                ConnectorSchemaValues.elementJavaType(type),
                ConnectorSchemaValues.jsonValue(
                        option.defaultValue()),
                allowedValues(option, type),
                option.getDescription(),
                option.getFallbackKeys(),
                required,
                option.isSensitive(),
                option.getSemanticType(),
                option.getScope());
    }

    private static List<Object> allowedValues(
            Option<?> option,
            Type type) {

        List<Object> result = new ArrayList<Object>();

        if (option instanceof SingleChoiceOption) {
            SingleChoiceOption<?> singleChoice =
                    (SingleChoiceOption<?>) option;

            for (Object value :
                    singleChoice.getOptionValues()) {
                result.add(
                        ConnectorSchemaValues.jsonValue(value));
            }
            return result;
        }

        if (!(type instanceof Class)
                || !((Class<?>) type).isEnum()) {
            return result;
        }

        Object[] constants =
                ((Class<?>) type).getEnumConstants();

        if (constants == null) {
            return result;
        }

        for (Object constant : constants) {
            result.add(
                    ConnectorSchemaValues.jsonValue(constant));
        }

        return result;
    }
}
