package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Validation and merge helpers for the public {@link OptionRule.Builder} DSL. */
final class OptionRuleBuilderSupport {

    private OptionRuleBuilderSupport() {
    }

    static <T> List<T> merge(
            List<T> first,
            List<T> second) {

        List<T> result = new ArrayList<T>(first);
        result.addAll(second);
        return result;
    }

    static void requireOptions(Option<?>[] options) {
        if (options == null || options.length == 0) {
            throw new OptionValidationException(
                    "Options must not be empty");
        }

        for (Option<?> option : options) {
            Objects.requireNonNull(option, "option");
        }
    }

    static <T> Condition<T> anyOf(
            Option<T> conditionOption,
            List<T> expectedValues) {

        if (expectedValues == null
                || expectedValues.isEmpty()) {
            throw new OptionValidationException(
                    "Conditional values must not be empty");
        }

        Condition<T> condition = null;
        for (T value : expectedValues) {
            Condition<T> candidate =
                    Condition.of(conditionOption, value);

            if (condition == null) {
                condition = candidate;
            } else {
                condition.or(candidate);
            }
        }

        return condition;
    }

    static void verifyOptionalDuplicate(
            Option<?> option,
            List<Option<?>> optionalOptions,
            List<RequiredOption> requiredOptions) {

        if (optionalOptions.contains(option)) {
            throw duplicate(option);
        }

        for (RequiredOption required : requiredOptions) {
            if (required
                    instanceof RequiredOption.ConditionalRequiredOptions) {
                continue;
            }

            if (required.getOptions().contains(option)) {
                throw duplicate(option);
            }
        }
    }

    static void verifyStructuralDuplicate(
            RequiredOption current,
            List<Option<?>> optionalOptions,
            List<RequiredOption> requiredOptions) {

        for (Option<?> option : current.getOptions()) {
            if (optionalOptions.contains(option)) {
                throw duplicate(option);
            }

            for (RequiredOption existing : requiredOptions) {
                if (existing.getOptions().contains(option)) {
                    throw duplicate(option);
                }
            }
        }
    }

    static void verifyConditionalTarget(
            Option<?>[] options,
            List<RequiredOption> requiredOptions) {

        for (Option<?> option : options) {
            for (RequiredOption existing : requiredOptions) {
                if (existing
                        instanceof RequiredOption.ConditionalRequiredOptions) {
                    continue;
                }

                if (existing.getOptions().contains(option)) {
                    throw duplicate(option);
                }
            }
        }
    }

    static void verifyConditionalExists(
            Option<?> conditionOption,
            List<Option<?>> optionalOptions,
            List<RequiredOption> requiredOptions) {

        Objects.requireNonNull(
                conditionOption,
                "conditionOption");

        if (optionalOptions.contains(conditionOption)) {
            return;
        }

        for (RequiredOption required : requiredOptions) {
            if (required.getOptions().contains(conditionOption)) {
                return;
            }
        }

        throw new OptionValidationException(
                "Conditional option '%s' is not declared",
                conditionOption.key());
    }

    private static OptionValidationException duplicate(
            Option<?> option) {

        return new OptionValidationException(
                "Option '%s' is declared repeatedly",
                option.key());
    }
}
