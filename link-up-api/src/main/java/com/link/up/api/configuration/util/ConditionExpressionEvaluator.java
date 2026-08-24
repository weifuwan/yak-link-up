package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.ReadonlyConfig;

import java.util.HashSet;
import java.util.Set;

/** Evaluates condition chains using Link-Up's AND-before-OR semantics. */
final class ConditionExpressionEvaluator {

    private final ReadonlyConfig config;

    ConditionExpressionEvaluator(ReadonlyConfig config) {
        this.config = config;
    }

    boolean evaluate(Condition<?> condition) {
        Condition<?> current = condition;

        while (current != null) {
            boolean groupResult = true;

            while (current != null) {
                if (groupResult) {
                    groupResult =
                            ConditionEvaluators.evaluate(current, config);
                }

                if (!current.hasNext()) {
                    current = null;
                    break;
                }

                boolean isAnd = Boolean.TRUE.equals(current.and());
                current = current.getNext();

                if (!isAnd) {
                    break;
                }
            }

            if (groupResult) {
                return true;
            }
        }

        return false;
    }

    boolean isConstraintApplicable(
            Condition<?> condition,
            OptionRule rule) {

        Option<?> headOption = condition.getOption();

        for (RequiredOption required : rule.getRequiredOptions()) {
            if (required
                    instanceof RequiredOption.AbsolutelyRequiredOptions
                    && required.getOptions().contains(headOption)) {
                return true;
            }
        }

        return anyOrSegmentFullyPresent(condition);
    }

    private boolean anyOrSegmentFullyPresent(Condition<?> condition) {
        Condition<?> current = condition;

        while (current != null) {
            Set<Option<?>> segmentOptions = new HashSet<Option<?>>();

            while (current != null) {
                segmentOptions.add(current.getOption());
                if (current.getCompareOption() != null) {
                    segmentOptions.add(current.getCompareOption());
                }

                if (!current.hasNext()) {
                    current = null;
                    break;
                }

                boolean isAnd = Boolean.TRUE.equals(current.and());
                current = current.getNext();

                if (!isAnd) {
                    break;
                }
            }

            if (allPresent(segmentOptions)) {
                return true;
            }
        }

        return false;
    }

    private boolean allPresent(Set<Option<?>> options) {
        for (Option<?> option : options) {
            if (!hasOption(option)) {
                return false;
            }
        }
        return true;
    }

    boolean hasOption(Option<?> option) {
        return config.getOptional(option).isPresent();
    }
}
