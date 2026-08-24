package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.SingleChoiceOption;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.link.up.api.configuration.util.OptionUtil.formatError;

/** Coordinates validation of one {@link OptionRule} tree. */
final class OptionRuleValidator {

    private static final String TYPE_VALUE = "value";
    private static final String TYPE_CONDITIONAL = "conditional";
    private static final String TYPE_SINGLE_CHOICE = "singleChoice";

    private final ReadonlyConfig config;
    private final ConditionExpressionEvaluator conditions;
    private final RequiredOptionValidator requiredOptions;

    OptionRuleValidator(ReadonlyConfig config) {
        this.config = config;
        this.conditions = new ConditionExpressionEvaluator(config);
        this.requiredOptions =
                new RequiredOptionValidator(conditions);
    }

    void validate(OptionRule rule) {
        if (rule == null) {
            return;
        }

        List<String> errors = new ArrayList<String>();
        collectErrors(rule, null, errors);

        if (errors.isEmpty()) {
            return;
        }

        throw new OptionValidationException(
                errorMessage(errors));
    }

    private void collectErrors(
            OptionRule rule,
            Condition<?> activeCondition,
            List<String> errors) {

        Set<String> structurallyAbsentKeys =
                new HashSet<String>();

        for (RequiredOption required :
                rule.getRequiredOptions()) {

            String error =
                    requiredOptions.validate(
                            required,
                            activeCondition);

            if (error != null) {
                errors.add(error);
                requiredOptions.collectAbsentKeys(
                        required,
                        structurallyAbsentKeys);
            }

            if (!requiredOptions.isApplicable(required)) {
                continue;
            }

            validateSingleChoiceOptions(
                    required.getOptions(),
                    errors);
        }

        validateSingleChoiceOptions(
                rule.getOptionalOptions(),
                errors);

        validateConditionalRules(rule, errors);
        validateConstraints(
                rule,
                structurallyAbsentKeys,
                errors);
    }

    private void validateConditionalRules(
            OptionRule rule,
            List<String> errors) {

        for (ConditionRule conditionRule :
                rule.getConditionRules()) {

            Condition<?> condition =
                    conditionRule.getCondition();

            try {
                if (conditions.evaluate(condition)) {
                    collectErrors(
                            conditionRule.getOptionRule(),
                            condition,
                            errors);
                }
            } catch (OptionValidationException failure) {
                errors.add(
                        formatError(
                                condition.toString(),
                                TYPE_CONDITIONAL,
                                failure.getRawMessage()));
            }
        }
    }

    private void validateConstraints(
            OptionRule rule,
            Set<String> structurallyAbsentKeys,
            List<String> errors) {

        for (Condition<?> constraint :
                rule.getValueConstraints()) {

            String optionKey =
                    constraint.getOption().key();

            if (structurallyAbsentKeys.contains(optionKey)) {
                continue;
            }

            if (!conditions.isConstraintApplicable(
                    constraint,
                    rule)) {
                continue;
            }

            try {
                if (!conditions.evaluate(constraint)) {
                    errors.add(
                            formatError(
                                    optionKey,
                                    TYPE_VALUE,
                                    constraint.toString()));
                }
            } catch (OptionValidationException failure) {
                errors.add(
                        formatError(
                                optionKey,
                                TYPE_VALUE,
                                failure.getRawMessage()));
            }
        }
    }

    private void validateSingleChoiceOptions(
            List<? extends Option<?>> options,
            List<String> errors) {

        for (Option<?> option : options) {
            if (option instanceof SingleChoiceOption) {
                validateSingleChoice(
                        (SingleChoiceOption<?>) option,
                        errors);
            }
        }
    }

    private void validateSingleChoice(
            SingleChoiceOption<?> option,
            List<String> errors) {

        List<?> values = option.getOptionValues();

        if (values == null || values.isEmpty()) {
            errors.add(
                    formatError(
                            option.key(),
                            TYPE_SINGLE_CHOICE,
                            "optionValues must not be empty"));
            return;
        }

        Object defaultValue = option.defaultValue();
        if (defaultValue != null
                && !values.contains(defaultValue)) {
            errors.add(
                    formatError(
                            option.key(),
                            TYPE_SINGLE_CHOICE,
                            String.format(
                                    "defaultValue(%s) must be one of %s",
                                    defaultValue,
                                    values)));
        }

        Optional<?> configuredValue =
                config.getOptional(option);

        if (configuredValue.isPresent()
                && !values.contains(configuredValue.get())) {
            errors.add(
                    formatError(
                            option.key(),
                            TYPE_SINGLE_CHOICE,
                            String.format(
                                    "value(%s) must be one of %s",
                                    configuredValue.get(),
                                    values)));
        }
    }

    private static String errorMessage(List<String> errors) {
        StringBuilder message = new StringBuilder();
        message.append(
                String.format(
                        "Option validation failed (%d error%s):",
                        errors.size(),
                        errors.size() > 1 ? "s" : ""));

        for (int index = 0;
             index < errors.size();
             index++) {
            message.append(
                    String.format(
                            "\n  [%d] %s",
                            index + 1,
                            errors.get(index)));
        }

        return message.toString();
    }
}
