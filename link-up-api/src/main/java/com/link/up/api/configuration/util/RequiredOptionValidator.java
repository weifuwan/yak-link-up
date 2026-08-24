package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.link.up.api.configuration.util.OptionUtil.formatError;
import static com.link.up.api.configuration.util.OptionUtil.formatOptionsError;
import static com.link.up.api.configuration.util.OptionUtil.getOptionKeys;

/** Validates required-option structures without owning rule traversal. */
final class RequiredOptionValidator {

    static final String TYPE_REQUIRED = "required";
    static final String TYPE_BUNDLED = "bundled";
    static final String TYPE_EXCLUSIVE = "exclusive";
    static final String TYPE_CONDITIONAL = "conditional";

    private final ConditionExpressionEvaluator conditions;

    RequiredOptionValidator(ConditionExpressionEvaluator conditions) {
        this.conditions = conditions;
    }

    String validate(
            RequiredOption requiredOption,
            Condition<?> activeCondition) {

        if (requiredOption
                instanceof RequiredOption.AbsolutelyRequiredOptions) {
            return validateAbsolute(
                    (RequiredOption.AbsolutelyRequiredOptions)
                            requiredOption,
                    activeCondition);
        }

        if (requiredOption
                instanceof RequiredOption.BundledRequiredOptions) {
            return validateBundled(
                    (RequiredOption.BundledRequiredOptions)
                            requiredOption);
        }

        if (requiredOption
                instanceof RequiredOption.ExclusiveRequiredOptions) {
            return validateExclusive(
                    (RequiredOption.ExclusiveRequiredOptions)
                            requiredOption);
        }

        if (requiredOption
                instanceof RequiredOption.ConditionalRequiredOptions) {
            return validateConditional(
                    (RequiredOption.ConditionalRequiredOptions)
                            requiredOption);
        }

        throw new UnsupportedOperationException(
                "Unsupported required option type: "
                        + requiredOption.getClass().getName());
    }

    boolean isApplicable(RequiredOption requiredOption) {
        if (!(requiredOption
                instanceof RequiredOption.ConditionalRequiredOptions)) {
            return true;
        }

        RequiredOption.ConditionalRequiredOptions conditional =
                (RequiredOption.ConditionalRequiredOptions)
                        requiredOption;
        return conditions.evaluate(conditional.getCondition());
    }

    void collectAbsentKeys(
            RequiredOption requiredOption,
            Set<String> absentKeys) {

        if (!isApplicable(requiredOption)) {
            return;
        }

        for (Option<?> option :
                absentOptions(requiredOption.getOptions())) {
            absentKeys.add(option.key());
        }
    }

    private String validateAbsolute(
            RequiredOption.AbsolutelyRequiredOptions required,
            Condition<?> activeCondition) {

        List<Option<?>> absent =
                absentOptions(required.getRequiredOption());
        if (absent.isEmpty()) {
            return null;
        }

        String hint = activeCondition == null
                ? ""
                : " when [" + activeCondition + "]";

        return formatError(
                getOptionKeys(absent),
                TYPE_REQUIRED,
                "required option is not configured" + hint);
    }

    private String validateBundled(
            RequiredOption.BundledRequiredOptions bundled) {

        List<Option<?>> present = new ArrayList<Option<?>>();
        List<Option<?>> absent = new ArrayList<Option<?>>();

        for (Option<?> option : bundled.getRequiredOption()) {
            if (conditions.hasOption(option)) {
                present.add(option);
            } else {
                absent.add(option);
            }
        }

        if (present.isEmpty() || absent.isEmpty()) {
            return null;
        }

        return formatOptionsError(
                getOptionKeys(bundled.getRequiredOption()),
                TYPE_BUNDLED,
                String.format(
                        "bundled options must be present or absent together "
                                + "(present: [%s], absent: [%s])",
                        getOptionKeys(present),
                        getOptionKeys(absent)));
    }

    private String validateExclusive(
            RequiredOption.ExclusiveRequiredOptions exclusive) {

        List<Option<?>> present = new ArrayList<Option<?>>();

        for (Option<?> option : exclusive.getExclusiveOptions()) {
            if (conditions.hasOption(option)) {
                present.add(option);
            }
        }

        if (present.size() == 1) {
            return null;
        }

        if (present.isEmpty()) {
            return formatOptionsError(
                    getOptionKeys(exclusive.getExclusiveOptions()),
                    TYPE_EXCLUSIVE,
                    "exactly one option must be configured");
        }

        return formatOptionsError(
                getOptionKeys(exclusive.getExclusiveOptions()),
                TYPE_EXCLUSIVE,
                "multiple exclusive options are configured: "
                        + getOptionKeys(present));
    }

    private String validateConditional(
            RequiredOption.ConditionalRequiredOptions conditional) {

        if (!conditions.evaluate(conditional.getCondition())) {
            return null;
        }

        List<Option<?>> absent =
                absentOptions(conditional.getRequiredOption());

        if (absent.isEmpty()) {
            return null;
        }

        return formatError(
                getOptionKeys(absent),
                TYPE_CONDITIONAL,
                String.format(
                        "required because [%s] is true",
                        conditional.getCondition()));
    }

    private List<Option<?>> absentOptions(
            List<Option<?>> options) {

        List<Option<?>> absent = new ArrayList<Option<?>>();

        for (Option<?> option : options) {
            if (!conditions.hasOption(option)
                    && option.defaultValue() == null) {
                absent.add(option);
            }
        }

        return absent;
    }
}
