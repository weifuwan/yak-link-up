package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable connector option rules and the public builder DSL used by
 * connector factories.
 */
public final class OptionRule {

    private final List<Option<?>> optionalOptions;
    private final List<RequiredOption> requiredOptions;
    private final List<ConditionRule> conditionRules;
    private final List<Condition<?>> valueConstraints;

    private OptionRule(
            List<Option<?>> optionalOptions,
            List<RequiredOption> requiredOptions,
            List<ConditionRule> conditionRules,
            List<Condition<?>> valueConstraints) {

        this.optionalOptions = immutableCopy(optionalOptions);
        this.requiredOptions = immutableCopy(requiredOptions);
        this.conditionRules = immutableCopy(conditionRules);
        this.valueConstraints = immutableCopy(valueConstraints);
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Option<?>> getOptionalOptions() {
        return optionalOptions;
    }

    public List<RequiredOption> getRequiredOptions() {
        return requiredOptions;
    }

    public List<ConditionRule> getConditionRules() {
        return conditionRules;
    }

    public List<Condition<?>> getValueConstraints() {
        return valueConstraints;
    }

    public boolean hasOptions() {
        return !optionalOptions.isEmpty()
                || !requiredOptions.isEmpty()
                || !conditionRules.isEmpty()
                || !valueConstraints.isEmpty();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof OptionRule)) {
            return false;
        }

        OptionRule that = (OptionRule) obj;
        return Objects.equals(optionalOptions, that.optionalOptions)
                && Objects.equals(requiredOptions, that.requiredOptions)
                && Objects.equals(conditionRules, that.conditionRules)
                && Objects.equals(valueConstraints, that.valueConstraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                optionalOptions,
                requiredOptions,
                conditionRules,
                valueConstraints);
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        Objects.requireNonNull(values, "values");
        return Collections.unmodifiableList(
                new ArrayList<T>(values));
    }

    public static final class Builder {

        private final List<Option<?>> optionalOptions =
                new ArrayList<Option<?>>();

        private final List<RequiredOption> requiredOptions =
                new ArrayList<RequiredOption>();

        private final List<ConditionRule> conditionRules =
                new ArrayList<ConditionRule>();

        private final List<Condition<?>> valueConstraints =
                new ArrayList<Condition<?>>();

        private Builder() {
        }

        public Builder optional(Option<?>... options) {
            OptionRuleBuilderSupport.requireOptions(options);

            for (Option<?> option : options) {
                OptionRuleBuilderSupport.verifyOptionalDuplicate(
                        option,
                        optionalOptions,
                        requiredOptions);
                optionalOptions.add(option);
            }

            return this;
        }

        public Builder required(Option<?>... options) {
            OptionRuleBuilderSupport.requireOptions(options);

            RequiredOption required =
                    RequiredOption.AbsolutelyRequiredOptions.of(
                            options);

            OptionRuleBuilderSupport.verifyStructuralDuplicate(
                    required,
                    optionalOptions,
                    requiredOptions);
            requiredOptions.add(required);
            return this;
        }

        public Builder exclusive(Option<?>... options) {
            OptionRuleBuilderSupport.requireOptions(options);

            if (options.length < 2) {
                throw new OptionValidationException(
                        "Exclusive options must contain at least two options");
            }

            RequiredOption required =
                    RequiredOption.ExclusiveRequiredOptions.of(
                            options);

            OptionRuleBuilderSupport.verifyStructuralDuplicate(
                    required,
                    optionalOptions,
                    requiredOptions);
            requiredOptions.add(required);
            return this;
        }

        public Builder bundled(Option<?>... options) {
            OptionRuleBuilderSupport.requireOptions(options);

            if (options.length < 2) {
                throw new OptionValidationException(
                        "Bundled options must contain at least two options");
            }

            RequiredOption required =
                    RequiredOption.BundledRequiredOptions.of(
                            options);

            OptionRuleBuilderSupport.verifyStructuralDuplicate(
                    required,
                    optionalOptions,
                    requiredOptions);
            requiredOptions.add(required);
            return this;
        }

        public <T> Builder conditional(
                Option<T> conditionOption,
                T expectedValue,
                Option<?>... required) {

            verifyConditionalExists(conditionOption);
            return requiredWhen(
                    Condition.of(
                            conditionOption,
                            expectedValue),
                    required);
        }

        public <T> Builder conditional(
                Option<T> conditionOption,
                List<T> expectedValues,
                Option<?>... required) {

            verifyConditionalExists(conditionOption);
            return requiredWhen(
                    OptionRuleBuilderSupport.anyOf(
                            conditionOption,
                            expectedValues),
                    required);
        }

        public Builder requiredWhen(
                Condition<?> condition,
                Option<?>... required) {

            Objects.requireNonNull(condition, "condition");
            OptionRuleBuilderSupport.requireOptions(required);

            OptionRuleBuilderSupport.verifyConditionalTarget(
                    required,
                    requiredOptions);

            RequiredOption conditional =
                    RequiredOption.ConditionalRequiredOptions.of(
                            condition,
                            Arrays.asList(required));

            requiredOptions.add(conditional);
            return this;
        }

        public <T> Builder conditionalRule(
                Option<T> conditionOption,
                T expectedValue,
                OptionRule rule) {

            verifyConditionalExists(conditionOption);
            return ruleWhen(
                    Condition.of(
                            conditionOption,
                            expectedValue),
                    rule);
        }

        public <T> Builder conditionalRule(
                Option<T> conditionOption,
                List<T> expectedValues,
                OptionRule rule) {

            verifyConditionalExists(conditionOption);
            return ruleWhen(
                    OptionRuleBuilderSupport.anyOf(
                            conditionOption,
                            expectedValues),
                    rule);
        }

        public Builder ruleWhen(
                Condition<?> condition,
                OptionRule rule) {

            Objects.requireNonNull(condition, "condition");
            Objects.requireNonNull(rule, "rule");

            if (!rule.hasOptions()) {
                throw new OptionValidationException(
                        "Conditional rule must not be empty");
            }

            mergeConditionRule(condition, rule);
            return this;
        }

        public Builder required(
                Option<?> option,
                Condition<?> firstCondition,
                Condition<?>... otherConditions) {

            required(option);
            addConstraints(
                    firstCondition,
                    otherConditions);
            return this;
        }

        public Builder required(
                Option<?> firstOption,
                Option<?> secondOption,
                Condition<?> firstCondition,
                Condition<?>... otherConditions) {

            required(
                    firstOption,
                    secondOption);
            addConstraints(
                    firstCondition,
                    otherConditions);
            return this;
        }

        public Builder optional(
                Option<?> option,
                Condition<?> firstCondition,
                Condition<?>... otherConditions) {

            optional(option);
            addConstraints(
                    firstCondition,
                    otherConditions);
            return this;
        }

        public Builder optional(
                Option<?> firstOption,
                Option<?> secondOption,
                Condition<?> firstCondition,
                Condition<?>... otherConditions) {

            optional(
                    firstOption,
                    secondOption);
            addConstraints(
                    firstCondition,
                    otherConditions);
            return this;
        }

        public <T> Builder conditional(
                Option<T> conditionOption,
                T expectedValue,
                Condition<?> firstCondition,
                Condition<?>... otherConditions) {

            verifyConditionalExists(conditionOption);
            return constraintsWhen(
                    Condition.of(
                            conditionOption,
                            expectedValue),
                    firstCondition,
                    otherConditions);
        }

        public Builder constraintsWhen(
                Condition<?> trigger,
                Condition<?> firstCondition,
                Condition<?>... otherConditions) {

            Objects.requireNonNull(trigger, "trigger");

            List<Condition<?>> constraints =
                    new ArrayList<Condition<?>>();

            constraints.add(
                    Objects.requireNonNull(
                            firstCondition,
                            "firstCondition"));

            if (otherConditions != null) {
                Collections.addAll(
                        constraints,
                        otherConditions);
            }

            OptionRule rule =
                    new OptionRule(
                            Collections.<Option<?>>emptyList(),
                            Collections.<RequiredOption>emptyList(),
                            Collections.<ConditionRule>emptyList(),
                            constraints);

            mergeConditionRule(trigger, rule);
            return this;
        }

        public OptionRule build() {
            return new OptionRule(
                    optionalOptions,
                    requiredOptions,
                    conditionRules,
                    valueConstraints);
        }

        private void addConstraints(
                Condition<?> firstCondition,
                Condition<?>[] otherConditions) {

            valueConstraints.add(
                    Objects.requireNonNull(
                            firstCondition,
                            "firstCondition"));

            if (otherConditions != null) {
                Collections.addAll(
                        valueConstraints,
                        otherConditions);
            }
        }

        private void mergeConditionRule(
                Condition<?> condition,
                OptionRule newRule) {

            for (int index = 0;
                 index < conditionRules.size();
                 index++) {

                ConditionRule current =
                        conditionRules.get(index);

                if (!current.getCondition().equals(condition)) {
                    continue;
                }

                OptionRule oldRule =
                        current.getOptionRule();

                OptionRule merged =
                        new OptionRule(
                                OptionRuleBuilderSupport.merge(
                                        oldRule.optionalOptions,
                                        newRule.optionalOptions),
                                OptionRuleBuilderSupport.merge(
                                        oldRule.requiredOptions,
                                        newRule.requiredOptions),
                                OptionRuleBuilderSupport.merge(
                                        oldRule.conditionRules,
                                        newRule.conditionRules),
                                OptionRuleBuilderSupport.merge(
                                        oldRule.valueConstraints,
                                        newRule.valueConstraints));

                conditionRules.set(
                        index,
                        new ConditionRule(
                                condition,
                                merged));
                return;
            }

            conditionRules.add(
                    new ConditionRule(
                            condition,
                            newRule));
        }

        private void verifyConditionalExists(
                Option<?> conditionOption) {

            OptionRuleBuilderSupport.verifyConditionalExists(
                    conditionOption,
                    optionalOptions,
                    requiredOptions);
        }
    }
}
