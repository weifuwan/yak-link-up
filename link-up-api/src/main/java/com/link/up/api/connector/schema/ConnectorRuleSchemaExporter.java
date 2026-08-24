package com.link.up.api.connector.schema;

import com.link.up.api.configuration.util.Condition;
import com.link.up.api.configuration.util.ConditionRule;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.configuration.util.RequiredOption;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Exports validation rules and condition chains into protocol-safe models. */
final class ConnectorRuleSchemaExporter {

    List<ConnectorRuleSchema> export(OptionRule rule) {
        List<ConnectorRuleSchema> result =
                new ArrayList<ConnectorRuleSchema>();

        exportRequiredRules(rule, result);
        exportValueConstraints(rule, result);
        exportConditionalRules(rule, result);

        return Collections.unmodifiableList(result);
    }

    private void exportRequiredRules(
            OptionRule rule,
            List<ConnectorRuleSchema> result) {

        for (RequiredOption required :
                rule.getRequiredOptions()) {
            ConnectorRuleSchema exported =
                    exportRequiredRule(required);
            if (exported != null) {
                result.add(exported);
            }
        }
    }

    private ConnectorRuleSchema exportRequiredRule(
            RequiredOption required) {

        if (required
                instanceof RequiredOption.AbsolutelyRequiredOptions) {
            return simpleRule(
                    ConnectorRuleSchema.REQUIRED,
                    required);
        }

        if (required
                instanceof RequiredOption.ExclusiveRequiredOptions) {
            return simpleRule(
                    ConnectorRuleSchema.EXCLUSIVE,
                    required);
        }

        if (required
                instanceof RequiredOption.BundledRequiredOptions) {
            return simpleRule(
                    ConnectorRuleSchema.BUNDLED,
                    required);
        }

        if (required
                instanceof RequiredOption.ConditionalRequiredOptions) {
            RequiredOption.ConditionalRequiredOptions conditional =
                    (RequiredOption.ConditionalRequiredOptions)
                            required;

            return new ConnectorRuleSchema(
                    ConnectorRuleSchema.REQUIRED_WHEN,
                    ConnectorSchemaValues.optionKeys(
                            conditional.getOptions()),
                    exportCondition(
                            conditional.getCondition()),
                    null);
        }

        return null;
    }

    private static ConnectorRuleSchema simpleRule(
            String type,
            RequiredOption required) {

        return new ConnectorRuleSchema(
                type,
                ConnectorSchemaValues.optionKeys(
                        required.getOptions()),
                null,
                null);
    }

    private void exportValueConstraints(
            OptionRule rule,
            List<ConnectorRuleSchema> result) {

        for (Condition<?> constraint :
                rule.getValueConstraints()) {
            result.add(
                    new ConnectorRuleSchema(
                            ConnectorRuleSchema.CONSTRAINT,
                            Collections.singletonList(
                                    constraint.getOption().key()),
                            exportCondition(constraint),
                            null));
        }
    }

    private void exportConditionalRules(
            OptionRule rule,
            List<ConnectorRuleSchema> result) {

        for (ConditionRule conditionRule :
                rule.getConditionRules()) {

            OptionRule nested =
                    conditionRule.getOptionRule();

            result.add(
                    new ConnectorRuleSchema(
                            ConnectorRuleSchema.RULE_WHEN,
                            ConnectorSchemaValues.optionKeys(
                                    nested.getOptionalOptions()),
                            exportCondition(
                                    conditionRule.getCondition()),
                            export(nested)));
        }
    }

    private ConnectorConditionSchema exportCondition(
            Condition<?> condition) {

        if (condition == null) {
            return null;
        }

        String logicalOperator = null;
        if (condition.getNext() != null) {
            logicalOperator =
                    Boolean.TRUE.equals(condition.and())
                            ? "AND"
                            : "OR";
        }

        String extensionDescription =
                condition.getExtension() == null
                        ? null
                        : condition.getExtension().description();

        return new ConnectorConditionSchema(
                condition.getOption().key(),
                condition.getOperator().name(),
                ConnectorSchemaValues.jsonValue(
                        condition.getExpectedValue()),
                condition.getCompareOption() == null
                        ? null
                        : condition.getCompareOption().key(),
                extensionDescription,
                logicalOperator,
                exportCondition(condition.getNext()));
    }
}
