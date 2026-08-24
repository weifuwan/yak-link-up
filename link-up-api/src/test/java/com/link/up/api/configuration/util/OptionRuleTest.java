package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OptionRuleTest {

    private static final Option<String> MODE =
            Options.key("mode")
                    .stringType()
                    .noDefaultValue();

    private static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue();

    private static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue();

    @Test
    public void shouldRejectStructuralDuplicateOption() {
        try {
            OptionRule.builder()
                    .optional(MODE)
                    .required(MODE);
            fail("Expected duplicate declaration failure");
        } catch (OptionValidationException failure) {
            assertEquals(
                    "Option 'mode' is declared repeatedly",
                    failure.getRawMessage());
        }
    }

    @Test
    public void shouldRejectUndeclaredConditionalTrigger() {
        try {
            OptionRule.builder()
                    .conditional(
                            MODE,
                            "TABLE",
                            TABLE);
            fail("Expected undeclared trigger failure");
        } catch (OptionValidationException failure) {
            assertEquals(
                    "Conditional option 'mode' is not declared",
                    failure.getRawMessage());
        }
    }

    @Test
    public void shouldMergeRulesWithEquivalentCondition() {
        OptionRule tableRule =
                OptionRule.builder()
                        .optional(TABLE)
                        .build();

        OptionRule queryRule =
                OptionRule.builder()
                        .optional(QUERY)
                        .build();

        OptionRule rule =
                OptionRule.builder()
                        .required(MODE)
                        .ruleWhen(
                                Conditions.equalTo(
                                        MODE,
                                        "TABLE"),
                                tableRule)
                        .ruleWhen(
                                Conditions.equalTo(
                                        MODE,
                                        "TABLE"),
                                queryRule)
                        .build();

        assertEquals(1, rule.getConditionRules().size());

        List<Option<?>> nested =
                rule.getConditionRules()
                        .get(0)
                        .getOptionRule()
                        .getOptionalOptions();

        assertEquals(2, nested.size());
        assertEquals(TABLE, nested.get(0));
        assertEquals(QUERY, nested.get(1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldExposeImmutableRuleCollections() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(TABLE)
                        .build();

        rule.getOptionalOptions().add(QUERY);
    }
}
