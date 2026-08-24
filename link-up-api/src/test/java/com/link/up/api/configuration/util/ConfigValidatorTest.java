package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigValidatorTest {

    private static final Option<Integer> A =
            Options.key("a")
                    .intType()
                    .noDefaultValue();

    private static final Option<Integer> B =
            Options.key("b")
                    .intType()
                    .noDefaultValue();

    private static final Option<Integer> C =
            Options.key("c")
                    .intType()
                    .noDefaultValue();

    @Test
    public void shouldAggregateIndependentRequiredErrors() {
        OptionRule rule =
                OptionRule.builder()
                        .required(A)
                        .required(B)
                        .build();

        try {
            ConfigValidator.of(
                    ReadonlyConfig.fromMap(
                            new LinkedHashMap<String, Object>()))
                    .validate(rule);
            fail("Expected validation failure");
        } catch (OptionValidationException failure) {
            assertTrue(
                    failure.getRawMessage()
                            .contains(
                                    "Option validation failed (2 errors):"));
            assertTrue(
                    failure.getRawMessage().contains("'a'"));
            assertTrue(
                    failure.getRawMessage().contains("'b'"));
        }
    }

    @Test
    public void shouldRespectAndBeforeOrConditionPrecedence() {
        Condition<Integer> expression =
                Conditions.greaterThan(A, 0)
                        .and(Conditions.lessThan(B, 10))
                        .or(Conditions.equalTo(C, 7));

        OptionRule rule =
                OptionRule.builder()
                        .required(A, expression)
                        .optional(B, C)
                        .build();

        Map<String, Object> valid =
                new LinkedHashMap<String, Object>();
        valid.put("a", 1);
        valid.put("b", 20);
        valid.put("c", 7);

        ConfigValidator.of(
                ReadonlyConfig.fromMap(valid))
                .validate(rule);

        Map<String, Object> invalid =
                new LinkedHashMap<String, Object>();
        invalid.put("a", 1);
        invalid.put("b", 20);
        invalid.put("c", 6);

        try {
            ConfigValidator.of(
                    ReadonlyConfig.fromMap(invalid))
                    .validate(rule);
            fail("Expected value constraint failure");
        } catch (OptionValidationException failure) {
            assertTrue(
                    failure.getRawMessage()
                            .contains("type: value"));
        }
    }

    @Test
    public void shouldReportUnknownNestedConfigurationPath() {
        Option<String> jdbcUrl =
                Options.key("jdbc.url")
                        .stringType()
                        .noDefaultValue();

        OptionRule rule =
                OptionRule.builder()
                        .required(jdbcUrl)
                        .build();

        Map<String, Object> jdbc =
                new LinkedHashMap<String, Object>();
        jdbc.put("url", "jdbc:test");
        jdbc.put("timeout", 30);

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();
        values.put("jdbc", jdbc);

        try {
            ConfigValidator.validateUnknownKeys(
                    ReadonlyConfig.fromMap(values),
                    rule,
                    "test");
            fail("Expected unknown-key failure");
        } catch (OptionValidationException failure) {
            assertTrue(
                    failure.getRawMessage()
                            .contains("jdbc.timeout"));
        }
    }
}
