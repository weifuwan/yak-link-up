package com.link.up.api.configuration.util;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.ReadonlyConfig;

/**
 * Validates connector configuration against an {@link OptionRule}.
 *
 * <p>This public facade preserves the extension API while delegating
 * validation mechanics to focused package-private roles.</p>
 */
public final class ConfigValidator {

    private final OptionRuleValidator ruleValidator;

    private ConfigValidator(ReadonlyConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.ruleValidator = new OptionRuleValidator(config);
    }

    public static ConfigValidator of(ReadonlyConfig config) {
        return new ConfigValidator(config);
    }

    /**
     * Validates that the configuration contains only declared option keys.
     *
     * @param commonOptions common options accepted in addition to the rule
     */
    public static void validateUnknownKeys(
            ReadonlyConfig config,
            OptionRule rule,
            String connectorName,
            Option<?>... commonOptions) {

        OptionKeyValidator.validate(
                config,
                rule,
                connectorName,
                commonOptions);
    }

    public void validate(OptionRule rule) {
        ruleValidator.validate(rule);
    }
}
