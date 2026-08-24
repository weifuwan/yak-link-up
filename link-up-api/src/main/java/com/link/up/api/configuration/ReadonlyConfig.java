package com.link.up.api.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.configuration.util.ConfigUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable snapshot of connector configuration.
 *
 * <p>The source map is copied recursively so callers cannot mutate nested
 * configuration after construction.</p>
 */
@Slf4j
public final class ReadonlyConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper();

    private final Map<String, Object> confData;

    private ReadonlyConfig(Map<String, Object> confData) {
        this.confData = immutableStringMap(confData);
    }

    public static ReadonlyConfig fromMap(
            Map<String, Object> map) {

        Objects.requireNonNull(map, "map");
        return new ReadonlyConfig(map);
    }

    public static ReadonlyConfig fromConfig(Config config) {
        Objects.requireNonNull(config, "config");

        try {
            String json =
                    config.root().render(
                            ConfigRenderOptions.concise());

            Map<String, Object> data =
                    OBJECT_MAPPER.readValue(
                            json,
                            new TypeReference<Map<String, Object>>() {
                            });

            return fromMap(data);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException(
                    "Failed to parse HOCON configuration",
                    failure);
        }
    }

    /** Returns the configured value or the option default. */
    public <T> T get(Option<T> option) {
        return getOptional(option)
                .orElseGet(option::defaultValue);
    }

    /** Returns only an explicitly configured value. */
    public <T> Optional<T> getOptional(Option<T> option) {
        Objects.requireNonNull(option, "option");

        Object value = getValue(option.key());

        if (value == null) {
            value = fallbackValue(option);
        }

        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(
                ConfigUtil.convertValue(value, option));
    }

    public Map<String, String> toMap() {
        if (confData.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> result =
                new LinkedHashMap<String, String>();

        for (Map.Entry<String, Object> entry :
                confData.entrySet()) {
            result.put(
                    entry.getKey(),
                    ConfigUtil.convertToJsonString(
                            entry.getValue()));
        }

        return result;
    }

    /**
     * Returns the immutable source snapshot used for option/path validation.
     */
    public Map<String, Object> getSourceMap() {
        return confData;
    }

    private <T> Object fallbackValue(Option<T> option) {
        for (String fallbackKey : option.getFallbackKeys()) {
            Object value = getValue(fallbackKey);

            if (value == null) {
                continue;
            }

            log.warn(
                    "Please use the new key '{}' instead of deprecated key '{}'",
                    option.key(),
                    fallbackKey);
            return value;
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private Object getValue(String key) {
        if (confData.containsKey(key)) {
            return confData.get(key);
        }

        String[] paths = key.split("\\.");
        Map<String, Object> current = confData;

        for (int index = 0; index < paths.length; index++) {
            Object value = current.get(paths[index]);

            if (index == paths.length - 1) {
                return value;
            }

            if (!(value instanceof Map)) {
                return null;
            }

            current = (Map<String, Object>) value;
        }

        return null;
    }

    private static Map<String, Object> immutableStringMap(
            Map<String, Object> source) {

        Map<String, Object> copy =
                new LinkedHashMap<String, Object>();

        for (Map.Entry<String, Object> entry :
                source.entrySet()) {
            copy.put(
                    entry.getKey(),
                    immutableValue(entry.getValue()));
        }

        return Collections.unmodifiableMap(copy);
    }

    private static Object immutableValue(Object value) {
        if (value instanceof Map) {
            Map<?, ?> source = (Map<?, ?>) value;
            Map<Object, Object> copy =
                    new LinkedHashMap<Object, Object>();

            for (Map.Entry<?, ?> entry : source.entrySet()) {
                copy.put(
                        entry.getKey(),
                        immutableValue(entry.getValue()));
            }

            return Collections.unmodifiableMap(copy);
        }

        if (value instanceof List) {
            List<Object> copy = new ArrayList<Object>();
            for (Object item : (List<?>) value) {
                copy.add(immutableValue(item));
            }
            return Collections.unmodifiableList(copy);
        }

        if (value instanceof Set) {
            Set<Object> copy =
                    new LinkedHashSet<Object>();
            for (Object item : (Set<?>) value) {
                copy.add(immutableValue(item));
            }
            return Collections.unmodifiableSet(copy);
        }

        if (value instanceof Collection) {
            List<Object> copy = new ArrayList<Object>();
            for (Object item : (Collection<?>) value) {
                copy.add(immutableValue(item));
            }
            return Collections.unmodifiableList(copy);
        }

        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReadonlyConfig)) {
            return false;
        }

        ReadonlyConfig that = (ReadonlyConfig) obj;
        return Objects.equals(confData, that.confData);
    }

    @Override
    public int hashCode() {
        return confData.hashCode();
    }

    @Override
    public String toString() {
        return ConfigUtil.convertToJsonString(confData);
    }
}
