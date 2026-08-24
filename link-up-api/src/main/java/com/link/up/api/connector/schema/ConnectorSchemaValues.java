package com.link.up.api.connector.schema;

import com.link.up.api.configuration.Option;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Protocol-safe value and Java type conversion used by schema exporters. */
final class ConnectorSchemaValues {

    private ConnectorSchemaValues() {
    }

    static ConnectorOptionValueType valueType(Type type) {
        if (type == Boolean.class || type == boolean.class) {
            return ConnectorOptionValueType.BOOLEAN;
        }
        if (type == Integer.class
                || type == int.class
                || type == Short.class
                || type == short.class
                || type == Byte.class
                || type == byte.class) {
            return ConnectorOptionValueType.INTEGER;
        }
        if (type == Long.class || type == long.class) {
            return ConnectorOptionValueType.LONG;
        }
        if (type == BigDecimal.class) {
            return ConnectorOptionValueType.DECIMAL;
        }
        if (type == Float.class || type == float.class) {
            return ConnectorOptionValueType.FLOAT;
        }
        if (type == Double.class || type == double.class) {
            return ConnectorOptionValueType.DOUBLE;
        }
        if (type == String.class
                || type == Character.class
                || type == char.class) {
            return ConnectorOptionValueType.STRING;
        }
        if (type == Duration.class) {
            return ConnectorOptionValueType.DURATION;
        }
        if (type instanceof Class
                && ((Class<?>) type).isEnum()) {
            return ConnectorOptionValueType.ENUM;
        }

        Class<?> rawType = rawType(type);
        if (rawType != null
                && Map.class.isAssignableFrom(rawType)) {
            return ConnectorOptionValueType.MAP;
        }
        if (rawType != null
                && Collection.class.isAssignableFrom(rawType)) {
            return ConnectorOptionValueType.LIST;
        }
        if (type instanceof Class) {
            return ConnectorOptionValueType.OBJECT;
        }

        return ConnectorOptionValueType.UNKNOWN;
    }

    static String elementJavaType(Type type) {
        if (!(type instanceof ParameterizedType)) {
            return null;
        }

        ParameterizedType parameterizedType =
                (ParameterizedType) type;
        Type raw = parameterizedType.getRawType();

        if (!(raw instanceof Class)
                || !Collection.class.isAssignableFrom(
                        (Class<?>) raw)) {
            return null;
        }

        Type[] arguments =
                parameterizedType.getActualTypeArguments();

        return arguments.length == 0
                ? null
                : arguments[0].getTypeName();
    }

    static List<String> optionKeys(
            List<? extends Option<?>> options) {

        List<String> result =
                new ArrayList<String>(options.size());

        for (Option<?> option : options) {
            result.add(option.key());
        }

        return result;
    }

    static Object jsonValue(Object value) {
        if (value == null
                || value instanceof String
                || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }

        if (value instanceof Enum) {
            return ((Enum<?>) value).name();
        }

        if (value instanceof Duration) {
            return value.toString();
        }

        if (value instanceof Map) {
            Map<?, ?> source = (Map<?, ?>) value;
            Map<String, Object> result =
                    new TreeMap<String, Object>();

            for (Map.Entry<?, ?> entry : source.entrySet()) {
                result.put(
                        String.valueOf(entry.getKey()),
                        jsonValue(entry.getValue()));
            }
            return result;
        }

        if (value instanceof Collection) {
            List<Object> result = new ArrayList<Object>();
            for (Object item : (Collection<?>) value) {
                result.add(jsonValue(item));
            }
            return result;
        }

        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<Object> result =
                    new ArrayList<Object>(length);

            for (int index = 0; index < length; index++) {
                result.add(
                        jsonValue(Array.get(value, index)));
            }
            return result;
        }

        /*
         * Connector-specific objects must not leak plugin implementation
         * classes into the stable control-plane schema.
         */
        return String.valueOf(value);
    }

    private static Class<?> rawType(Type type) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }

        if (!(type instanceof ParameterizedType)) {
            return null;
        }

        Type raw =
                ((ParameterizedType) type).getRawType();
        return raw instanceof Class
                ? (Class<?>) raw
                : null;
    }
}
