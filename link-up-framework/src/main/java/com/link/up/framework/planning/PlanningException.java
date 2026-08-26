package com.link.up.framework.planning;

import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.connector.schema.ConnectorRole;
import com.link.up.api.exception.FluxRuntimeException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Structured, Secret-safe failure raised by planning and negotiation. */
public final class PlanningException
        extends FluxRuntimeException {

    private static final long serialVersionUID = 1L;

    private final PlanningErrorCode errorCode;
    private final Map<String, String> parameters;

    public PlanningException(
            PlanningErrorCode errorCode,
            String message,
            Map<String, String> parameters,
            Throwable cause) {

        super(
                Objects.requireNonNull(
                        errorCode,
                        "errorCode must not be null"),
                safeMessage(message),
                cause);

        this.errorCode = errorCode;
        this.parameters = immutable(parameters);
    }

    public static PlanningException invalidRequest(
            String reason,
            Throwable cause) {

        return new PlanningException(
                PlanningErrorCode.INVALID_REQUEST,
                "Planning request is invalid",
                parameters("reason", reason),
                cause);
    }

    public static PlanningException invalidDefinition(
            String format,
            Throwable cause) {

        return new PlanningException(
                PlanningErrorCode.INVALID_DEFINITION,
                "Job definition could not be compiled",
                parameters("format", format),
                cause);
    }

    public static PlanningException connectorNotFound(
            ConnectorRole role,
            String connectorId,
            Throwable cause) {

        Map<String, String> parameters =
                parameters(
                        "role",
                        role.name());
        parameters.put(
                "connectorId",
                safeParameter(connectorId));

        return new PlanningException(
                PlanningErrorCode.CONNECTOR_NOT_FOUND,
                role.name()
                        + " connector could not be resolved",
                parameters,
                cause);
    }

    public static PlanningException connectorOptionsInvalid(
            ConnectorRole role,
            String connectorId,
            Throwable cause) {

        Map<String, String> parameters =
                parameters(
                        "role",
                        role.name());
        parameters.put(
                "connectorId",
                safeParameter(connectorId));

        return new PlanningException(
                PlanningErrorCode.CONNECTOR_OPTIONS_INVALID,
                role.name()
                        + " connector options are invalid",
                parameters,
                cause);
    }

    public static PlanningException requiredCapabilityMissing(
            ConnectorRole role,
            String connectorId,
            ConnectorCapability capability) {

        Map<String, String> parameters =
                parameters(
                        "role",
                        role.name());
        parameters.put(
                "connectorId",
                safeParameter(connectorId));
        parameters.put(
                "capability",
                capability.name());

        return new PlanningException(
                PlanningErrorCode.REQUIRED_CAPABILITY_MISSING,
                role.name()
                        + " connector does not support required capability "
                        + capability.name(),
                parameters,
                null);
    }

    public static PlanningException sourcePreparationFailed(
            String connectorId,
            Throwable cause) {

        return new PlanningException(
                PlanningErrorCode.SOURCE_PREPARATION_FAILED,
                "Source preparation or schema discovery failed",
                parameters(
                        "connectorId",
                        connectorId),
                cause);
    }

    public static PlanningException splitDiscoveryFailed(
            String connectorId,
            Throwable cause) {

        return new PlanningException(
                PlanningErrorCode.SPLIT_DISCOVERY_FAILED,
                "Source split discovery failed",
                parameters(
                        "connectorId",
                        connectorId),
                cause);
    }

    public static PlanningException physicalPlanningFailed(
            Throwable cause) {

        return new PlanningException(
                PlanningErrorCode.PHYSICAL_PLANNING_FAILED,
                "Physical Job planning failed",
                Collections.<String, String>emptyMap(),
                cause);
    }

    public PlanningErrorCode getPlanningErrorCode() {
        return errorCode;
    }

    @Override
    public Map<String, String> getParams() {
        return parameters;
    }

    private static Map<String, String> parameters(
            String key,
            String value) {

        Map<String, String> result =
                new LinkedHashMap<String, String>();
        result.put(
                key,
                safeParameter(value));
        return result;
    }

    private static Map<String, String> immutable(
            Map<String, String> source) {

        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> result =
                new LinkedHashMap<String, String>();

        for (Map.Entry<String, String> entry :
                source.entrySet()) {
            result.put(
                    safeParameter(entry.getKey()),
                    safeParameter(entry.getValue()));
        }

        return Collections.unmodifiableMap(result);
    }

    private static String safeMessage(String value) {
        String normalized = safeParameter(value);
        return normalized.length() <= 500
                ? normalized
                : normalized.substring(0, 500);
    }

    private static String safeParameter(String value) {
        if (value == null) {
            return "unknown";
        }

        String normalized = value.trim()
                .replace('\r', ' ')
                .replace('\n', ' ')
                .replace('\t', ' ');

        if (normalized.isEmpty()) {
            return "unknown";
        }

        return normalized.length() <= 200
                ? normalized
                : normalized.substring(0, 200);
    }
}
