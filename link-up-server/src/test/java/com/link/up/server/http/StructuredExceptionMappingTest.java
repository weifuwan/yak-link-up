package com.link.up.server.http;

import com.link.up.api.configuration.util.OptionValidationException;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.connector.schema.ConnectorRole;
import com.link.up.api.exception.FluxErrorCategory;
import com.link.up.api.exception.FluxErrorPhase;
import com.link.up.api.exception.FluxRetryScope;
import com.link.up.framework.planning.PlanningException;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StructuredExceptionMappingTest {

    @Test
    public void capabilityFailureShouldMapToUnprocessableEntity()
            throws Exception {

        Object mapping = map(
                PlanningException.requiredCapabilityMissing(
                        ConnectorRole.SINK,
                        "doris",
                        ConnectorCapability.TWO_PHASE_COMMIT));

        assertEquals(422, field(mapping, "httpStatus"));
        assertEquals("PLAN-005", field(mapping, "code"));
        assertEquals(
                FluxErrorCategory.CAPABILITY,
                field(mapping, "category"));
        assertEquals(
                FluxErrorPhase.CAPABILITY_NEGOTIATION,
                field(mapping, "phase"));
        assertEquals(Boolean.FALSE, field(mapping, "retryable"));
        assertEquals(
                FluxRetryScope.NONE,
                field(mapping, "retryScope"));

        @SuppressWarnings("unchecked")
        Map<String, String> parameters =
                (Map<String, String>) field(
                        mapping,
                        "parameters");
        assertEquals(
                "TWO_PHASE_COMMIT",
                parameters.get("capability"));
    }

    @Test
    public void retryableDiscoveryFailureShouldMapToServiceUnavailable()
            throws Exception {

        Object mapping = map(
                PlanningException.sourcePreparationFailed(
                        "jdbc",
                        new IllegalStateException("unavailable")));

        assertEquals(503, field(mapping, "httpStatus"));
        assertEquals("PLAN-006", field(mapping, "code"));
        assertEquals(Boolean.TRUE, field(mapping, "retryable"));
        assertEquals(
                FluxRetryScope.JOB,
                field(mapping, "retryScope"));
    }

    @Test
    public void optionValidationShouldMapToBadRequest()
            throws Exception {

        Object mapping = map(
                new OptionValidationException(
                        "invalid option"));

        assertEquals(400, field(mapping, "httpStatus"));
        assertEquals("API-02", field(mapping, "code"));
        assertEquals(
                FluxErrorCategory.VALIDATION,
                field(mapping, "category"));
    }

    private Object map(Throwable failure)
            throws Exception {

        Method method = ExceptionHandlingFilter.class
                .getDeclaredMethod(
                        "map",
                        Throwable.class);
        method.setAccessible(true);
        return method.invoke(null, failure);
    }

    private Object field(
            Object target,
            String name)
            throws Exception {

        Field field = target.getClass()
                .getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
