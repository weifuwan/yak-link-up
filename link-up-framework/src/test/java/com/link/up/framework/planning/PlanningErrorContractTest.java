package com.link.up.framework.planning;

import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.connector.schema.ConnectorRole;
import com.link.up.api.exception.FluxErrorCategory;
import com.link.up.api.exception.FluxErrorPhase;
import com.link.up.api.exception.FluxRetryScope;
import com.link.up.api.exception.error.FluxApiErrorCode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PlanningErrorContractTest {

    @Test
    public void requiredCapabilityErrorShouldBeMachineReadable() {
        PlanningException failure =
                PlanningException.requiredCapabilityMissing(
                        ConnectorRole.SINK,
                        "doris",
                        ConnectorCapability.TWO_PHASE_COMMIT);

        assertEquals(
                "PLAN-005",
                failure.getFluxErrorCode().getCode());
        assertEquals(
                FluxErrorCategory.CAPABILITY,
                failure.getErrorCategory());
        assertEquals(
                FluxErrorPhase.CAPABILITY_NEGOTIATION,
                failure.getErrorPhase());
        assertFalse(failure.isRetryable());
        assertEquals(
                FluxRetryScope.NONE,
                failure.getRetryScope());
        assertEquals(
                "SINK",
                failure.getParams().get("role"));
        assertEquals(
                "doris",
                failure.getParams().get("connectorId"));
        assertEquals(
                "TWO_PHASE_COMMIT",
                failure.getParams().get("capability"));
    }

    @Test
    public void sourceDiscoveryFailureShouldNotExposeCauseMessage() {
        PlanningException failure =
                PlanningException.sourcePreparationFailed(
                        "jdbc",
                        new IllegalStateException(
                                "password=TEST_ONLY_SECRET"));

        assertEquals(
                PlanningErrorCode.SOURCE_PREPARATION_FAILED,
                failure.getPlanningErrorCode());
        assertTrue(failure.isRetryable());
        assertEquals(
                FluxRetryScope.JOB,
                failure.getRetryScope());
        assertFalse(
                failure.getMessage().contains(
                        "TEST_ONLY_SECRET"));
        assertFalse(
                failure.getParams().toString().contains(
                        "TEST_ONLY_SECRET"));
    }

    @Test
    public void sinkPreparationFailureShouldBeConservativelyNonRetryable() {
        PlanningException failure =
                PlanningException.sinkPreparationFailed(
                        "doris",
                        new IllegalStateException(
                                "password=TEST_ONLY_SECRET"));

        assertEquals(
                PlanningErrorCode.SINK_PREPARATION_FAILED,
                failure.getPlanningErrorCode());
        assertEquals(
                FluxErrorPhase.SINK_PREPARATION,
                failure.getErrorPhase());
        assertFalse(failure.isRetryable());
        assertEquals(
                FluxRetryScope.NONE,
                failure.getRetryScope());
        assertFalse(
                failure.getMessage().contains(
                        "TEST_ONLY_SECRET"));
    }

    @Test
    public void legacyApiErrorsShouldExposeStructuredDefaults() {
        assertEquals(
                FluxErrorCategory.VALIDATION,
                FluxApiErrorCode.OPTION_VALIDATION_FAILED
                        .getCategory());
        assertEquals(
                FluxErrorPhase.OPTION_VALIDATION,
                FluxApiErrorCode.OPTION_VALIDATION_FAILED
                        .getPhase());
        assertFalse(
                FluxApiErrorCode.OPTION_VALIDATION_FAILED
                        .isRetryable());
        assertEquals(
                FluxRetryScope.NONE,
                FluxApiErrorCode.OPTION_VALIDATION_FAILED
                        .getRetryScope());
    }
}
