package com.link.up.framework.planning;

import com.link.up.api.connector.schema.ConnectorRole;
import com.link.up.framework.connector.ConnectorPreparer;
import com.link.up.framework.connector.PreparedJob;
import com.link.up.framework.connector.PreparedSource;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.JobPlanner;

import java.util.Objects;

/** Compiles validation and Explain results without executing a Job. */
public final class JobPlanExplainer {

    private final ConnectorPreparer connectorPreparer;
    private final JobPlanner jobPlanner;
    private final CapabilityNegotiator capabilityNegotiator;

    public JobPlanExplainer(
            ConnectorPreparer connectorPreparer,
            JobPlanner jobPlanner) {

        this.connectorPreparer = Objects.requireNonNull(
                connectorPreparer,
                "connectorPreparer must not be null");
        this.jobPlanner = Objects.requireNonNull(
                jobPlanner,
                "jobPlanner must not be null");
        this.capabilityNegotiator =
                new CapabilityNegotiator(
                        this.connectorPreparer);
    }

    public JobPlanResult validate(
            JobDefinition definition) {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        LogicalJobPlan logicalPlan =
                LogicalJobPlan.from(job);

        CapabilityNegotiation negotiation =
                capabilityNegotiator.negotiate(job);
        capabilityNegotiator.requireSatisfied(negotiation);

        validateSourceOptions(job);
        validateSinkOptions(job);

        return JobPlanResult.validated(
                logicalPlan,
                negotiation);
    }

    public JobPlanResult explain(
            JobDefinition definition)
            throws Exception {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        LogicalJobPlan logicalPlan =
                LogicalJobPlan.from(job);

        CapabilityNegotiation initial =
                capabilityNegotiator.negotiate(job);
        capabilityNegotiator.requireSatisfied(initial);

        validateSourceOptions(job);
        validateSinkOptions(job);

        PreparedSource<?> preparedSource =
                prepareSource(job);
        CapabilityNegotiation preparedNegotiation =
                capabilityNegotiator.negotiate(
                        job,
                        preparedSource);
        capabilityNegotiator.requireSatisfied(
                preparedNegotiation);

        PreparedJob preparedJob;
        try {
            preparedJob = connectorPreparer.prepareForExplain(
                    job,
                    preparedSource);
        } catch (PlanningException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw PlanningException.physicalPlanningFailed(
                    failure);
        }

        JobGraph jobGraph;
        try {
            jobGraph = jobPlanner.plan(preparedJob);
        } catch (PlanningException failure) {
            throw failure;
        } catch (Exception failure) {
            throw PlanningException.splitDiscoveryFailed(
                    job.getSource().getType(),
                    failure);
        }

        CapabilityNegotiation finalNegotiation =
                capabilityNegotiator.negotiate(
                        job,
                        jobGraph);
        capabilityNegotiator.requireSatisfied(
                finalNegotiation);

        final PhysicalJobPlan physicalPlan;
        try {
            physicalPlan = PhysicalJobPlan.from(
                    jobGraph,
                    logicalPlan.getFingerprint());
        } catch (RuntimeException failure) {
            throw PlanningException.physicalPlanningFailed(
                    failure);
        }

        return JobPlanResult.explained(
                logicalPlan,
                physicalPlan,
                finalNegotiation);
    }

    private PreparedSource<?> prepareSource(
            JobDefinition job)
            throws Exception {

        try {
            return connectorPreparer.prepareSource(job);
        } catch (PlanningException failure) {
            throw failure;
        } catch (Exception failure) {
            throw PlanningException.sourcePreparationFailed(
                    job.getSource().getType(),
                    failure);
        }
    }

    private void validateSourceOptions(
            JobDefinition job) {

        try {
            connectorPreparer.validateSourceOptions(
                    job.getSource());
        } catch (PlanningException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw PlanningException.connectorOptionsInvalid(
                    ConnectorRole.SOURCE,
                    job.getSource().getType(),
                    failure);
        }
    }

    private void validateSinkOptions(
            JobDefinition job) {

        try {
            connectorPreparer.validateSinkOptions(
                    job.getSink());
        } catch (PlanningException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw PlanningException.connectorOptionsInvalid(
                    ConnectorRole.SINK,
                    job.getSink().getType(),
                    failure);
        }
    }
}
