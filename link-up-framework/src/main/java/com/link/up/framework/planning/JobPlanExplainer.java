package com.link.up.framework.planning;

import com.link.up.framework.connector.ConnectorPreparer;
import com.link.up.framework.connector.PreparedJob;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.planner.JobGraph;
import com.link.up.framework.planner.JobPlanner;

import java.util.Objects;

/** Coordinates validation and side-effect-safe physical planning. */
public final class JobPlanExplainer {

    private final ConnectorPreparer connectorPreparer;
    private final JobPlanner jobPlanner;

    public JobPlanExplainer(
            ConnectorPreparer connectorPreparer,
            JobPlanner jobPlanner) {

        this.connectorPreparer = Objects.requireNonNull(
                connectorPreparer,
                "connectorPreparer must not be null");
        this.jobPlanner = Objects.requireNonNull(
                jobPlanner,
                "jobPlanner must not be null");
    }

    /**
     * Validates the compiled definition and connector option rules without
     * creating connector runtime objects or accessing external systems.
     */
    public JobPlanResult validate(JobDefinition definition) {
        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");

        connectorPreparer.validate(job);
        return JobPlanResult.validated(
                LogicalJobPlan.from(job));
    }

    /**
     * Discovers source metadata and splits, then projects the formal JobGraph.
     * Sink preparation is replaced with a metadata-only planning stub.
     */
    public JobPlanResult explain(JobDefinition definition)
            throws Exception {

        JobDefinition job = Objects.requireNonNull(
                definition,
                "definition must not be null");
        LogicalJobPlan logicalPlan = LogicalJobPlan.from(job);
        PreparedJob preparedJob =
                connectorPreparer.prepareForExplain(job);
        JobGraph jobGraph = jobPlanner.plan(preparedJob);
        PhysicalJobPlan physicalPlan =
                PhysicalJobPlan.from(
                        jobGraph,
                        logicalPlan.getFingerprint());

        return JobPlanResult.explained(
                logicalPlan,
                physicalPlan);
    }
}
