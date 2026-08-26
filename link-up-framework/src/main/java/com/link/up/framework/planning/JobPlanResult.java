package com.link.up.framework.planning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Stable protocol model returned by validate and explain operations. */
public final class JobPlanResult {

    public static final String CURRENT_API_VERSION =
            "link-up-plan/v1";

    public enum Mode {
        VALIDATE,
        EXPLAIN
    }

    private final String apiVersion;
    private final Mode mode;
    private final boolean valid;
    private final LogicalJobPlan logicalPlan;
    private final PhysicalJobPlan physicalPlan;
    private final List<PlanningDiagnostic> diagnostics;
    private final String text;

    private JobPlanResult(
            Mode mode,
            LogicalJobPlan logicalPlan,
            PhysicalJobPlan physicalPlan,
            List<PlanningDiagnostic> diagnostics) {

        this.apiVersion = CURRENT_API_VERSION;
        this.mode = Objects.requireNonNull(
                mode,
                "mode must not be null");
        this.valid = true;
        this.logicalPlan = Objects.requireNonNull(
                logicalPlan,
                "logicalPlan must not be null");
        this.physicalPlan = physicalPlan;
        this.diagnostics = Collections.unmodifiableList(
                new ArrayList<PlanningDiagnostic>(
                        Objects.requireNonNull(
                                diagnostics,
                                "diagnostics must not be null")));
        this.text = render(
                mode,
                logicalPlan,
                physicalPlan,
                this.diagnostics);
    }

    public static JobPlanResult validated(
            LogicalJobPlan logicalPlan) {

        List<PlanningDiagnostic> diagnostics =
                new ArrayList<PlanningDiagnostic>();
        diagnostics.add(
                new PlanningDiagnostic(
                        "PLAN_VALIDATED",
                        PlanningDiagnostic.Severity.INFO,
                        "VALIDATE",
                        "Job definition and connector options are valid"));

        return new JobPlanResult(
                Mode.VALIDATE,
                logicalPlan,
                null,
                diagnostics);
    }

    public static JobPlanResult explained(
            LogicalJobPlan logicalPlan,
            PhysicalJobPlan physicalPlan) {

        List<PlanningDiagnostic> diagnostics =
                new ArrayList<PlanningDiagnostic>();
        diagnostics.add(
                new PlanningDiagnostic(
                        "PLAN_CREATED",
                        PlanningDiagnostic.Severity.INFO,
                        "PLAN",
                        "Physical execution topology was created from the runtime JobGraph"));
        diagnostics.add(
                new PlanningDiagnostic(
                        "PLAN_SINK_PREPARATION_SKIPPED",
                        PlanningDiagnostic.Severity.WARNING,
                        "PREPARE",
                        "Sink preparation is intentionally skipped during explain to prevent target-side side effects"));

        return new JobPlanResult(
                Mode.EXPLAIN,
                logicalPlan,
                Objects.requireNonNull(
                        physicalPlan,
                        "physicalPlan must not be null"),
                diagnostics);
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public Mode getMode() {
        return mode;
    }

    public boolean isValid() {
        return valid;
    }

    public LogicalJobPlan getLogicalPlan() {
        return logicalPlan;
    }

    public PhysicalJobPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public List<PlanningDiagnostic> getDiagnostics() {
        return diagnostics;
    }

    public String getText() {
        return text;
    }

    private static String render(
            Mode mode,
            LogicalJobPlan logical,
            PhysicalJobPlan physical,
            List<PlanningDiagnostic> diagnostics) {

        StringBuilder result = new StringBuilder();
        result.append("Plan ")
                .append(logical.getFingerprint())
                .append('\n');
        result.append("Mode: ")
                .append(mode.name())
                .append('\n');
        result.append("Logical:\n");
        result.append("  Job: ")
                .append(logical.getJobName())
                .append('\n');
        result.append("  Source: ")
                .append(logical.getSourceConnectorId())
                .append('\n');
        result.append("  Sink: ")
                .append(logical.getSinkConnectorId())
                .append('\n');
        result.append("  Mapping columns: ")
                .append(logical.getColumns().size())
                .append('\n');
        result.append("  Parallelism: source=")
                .append(logical.getRuntime().getSourceParallelism())
                .append(", sink=")
                .append(logical.getRuntime().getSinkParallelism())
                .append(", pipeline=")
                .append(logical.getRuntime().getPipelineParallelism())
                .append('\n');

        if (physical != null) {
            result.append("Physical:\n");
            result.append("  Pipelines: ")
                    .append(physical.getPipelineCount())
                    .append('\n');
            result.append("  Tasks: source=")
                    .append(physical.getSourceTaskCount())
                    .append(", sink=")
                    .append(physical.getSinkTaskCount())
                    .append('\n');

            for (PhysicalJobPlan.Pipeline pipeline :
                    physical.getPipelines()) {
                result.append("  - ")
                        .append(pipeline.getPipelineId())
                        .append(" dataSet=")
                        .append(pipeline.getDataSetId())
                        .append(" splits=")
                        .append(pipeline.getSourceSplitCount())
                        .append(" sourceTasks=")
                        .append(pipeline.getSourceTaskCount())
                        .append(" sinkTasks=")
                        .append(pipeline.getSinkTaskCount())
                        .append(" columns=")
                        .append(pipeline.getOutputColumnCount())
                        .append('\n');
            }
        }

        result.append("Diagnostics:\n");
        for (PlanningDiagnostic diagnostic : diagnostics) {
            result.append("  [")
                    .append(diagnostic.getSeverity().name())
                    .append("] ")
                    .append(diagnostic.getCode())
                    .append(" (")
                    .append(diagnostic.getPhase())
                    .append("): ")
                    .append(diagnostic.getMessage())
                    .append('\n');
        }

        return result.toString();
    }
}
