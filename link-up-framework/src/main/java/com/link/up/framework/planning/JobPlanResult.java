package com.link.up.framework.planning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Stable protocol model returned by validate and explain operations. */
public final class JobPlanResult {

    public static final String CURRENT_API_VERSION =
            "link-up-plan/v2";

    public enum Mode {
        VALIDATE,
        EXPLAIN
    }

    private final String apiVersion;
    private final Mode mode;
    private final boolean valid;
    private final LogicalJobPlan logicalPlan;
    private final PhysicalJobPlan physicalPlan;
    private final CapabilityNegotiation capabilityNegotiation;
    private final List<PlanningDiagnostic> diagnostics;
    private final String text;

    private JobPlanResult(
            Mode mode,
            LogicalJobPlan logicalPlan,
            PhysicalJobPlan physicalPlan,
            CapabilityNegotiation capabilityNegotiation,
            List<PlanningDiagnostic> diagnostics) {

        this.apiVersion = CURRENT_API_VERSION;
        this.mode = Objects.requireNonNull(
                mode,
                "mode must not be null");
        this.logicalPlan = Objects.requireNonNull(
                logicalPlan,
                "logicalPlan must not be null");
        this.physicalPlan = physicalPlan;
        this.capabilityNegotiation = capabilityNegotiation;
        this.valid = capabilityNegotiation == null
                || !capabilityNegotiation.isRejected();
        this.diagnostics = Collections.unmodifiableList(
                new ArrayList<PlanningDiagnostic>(
                        Objects.requireNonNull(
                                diagnostics,
                                "diagnostics must not be null")));
        this.text = render(
                mode,
                logicalPlan,
                physicalPlan,
                capabilityNegotiation,
                this.diagnostics);
    }

    /** Compatibility overload retained for embedded callers. */
    public static JobPlanResult validated(
            LogicalJobPlan logicalPlan) {
        return validated(logicalPlan, null);
    }

    public static JobPlanResult validated(
            LogicalJobPlan logicalPlan,
            CapabilityNegotiation capabilityNegotiation) {

        List<PlanningDiagnostic> diagnostics =
                new ArrayList<PlanningDiagnostic>();
        diagnostics.add(
                new PlanningDiagnostic(
                        "PLAN_VALIDATED",
                        PlanningDiagnostic.Severity.INFO,
                        "VALIDATE",
                        "Job definition and Connector options are valid"));
        addNegotiationDiagnostics(
                diagnostics,
                capabilityNegotiation);

        return new JobPlanResult(
                Mode.VALIDATE,
                logicalPlan,
                null,
                capabilityNegotiation,
                diagnostics);
    }

    /** Compatibility overload retained for embedded callers. */
    public static JobPlanResult explained(
            LogicalJobPlan logicalPlan,
            PhysicalJobPlan physicalPlan) {
        return explained(
                logicalPlan,
                physicalPlan,
                null);
    }

    public static JobPlanResult explained(
            LogicalJobPlan logicalPlan,
            PhysicalJobPlan physicalPlan,
            CapabilityNegotiation capabilityNegotiation) {

        List<PlanningDiagnostic> diagnostics =
                new ArrayList<PlanningDiagnostic>();
        diagnostics.add(
                new PlanningDiagnostic(
                        "PLAN_CREATED",
                        PlanningDiagnostic.Severity.INFO,
                        "PHYSICAL_PLANNING",
                        "Physical execution topology was projected from the runtime JobGraph"));
        diagnostics.add(
                new PlanningDiagnostic(
                        "PLAN_SINK_PREPARATION_SKIPPED",
                        PlanningDiagnostic.Severity.WARNING,
                        "SINK_PREPARATION",
                        "Sink preparation is skipped during Explain to prevent target-side side effects"));
        addNegotiationDiagnostics(
                diagnostics,
                capabilityNegotiation);

        return new JobPlanResult(
                Mode.EXPLAIN,
                logicalPlan,
                Objects.requireNonNull(
                        physicalPlan,
                        "physicalPlan must not be null"),
                capabilityNegotiation,
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

    public CapabilityNegotiation getCapabilityNegotiation() {
        return capabilityNegotiation;
    }

    public List<PlanningDiagnostic> getDiagnostics() {
        return diagnostics;
    }

    public String getText() {
        return text;
    }

    private static void addNegotiationDiagnostics(
            List<PlanningDiagnostic> diagnostics,
            CapabilityNegotiation negotiation) {

        if (negotiation != null) {
            diagnostics.addAll(
                    negotiation.diagnostics());
        }
    }

    private static String render(
            Mode mode,
            LogicalJobPlan logical,
            PhysicalJobPlan physical,
            CapabilityNegotiation negotiation,
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

        if (negotiation != null) {
            result.append("Capabilities:\n");
            result.append("  Status: ")
                    .append(negotiation.getStatus().name())
                    .append('\n');
            renderEndpoint(
                    result,
                    negotiation.getSource());
            renderEndpoint(
                    result,
                    negotiation.getSink());
        }

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

    private static void renderEndpoint(
            StringBuilder result,
            CapabilityNegotiation.Endpoint endpoint) {

        result.append("  ")
                .append(endpoint.getRole().name())
                .append(" ")
                .append(endpoint.getConnectorId())
                .append(": status=")
                .append(endpoint.getStatus().name())
                .append(" supported=")
                .append(endpoint.getSupported())
                .append(" required=")
                .append(endpoint.getRequired())
                .append(" preferred=")
                .append(endpoint.getPreferred())
                .append(" derivedRequired=")
                .append(endpoint.getDerivedRequired())
                .append(" missingRequired=")
                .append(endpoint.getMissingRequired())
                .append(" missingPreferred=")
                .append(endpoint.getMissingPreferred())
                .append('\n');
    }
}
