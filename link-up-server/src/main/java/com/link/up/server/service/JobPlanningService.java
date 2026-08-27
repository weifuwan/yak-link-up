package com.link.up.server.service;

import com.link.up.api.job.JobSpec;
import com.link.up.framework.job.JobConfigParser;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobSpecCompiler;
import com.link.up.framework.planning.JobPlanExplainer;
import com.link.up.framework.planning.JobPlanResult;
import com.link.up.framework.planning.PlanningException;
import com.link.up.server.dto.JobSubmitRequest;
import com.typesafe.config.ConfigException;

import java.util.Objects;

/** REST-neutral Worker boundary for validation and Explain operations. */
public final class JobPlanningService {

    private final JobConfigParser hoconParser;
    private final JobSpecCompiler jobSpecCompiler;
    private final JobPlanExplainer planExplainer;

    public JobPlanningService(
            JobPlanExplainer planExplainer) {

        this.hoconParser = new JobConfigParser();
        this.jobSpecCompiler = new JobSpecCompiler();
        this.planExplainer = Objects.requireNonNull(
                planExplainer,
                "planExplainer must not be null");
    }

    public JobPlanResult validate(
            JobSubmitRequest request) {
        return planExplainer.validate(
                definition(request));
    }

    public JobPlanResult explain(
            JobSubmitRequest request)
            throws Exception {
        return planExplainer.explain(
                definition(request));
    }

    private JobDefinition definition(
            JobSubmitRequest request) {

        if (request == null) {
            throw PlanningException.invalidRequest(
                    "REQUEST_NULL",
                    null);
        }

        JobSpec jobSpec = request.getJobSpec();
        boolean hasJobSpec = jobSpec != null;
        boolean hasHocon = hasText(request.getHocon());

        if (hasJobSpec == hasHocon) {
            throw PlanningException.invalidRequest(
                    "EXACTLY_ONE_DEFINITION_REQUIRED",
                    null);
        }

        if (hasJobSpec) {
            try {
                return jobSpecCompiler.compile(jobSpec);
            } catch (PlanningException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw PlanningException.invalidDefinition(
                        "JOB_SPEC",
                        failure);
            }
        }

        try {
            return hoconParser.parse(
                    request.getHocon().trim());
        } catch (PlanningException failure) {
            throw failure;
        } catch (ConfigException failure) {
            throw PlanningException.invalidDefinition(
                    "HOCON",
                    failure);
        } catch (RuntimeException failure) {
            throw PlanningException.invalidDefinition(
                    "HOCON",
                    failure);
        }
    }

    private static boolean hasText(String value) {
        return value != null
                && !value.trim().isEmpty();
    }
}
