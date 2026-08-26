package com.link.up.server.service;

import com.link.up.api.job.JobSpec;
import com.link.up.framework.job.JobConfigParser;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobSpecCompiler;
import com.link.up.framework.planning.JobPlanExplainer;
import com.link.up.framework.planning.JobPlanResult;
import com.link.up.server.dto.JobSubmitRequest;
import com.typesafe.config.ConfigException;

import java.util.Objects;

/** REST-neutral Worker boundary for validation and explain operations. */
public final class JobPlanningService {

    private final JobConfigParser hoconParser;
    private final JobSpecCompiler jobSpecCompiler;
    private final JobPlanExplainer planExplainer;

    public JobPlanningService(JobPlanExplainer planExplainer) {
        this.hoconParser = new JobConfigParser();
        this.jobSpecCompiler = new JobSpecCompiler();
        this.planExplainer = Objects.requireNonNull(
                planExplainer,
                "planExplainer must not be null");
    }

    public JobPlanResult validate(JobSubmitRequest request) {
        return planExplainer.validate(
                definition(request));
    }

    public JobPlanResult explain(JobSubmitRequest request)
            throws Exception {
        return planExplainer.explain(
                definition(request));
    }

    private JobDefinition definition(JobSubmitRequest request) {
        if (request == null) {
            throw new IllegalArgumentException(
                    "Planning request must not be null");
        }

        JobSpec jobSpec = request.getJobSpec();
        boolean hasJobSpec = jobSpec != null;
        boolean hasHocon = hasText(request.getHocon());

        if (hasJobSpec == hasHocon) {
            throw new IllegalArgumentException(
                    "Exactly one of jobSpec or hocon must be provided");
        }

        if (hasJobSpec) {
            return jobSpecCompiler.compile(jobSpec);
        }

        try {
            return hoconParser.parse(
                    request.getHocon().trim());
        } catch (ConfigException failure) {
            throw new IllegalArgumentException(
                    "Invalid HOCON job configuration",
                    failure);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
