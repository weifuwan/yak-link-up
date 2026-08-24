package com.link.up.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.link.up.api.job.JobSpec;
import com.link.up.framework.job.JobConfigParser;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobSpecCompiler;
import com.link.up.server.application.JobApplication;
import com.link.up.server.domain.JobSubmission;
import com.link.up.server.dto.JobLogPageResponse;
import com.link.up.server.dto.JobResponse;
import com.link.up.server.dto.JobSubmitRequest;
import com.link.up.server.dto.PageResponse;
import com.link.up.server.dto.PipelineResponse;
import com.link.up.server.dto.WorkerNodeResponse;
import com.link.up.server.runtime.JobExecutionMetadata;
import com.link.up.server.runtime.JobSnapshot;
import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.WorkerIdentity;
import com.typesafe.config.ConfigException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/** REST inputs to the single-node offline Worker application boundary. */
public final class JobRestService {
    private static final int MAX_PAGE_SIZE = 200;

    private final JobApplication manager;
    private final JobConfigParser hoconParser;
    private final JobSpecCompiler jobSpecCompiler;
    private final ObjectMapper protocolMapper;
    private final JobLogReader jobLogReader;
    private final WorkerIdentity workerIdentity;
    private final int maxConcurrentJobs;
    private final int maxQueuedJobs;

    public JobRestService(JobApplication manager) {
        this(manager,
                new WorkerIdentity(
                        "embedded-link-up-node",
                        "Embedded Link-Up Offline Worker",
                        WorkerIdentity.implementationVersion()),
                1,
                1);
    }

    public JobRestService(
            JobApplication manager,
            WorkerIdentity workerIdentity,
            int maxConcurrentJobs,
            int maxQueuedJobs) {
        this.manager = manager;
        this.hoconParser = new JobConfigParser();
        this.jobSpecCompiler = new JobSpecCompiler();
        this.protocolMapper = new ObjectMapper()
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        this.jobLogReader = new JobLogReader();
        this.workerIdentity = workerIdentity;
        this.maxConcurrentJobs = maxConcurrentJobs;
        this.maxQueuedJobs = maxQueuedJobs;
    }

    public JobSnapshot.Summary submit(String hocon) {
        return submitLegacyResponse(hocon).toSummary();
    }

    public JobResponse submitLegacyResponse(String hocon) {
        String token = UUID.randomUUID().toString();
        JobSubmitRequest request = new JobSubmitRequest();
        request.setExternalExecutionId("legacy-" + token);
        request.setIdempotencyKey(token);
        request.setDefinitionVersion(1);
        request.setHocon(hocon);
        return submit(request);
    }

    public JobResponse submit(JobSubmitRequest request) {
        return response(manager.submit(toSubmission(request)));
    }

    /** Explicit manual retry. The request must describe the exact same job content. */
    public JobResponse retry(
            String jobId,
            JobSubmitRequest request) {
        return response(
                manager.retry(
                        requireText(jobId, "jobId"),
                        toSubmission(request)));
    }

    private JobSubmission toSubmission(JobSubmitRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Submit request must not be null");
        }
        String externalExecutionId = requireText(
                request.getExternalExecutionId(),
                "externalExecutionId");
        String idempotencyKey = requireText(
                request.getIdempotencyKey(),
                "idempotencyKey");
        Integer definitionVersion = request.getDefinitionVersion();
        if (definitionVersion == null || definitionVersion.intValue() <= 0) {
            throw new IllegalArgumentException(
                    "definitionVersion must be greater than 0");
        }

        boolean hasJobSpec = request.getJobSpec() != null;
        boolean hasHocon = hasText(request.getHocon());
        if (hasJobSpec == hasHocon) {
            throw new IllegalArgumentException(
                    "Exactly one of jobSpec or hocon must be provided");
        }

        JobDefinition definition;
        String configDigest;
        if (hasJobSpec) {
            definition = jobSpecCompiler.compile(request.getJobSpec());
            configDigest = sha256(
                    "job-spec\n" + canonical(request.getJobSpec()));
        } else {
            String hocon = requireText(request.getHocon(), "hocon");
            definition = parseHocon(hocon);
            configDigest = sha256("hocon\n" + hocon);
        }

        return new JobSubmission(
                externalExecutionId,
                idempotencyKey,
                definitionVersion.intValue(),
                configDigest,
                definition);
    }

    public JobSnapshot job(String jobId) { return manager.getJob(jobId); }
    public JobResponse jobResponse(String jobId) { return response(manager.getJob(jobId)); }
    public JobResponse jobByExternalExecutionId(String externalExecutionId) {
        return response(manager.getJobByExternalExecutionId(externalExecutionId));
    }
    public List<PipelineResponse> pipelines(String jobId) {
        return response(manager.getJob(jobId)).getPipelines();
    }
    public List<JobSnapshot.Task> tasks(String jobId) {
        List<JobSnapshot.Task> tasks = new ArrayList<JobSnapshot.Task>();
        for (PipelineResponse pipeline : pipelines(jobId)) {
            tasks.addAll(pipeline.getTasks());
        }
        return Collections.unmodifiableList(tasks);
    }
    public JobSnapshot.Metrics metrics(String jobId) {
        return manager.getJob(jobId).getMetrics();
    }

    public JobLogPageResponse logs(
            String jobId,
            long cursor,
            int limit) {
        JobSnapshot snapshot = manager.getJob(jobId);
        JobExecutionMetadata metadata = manager.getMetadata(jobId);
        return jobLogReader.read(
                jobId,
                metadata,
                snapshot.getStatus().isTerminal(),
                cursor,
                limit);
    }

    public PageResponse<JobSnapshot.Summary> jobs(
            String statusValue,
            int page,
            int pageSize) {
        List<JobSnapshot.Summary> summaries =
                new ArrayList<JobSnapshot.Summary>();
        for (JobSnapshot snapshot : filtered(statusValue)) {
            summaries.add(snapshot.toSummary());
        }
        return page(summaries, page, pageSize);
    }

    public PageResponse<JobResponse> executionPage(
            String statusValue,
            int page,
            int pageSize) {
        List<JobResponse> responses = new ArrayList<JobResponse>();
        for (JobSnapshot snapshot : filtered(statusValue)) {
            responses.add(response(snapshot));
        }
        return page(responses, page, pageSize);
    }

    public JobSnapshot.Summary cancel(String jobId) {
        return manager.cancel(jobId).toSummary();
    }
    public JobResponse cancelResponse(String jobId) {
        return response(manager.cancel(jobId));
    }
    public WorkerNodeResponse node() {
        return new WorkerNodeResponse(
                workerIdentity,
                manager,
                maxConcurrentJobs,
                maxQueuedJobs);
    }

    private JobDefinition parseHocon(String hocon) {
        try {
            return hoconParser.parse(hocon);
        } catch (ConfigException exception) {
            throw new IllegalArgumentException(
                    "Invalid HOCON job configuration",
                    exception);
        }
    }

    private String canonical(JobSpec spec) {
        try {
            return protocolMapper.writeValueAsString(spec);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(
                    "Invalid structured jobSpec",
                    exception);
        }
    }

    private List<JobSnapshot> filtered(String statusValue) {
        ServerJobStatus status = parseStatus(statusValue);
        List<JobSnapshot> result = new ArrayList<JobSnapshot>();
        for (JobSnapshot snapshot : manager.listJobs()) {
            if (status == null || snapshot.getStatus() == status) {
                result.add(snapshot);
            }
        }
        return result;
    }

    private <T> PageResponse<T> page(
            List<T> values,
            int page,
            int pageSize) {
        validatePage(page, pageSize);
        long startLong = (long) (page - 1) * pageSize;
        int from = startLong >= values.size()
                ? values.size()
                : (int) startLong;
        int to = Math.min(values.size(), from + pageSize);
        return new PageResponse<T>(
                values.subList(from, to),
                page,
                pageSize,
                values.size());
    }

    private void validatePage(int page, int pageSize) {
        if (page < 1) {
            throw new IllegalArgumentException(
                    "page must be greater than 0");
        }
        if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "pageSize must be between 1 and " + MAX_PAGE_SIZE);
        }
    }

    private JobResponse response(JobSnapshot snapshot) {
        JobExecutionMetadata metadata =
                manager.getMetadata(snapshot.getJobId());
        return new JobResponse(
                snapshot,
                metadata,
                workerIdentity,
                manager.retryDecision(snapshot.getJobId()));
    }

    private ServerJobStatus parseStatus(String statusValue) {
        if (!hasText(statusValue)) {
            return null;
        }
        try {
            return ServerJobStatus.valueOf(
                    statusValue.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "Unknown job status: " + statusValue,
                    exception);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String requireText(String value, String name) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte current : bytes) {
                result.append(String.format(Locale.ROOT, "%02x", current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    exception);
        }
    }
}
