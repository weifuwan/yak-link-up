package com.link.up.server.http.servlet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.link.up.server.application.JobNotFoundException;
import com.link.up.server.dto.JobSubmitRequest;
import com.link.up.server.http.FluxServlet;
import com.link.up.server.http.JsonSupport;
import com.link.up.server.http.RestException;
import com.link.up.server.service.JobRestService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

/** Handles one offline Job resource, details, cancellation and explicit retry. */
public final class JobResourceServlet extends FluxServlet {

    private static final int DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024;

    private final JobRestService service;
    private final int maxRequestBytes;

    public JobResourceServlet(JobRestService service) {
        this(service, DEFAULT_MAX_REQUEST_BYTES);
    }

    public JobResourceServlet(
            JobRestService service,
            int maxRequestBytes) {
        this.service = service;
        this.maxRequestBytes = maxRequestBytes;
    }

    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        List<String> segments = pathSegments(request);

        if (segments.size() == 1) {
            write(response, 200, service.jobResponse(segments.get(0)));
            return;
        }

        if (segments.size() == 2
                && "external".equals(segments.get(0))) {
            write(
                    response,
                    200,
                    service.jobByExternalExecutionId(segments.get(1)));
            return;
        }

        if (segments.size() == 2) {
            String jobId = segments.get(0);
            String resource = segments.get(1);

            if ("pipelines".equals(resource)) {
                write(response, 200, service.pipelines(jobId));
                return;
            }
            if ("tasks".equals(resource)) {
                write(response, 200, service.tasks(jobId));
                return;
            }
            if ("metrics".equals(resource)) {
                write(response, 200, service.metrics(jobId));
                return;
            }
            if ("logs".equals(resource)) {
                write(
                        response,
                        200,
                        service.logs(
                                jobId,
                                longParameter(request, "cursor", 0L),
                                intParameter(request, "limit", 500)));
                return;
            }
        }

        throw new JobNotFoundException(request.getRequestURI());
    }

    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        List<String> segments = pathSegments(request);
        if (segments.size() != 2
                || !"retry".equals(segments.get(1))) {
            throw new JobNotFoundException(request.getRequestURI());
        }

        requireJson(request);
        String body = requestBody(request, maxRequestBytes);
        try {
            JobSubmitRequest retryRequest =
                    JsonSupport.mapper().readValue(
                            body,
                            JobSubmitRequest.class);
            write(
                    response,
                    202,
                    service.retry(segments.get(0), retryRequest));
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException(
                    "Invalid JSON retry request");
        }
    }

    protected void doDelete(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        List<String> segments = pathSegments(request);
        if (segments.size() != 1) {
            throw new JobNotFoundException(request.getRequestURI());
        }

        write(
                response,
                202,
                service.cancelResponse(segments.get(0)));
    }

    private void requireJson(HttpServletRequest request) {
        String contentType = request.getContentType();
        if (contentType == null) {
            throw new RestException(
                    415,
                    "FLUX-REST-415",
                    "Retry requires Content-Type: application/json");
        }
        int separator = contentType.indexOf(';');
        String mediaType = separator >= 0
                ? contentType.substring(0, separator)
                : contentType;
        if (!"application/json".equals(
                mediaType.trim().toLowerCase(Locale.ROOT))) {
            throw new RestException(
                    415,
                    "FLUX-REST-415",
                    "Retry requires Content-Type: application/json");
        }
    }
}
