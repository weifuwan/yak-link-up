package com.link.up.server.http.servlet;

import com.link.up.server.application.JobNotFoundException;
import com.link.up.server.dto.JobSubmitRequest;
import com.link.up.server.http.FluxServlet;
import com.link.up.server.http.RestException;
import com.link.up.server.service.JobEventRestService;
import com.link.up.server.service.JobHistoryRestService;
import com.link.up.server.service.JobRestService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/** Handles one offline Job resource, details, history, cancellation and retry. */
public final class JobResourceServlet
        extends FluxServlet {

    private static final int DEFAULT_MAX_REQUEST_BYTES =
            1024 * 1024;

    private final JobRestService service;
    private final JobEventRestService eventService;
    private final JobHistoryRestService historyService;
    private final int maxRequestBytes;

    public JobResourceServlet(JobRestService service) {
        this(
                service,
                null,
                null,
                DEFAULT_MAX_REQUEST_BYTES);
    }

    public JobResourceServlet(
            JobRestService service,
            int maxRequestBytes) {
        this(
                service,
                null,
                null,
                maxRequestBytes);
    }

    public JobResourceServlet(
            JobRestService service,
            JobEventRestService eventService,
            int maxRequestBytes) {
        this(
                service,
                eventService,
                null,
                maxRequestBytes);
    }

    public JobResourceServlet(
            JobRestService service,
            JobEventRestService eventService,
            JobHistoryRestService historyService,
            int maxRequestBytes) {

        this.service = service;
        this.eventService = eventService;
        this.historyService = historyService;
        this.maxRequestBytes = maxRequestBytes;
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        List<String> segments =
                pathSegments(request);

        if (segments.size() == 1) {
            write(
                    response,
                    200,
                    service.jobResponse(
                            segments.get(0)));
            return;
        }

        if (segments.size() == 2
                && "external".equals(segments.get(0))) {

            write(
                    response,
                    200,
                    service.jobByExternalExecutionId(
                            segments.get(1)));
            return;
        }

        if (segments.size() == 2) {
            writeNestedResource(
                    request,
                    response,
                    segments.get(0),
                    segments.get(1));
            return;
        }

        throw notFound(request);
    }

    @Override
    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        List<String> segments =
                pathSegments(request);

        if (segments.size() != 2
                || !"retry".equals(segments.get(1))) {
            throw notFound(request);
        }

        requireJsonContentType(
                request,
                "Retry requires Content-Type: application/json");

        String body =
                requestBody(
                        request,
                        maxRequestBytes);

        JobSubmitRequest retryRequest =
                JobSubmitRequestDecoder.decode(
                        body,
                        "Invalid JSON retry request");

        write(
                response,
                202,
                service.retry(
                        segments.get(0),
                        retryRequest));
    }

    @Override
    protected void doDelete(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        List<String> segments =
                pathSegments(request);

        if (segments.size() != 1) {
            throw notFound(request);
        }

        write(
                response,
                202,
                service.cancelResponse(
                        segments.get(0)));
    }

    private void writeNestedResource(
            HttpServletRequest request,
            HttpServletResponse response,
            String jobId,
            String resource)
            throws IOException {

        if ("pipelines".equals(resource)) {
            write(
                    response,
                    200,
                    service.pipelines(jobId));
            return;
        }

        if ("tasks".equals(resource)) {
            write(
                    response,
                    200,
                    service.tasks(jobId));
            return;
        }

        if ("metrics".equals(resource)) {
            write(
                    response,
                    200,
                    service.metrics(jobId));
            return;
        }

        if ("logs".equals(resource)) {
            write(
                    response,
                    200,
                    service.logs(
                            jobId,
                            longParameter(
                                    request,
                                    "cursor",
                                    0L),
                            intParameter(
                                    request,
                                    "limit",
                                    500)));
            return;
        }

        if ("events".equals(resource)) {
            if (eventService == null) {
                throw new RestException(
                        501,
                        "FLUX-JOB-EVENT-HISTORY-DISABLED",
                        "Job event history is not configured");
            }

            write(
                    response,
                    200,
                    eventService.events(
                            jobId,
                            longParameter(
                                    request,
                                    "afterSequence",
                                    0L),
                            intParameter(
                                    request,
                                    "limit",
                                    200)));
            return;
        }

        if ("history".equals(resource)) {
            if (historyService == null) {
                throw new RestException(
                        501,
                        "FLUX-JOB-HISTORY-DISABLED",
                        "Job execution history is not configured");
            }

            write(
                    response,
                    200,
                    historyService.history(
                            jobId,
                            longParameter(
                                    request,
                                    "afterSequence",
                                    0L),
                            intParameter(
                                    request,
                                    "limit",
                                    200)));
            return;
        }

        throw notFound(request);
    }

    private JobNotFoundException notFound(
            HttpServletRequest request) {
        return new JobNotFoundException(
                request.getRequestURI());
    }
}
