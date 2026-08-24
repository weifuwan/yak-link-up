package com.link.up.server.http.servlet;

import com.link.up.api.job.JobSpec;
import com.link.up.server.dto.JobSubmitRequest;
import com.link.up.server.http.FluxServlet;
import com.link.up.server.service.JobRestService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Handles the /api/v1/jobs collection resource. */
public final class JobsServlet
        extends FluxServlet {

    private static final Logger LOG =
            LogManager.getLogger(JobsServlet.class);

    private final JobRestService service;
    private final int maxRequestBytes;

    public JobsServlet(
            JobRestService service,
            int maxRequestBytes) {

        this.service = service;
        this.maxRequestBytes = maxRequestBytes;
    }

    @Override
    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        validateSubmitContentType(request);

        String body =
                requestBody(
                        request,
                        maxRequestBytes);

        Object result =
                isJsonContentType(request)
                        ? submitJson(
                                request,
                                body)
                        : submitLegacy(
                                request,
                                body);

        write(
                response,
                202,
                result);
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {

        String externalExecutionId =
                request.getParameter(
                        "externalExecutionId");

        if (externalExecutionId != null
                && !externalExecutionId.trim().isEmpty()) {

            write(
                    response,
                    200,
                    service.jobByExternalExecutionId(
                            externalExecutionId));
            return;
        }

        int page =
                intParameter(
                        request,
                        "page",
                        1);
        int pageSize =
                intParameter(
                        request,
                        "pageSize",
                        20);

        write(
                response,
                200,
                service.executionPage(
                        request.getParameter("status"),
                        page,
                        pageSize));
    }

    private Object submitJson(
            HttpServletRequest request,
            String body) {

        JobSubmitRequest submitRequest =
                JobSubmitRequestDecoder.decode(
                        body,
                        "Invalid JSON submit request");

        logSubmissionSummary(
                request,
                submitRequest,
                body);

        return service.submit(submitRequest);
    }

    private Object submitLegacy(
            HttpServletRequest request,
            String body) {

        LOG.info(
                "Received legacy job submission, contentType={}, bodyBytes={}",
                safeLogValue(request.getContentType()),
                utf8Length(body));

        return service.submitLegacyResponse(body);
    }

    private void logSubmissionSummary(
            HttpServletRequest request,
            JobSubmitRequest submitRequest,
            String body) {

        JobSpec jobSpec =
                submitRequest == null
                        ? null
                        : submitRequest.getJobSpec();

        LOG.info(
                "Received job submission, contentType={}, bodyBytes={}, "
                        + "externalExecutionId={}, definitionVersion={}, "
                        + "jobName={}, sourceConnector={}, sinkConnector={}",
                safeLogValue(request.getContentType()),
                utf8Length(body),
                safeLogValue(
                        submitRequest == null
                                ? null
                                : submitRequest.getExternalExecutionId()),
                submitRequest == null
                        ? null
                        : submitRequest.getDefinitionVersion(),
                safeLogValue(
                        jobSpec == null
                                ? null
                                : jobSpec.getName()),
                safeLogValue(
                        connectorId(
                                jobSpec == null
                                        ? null
                                        : jobSpec.getSource())),
                safeLogValue(
                        connectorId(
                                jobSpec == null
                                        ? null
                                        : jobSpec.getSink())));
    }

    private String connectorId(
            JobSpec.Connector connector) {
        return connector == null
                ? null
                : connector.getConnectorId();
    }

    private String safeLogValue(String value) {
        if (value == null) {
            return null;
        }

        return value.replace('\r', ' ')
                .replace('\n', ' ')
                .replace('\t', ' ');
    }

    private int utf8Length(String value) {
        return value == null
                ? 0
                : value.getBytes(
                        StandardCharsets.UTF_8)
                        .length;
    }
}
