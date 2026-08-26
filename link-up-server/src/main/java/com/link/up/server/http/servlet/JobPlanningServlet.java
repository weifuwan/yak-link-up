package com.link.up.server.http.servlet;

import com.link.up.server.dto.JobSubmitRequest;
import com.link.up.server.http.FluxServlet;
import com.link.up.server.service.JobPlanningService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;

/** Handles side-effect-bounded Job validation and explain requests. */
public final class JobPlanningServlet
        extends FluxServlet {

    public enum Operation {
        VALIDATE,
        EXPLAIN
    }

    private final JobPlanningService service;
    private final Operation operation;
    private final int maxRequestBytes;

    public JobPlanningServlet(
            JobPlanningService service,
            Operation operation,
            int maxRequestBytes) {

        this.service = Objects.requireNonNull(
                service,
                "service must not be null");
        this.operation = Objects.requireNonNull(
                operation,
                "operation must not be null");
        this.maxRequestBytes = maxRequestBytes;
    }

    @Override
    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        requireJsonContentType(
                request,
                "Planning requires Content-Type: application/json");

        String body = requestBody(
                request,
                maxRequestBytes);
        JobSubmitRequest planningRequest =
                JobSubmitRequestDecoder.decode(
                        body,
                        "Invalid JSON planning request");

        try {
            Object result = operation == Operation.VALIDATE
                    ? service.validate(planningRequest)
                    : service.explain(planningRequest);
            write(response, 200, result);
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new ServletException(failure);
        }
    }
}
