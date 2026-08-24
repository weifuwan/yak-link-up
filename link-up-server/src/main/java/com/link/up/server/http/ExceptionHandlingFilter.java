package com.link.up.server.http;

import com.link.up.server.application.JobNotFoundException;
import com.link.up.server.application.JobRetryNotAllowedException;
import com.link.up.server.application.JobSubmissionConflictException;
import com.link.up.server.dto.ErrorResponse;
import com.link.up.server.runtime.JobStateConflictException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** REST unified exception mapping and request ID generation. */
public final class ExceptionHandlingFilter implements Filter {

    public static final String REQUEST_ID_ATTRIBUTE =
            ExceptionHandlingFilter.class.getName() + ".requestId";

    private static final Logger LOG =
            Logger.getLogger(ExceptionHandlingFilter.class.getName());

    public void init(FilterConfig filterConfig) {
    }

    public void doFilter(
            ServletRequest servletRequest,
            ServletResponse servletResponse,
            FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        String requestId = requestId(request);

        request.setAttribute(REQUEST_ID_ATTRIBUTE, requestId);
        response.setHeader("X-Request-Id", requestId);

        try {
            chain.doFilter(request, response);
        } catch (Exception exception) {
            Throwable failure = unwrap(exception);

            if (response.isCommitted()) {
                LOG.log(Level.SEVERE, logMessage(request, requestId), failure);
                throw exception instanceof ServletException
                        ? (ServletException) exception
                        : new ServletException(exception);
            }

            ErrorMapping mapping = map(failure);
            response.resetBuffer();
            response.setStatus(mapping.httpStatus);
            response.setCharacterEncoding("UTF-8");
            response.setContentType("application/json;charset=UTF-8");
            response.setHeader("Cache-Control", "no-store");

            JsonSupport.mapper().writeValue(
                    response.getOutputStream(),
                    new ErrorResponse(
                            mapping.code,
                            mapping.message,
                            requestId));

            Level level = mapping.httpStatus >= 500
                    ? Level.SEVERE
                    : Level.WARNING;
            LOG.log(level, logMessage(request, requestId), failure);
        }
    }

    public void destroy() {
    }

    private static ErrorMapping map(Throwable failure) {
        if (failure instanceof RestException) {
            RestException exception = (RestException) failure;
            return new ErrorMapping(
                    exception.getHttpStatus(),
                    exception.getCode(),
                    exception.getMessage());
        }
        if (failure instanceof JobNotFoundException) {
            return new ErrorMapping(
                    404,
                    "FLUX-JOB-NOT-FOUND",
                    failure.getMessage());
        }
        if (failure instanceof JobRetryNotAllowedException) {
            return new ErrorMapping(
                    409,
                    "FLUX-JOB-RETRY-NOT-ALLOWED",
                    failure.getMessage());
        }
        if (failure instanceof JobStateConflictException) {
            return new ErrorMapping(
                    409,
                    "FLUX-JOB-STATE-CONFLICT",
                    failure.getMessage());
        }
        if (failure instanceof JobSubmissionConflictException) {
            return new ErrorMapping(
                    409,
                    "FLUX-JOB-IDEMPOTENCY-CONFLICT",
                    failure.getMessage());
        }
        if (failure instanceof IllegalArgumentException) {
            return new ErrorMapping(
                    400,
                    "FLUX-REST-400",
                    safeMessage(failure));
        }
        if (failure instanceof RejectedExecutionException) {
            return new ErrorMapping(
                    503,
                    "FLUX-SERVER-BUSY",
                    "Job queue is full");
        }
        if (failure instanceof IllegalStateException) {
            return new ErrorMapping(
                    409,
                    "FLUX-REST-409",
                    safeMessage(failure));
        }
        return new ErrorMapping(
                500,
                "FLUX-REST-500",
                "Internal server error");
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof ServletException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String requestId(HttpServletRequest request) {
        String provided = request.getHeader("X-Request-Id");
        if (provided != null
                && provided.length() <= 128
                && provided.matches("[A-Za-z0-9._-]+")) {
            return provided;
        }
        return UUID.randomUUID().toString();
    }

    private static String safeMessage(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.trim().isEmpty()
                ? failure.getClass().getSimpleName()
                : message;
    }

    private static String logMessage(
            HttpServletRequest request,
            String requestId) {
        return "REST request failed"
                + ", requestId=" + requestId
                + ", method=" + request.getMethod()
                + ", uri=" + request.getRequestURI();
    }

    private static final class ErrorMapping {
        private final int httpStatus;
        private final String code;
        private final String message;

        private ErrorMapping(
                int httpStatus,
                String code,
                String message) {
            this.httpStatus = httpStatus;
            this.code = code;
            this.message = message;
        }
    }
}
