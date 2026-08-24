package com.link.up.server.http;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Base servlet for Link-Up REST adapters. */
public abstract class FluxServlet
        extends HttpServlet {

    protected void write(
            HttpServletResponse response,
            int status,
            Object body)
            throws IOException {

        response.setStatus(status);
        response.setCharacterEncoding("UTF-8");
        response.setContentType(
                "application/json;charset=UTF-8");
        response.setHeader(
                "Cache-Control",
                "no-store");

        JsonSupport.mapper()
                .writeValue(
                        response.getOutputStream(),
                        body);
    }

    protected String requestBody(
            HttpServletRequest request,
            int maxRequestBytes)
            throws IOException {

        ServletInputStream input =
                request.getInputStream();
        ByteArrayOutputStream output =
                new ByteArrayOutputStream();

        byte[] buffer = new byte[4096];
        int total = 0;
        int read;

        while ((read = input.read(buffer)) >= 0) {
            total += read;

            if (total > maxRequestBytes) {
                throw new RestException(
                        413,
                        "FLUX-REST-413",
                        "Request body exceeds "
                                + maxRequestBytes
                                + " bytes");
            }

            output.write(
                    buffer,
                    0,
                    read);
        }

        return new String(
                output.toByteArray(),
                StandardCharsets.UTF_8);
    }

    protected int intParameter(
            HttpServletRequest request,
            String name,
            int defaultValue) {

        String value = request.getParameter(name);

        if (value == null
                || value.trim().isEmpty()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    name + " must be an integer");
        }
    }

    protected long longParameter(
            HttpServletRequest request,
            String name,
            long defaultValue) {

        String value = request.getParameter(name);

        if (value == null
                || value.trim().isEmpty()) {
            return defaultValue;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    name + " must be a long integer");
        }
    }

    protected List<String> pathSegments(
            HttpServletRequest request) {

        String path = request.getPathInfo();
        List<String> segments =
                new ArrayList<String>();

        if (path == null
                || "/".equals(path)) {
            return segments;
        }

        for (String value : path.split("/")) {
            if (value != null
                    && !value.trim().isEmpty()) {
                segments.add(value.trim());
            }
        }

        return segments;
    }

    protected void validateSubmitContentType(
            HttpServletRequest request) {

        String mediaType = mediaType(request);

        if (mediaType == null) {
            return;
        }

        if (!"application/hocon".equals(mediaType)
                && !"text/plain".equals(mediaType)
                && !"application/json".equals(mediaType)) {

            throw new RestException(
                    415,
                    "FLUX-REST-415",
                    "Unsupported Content-Type: "
                            + mediaType);
        }
    }

    protected boolean isJsonContentType(
            HttpServletRequest request) {
        return "application/json".equals(
                mediaType(request));
    }

    protected void requireJsonContentType(
            HttpServletRequest request,
            String message) {

        if (isJsonContentType(request)) {
            return;
        }

        throw new RestException(
                415,
                "FLUX-REST-415",
                message);
    }

    protected final RestException methodNotAllowed(
            HttpServletRequest request) {

        return new RestException(
                405,
                "FLUX-REST-405",
                "HTTP method "
                        + request.getMethod()
                        + " is not allowed");
    }

    protected void doPut(
            HttpServletRequest request,
            HttpServletResponse response) {
        throw methodNotAllowed(request);
    }

    protected void doDelete(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {
        throw methodNotAllowed(request);
    }

    protected void doPost(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {
        throw methodNotAllowed(request);
    }

    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException {
        throw methodNotAllowed(request);
    }

    private String mediaType(
            HttpServletRequest request) {

        String contentType = request.getContentType();

        if (contentType == null) {
            return null;
        }

        int separator = contentType.indexOf(';');
        String value =
                separator >= 0
                        ? contentType.substring(
                                0,
                                separator)
                        : contentType;

        return value.trim()
                .toLowerCase(Locale.ROOT);
    }
}
