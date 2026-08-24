package com.link.up.server.application;

public final class JobNotFoundException
        extends RuntimeException {

    public JobNotFoundException(String jobId) {
        super("Job not found: " + jobId);
    }
}
