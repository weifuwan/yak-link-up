package com.link.up.server.application.port;

/**
 * Application port for generating Worker-local job identifiers.
 */
public interface JobIdGenerator {
    String nextId();
}
