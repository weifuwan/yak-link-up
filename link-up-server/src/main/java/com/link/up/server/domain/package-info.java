/**
 * Pure Worker control-plane state and lifecycle rules.
 *
 * <p>Domain code must not own local threads, futures, semaphores, executor
 * services, HTTP types, or framework JobExecution objects.</p>
 */
package com.link.up.server.domain;
