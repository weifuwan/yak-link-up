/**
 * Worker read-model and protocol compatibility types.
 *
 * <p>Control-plane orchestration no longer belongs here. Use
 * {@code server.application} for use cases, {@code server.domain} for lifecycle
 * rules/state, and {@code server.infrastructure} for local runtime adapters.
 * JobSnapshot/JobExecutionMetadata and stable protocol value objects remain in
 * this package in Phase 6 to avoid coupling the architecture refactor to REST
 * wire-type churn.</p>
 */
package com.link.up.server.runtime;
