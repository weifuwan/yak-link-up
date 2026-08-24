/**
 * Local Worker runtime orchestration and job state management.
 *
 * <p>This package owns submission/idempotency, queue admission, runtime
 * invocation, repositories, and Worker job read models. It must remain free of
 * connector-specific behavior and must not expose framework task/channel
 * internals to HTTP adapters.
 */
package com.link.up.server.runtime;
