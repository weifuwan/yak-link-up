# ADR-0006: Separate the Server control plane into application, domain, and infrastructure roles

- Status: Accepted
- Date: 2026-08-24

## Context

Phases 1-5 established framework/runtime roles and connector boundaries. The remaining large ownership hotspot was the
single-node Worker control plane. `LocalJobManager` simultaneously owned:

- idempotency/external-execution indexes;
- queue admission;
- job thread creation and interruption;
- framework `JobExecution` binding;
- job lifecycle transitions;
- terminal snapshot persistence;
- query/list/cancel use cases;
- shutdown timeout and `LOST` handling.

The code worked, but the class mixed application policy, domain state, infrastructure concurrency, framework integration,
and read-model assembly. This made each concern harder to test independently and made future persistence/recovery work
likely to depend on local Thread/Future objects.

## Decision

The Worker control plane is split by responsibility:

```text
HTTP / registration adapters
        |
        v
JobApplication
        |
        v
JobApplicationService
        |
        +--> JobExecutionState          domain lifecycle state
        |
        +--> JobSubmissionRegistry      application idempotency index
        |
        +--> JobRuntimeScheduler        application port
        +--> JobExecutor                application port
        +--> JobRepository              application port
        +--> JobIdGenerator             application port
                  |
                  v
             infrastructure
```

Concrete local adapters are composed only in `FluxServer`:

```text
LocalJobRuntimeScheduler
LocalJobExecutor
InMemoryJobRepository
LocalJobIdGenerator
```

### Domain state

`domain.JobExecutionState` owns:

- job/submission identity;
- lifecycle status;
- submitted/queued/start/end timestamps;
- cancellation intent;
- state version and auditable transitions;
- terminal `JobResult` / failure information;
- run/log identity values.

It does not own:

- `Thread`;
- `Future`;
- `Semaphore`;
- `ExecutorService`;
- framework `JobExecution` objects.

`domain.JobStateMachine` remains the pure transition rule owner.

### Application service

`JobApplicationService` owns Worker use-case semantics:

- submit and idempotency/content conflict checks;
- active-vs-history lookup;
- query/list/cancel use cases;
- final status derivation;
- terminal archival through `JobRepository`;
- Worker counts exposed to adapters.

It does not create or interrupt threads and does not bind framework runtime objects.

### Runtime scheduler port

`JobRuntimeScheduler` is the boundary for local admission/thread ownership. The current local adapter owns:

- the admission semaphore;
- job threads;
- framework `JobExecution` binding for cancellation/live metrics;
- interruption and shutdown waiting;
- `LOST` notification after shutdown timeout.

It communicates lifecycle meaning back to the application through semantic callbacks:

```text
onQueued
onStarting
onJobLogCreated
onCompleted
onLost
```

The scheduler does not own idempotency indexes, repository persistence, or final Worker status policy.

### Framework execution port

`JobExecutor` stays a narrow application port. `LocalJobExecutor` remains the adapter that creates one isolated
`LocalFluxEngine` per Worker job.

### Repository and identity ports

`JobRepository` and `JobIdGenerator` move to `application.port`; `InMemoryJobRepository` and `LocalJobIdGenerator` become
infrastructure adapters.

### Read-model compatibility

`JobSnapshot`, `JobExecutionMetadata`, `ServerJobStatus`, `JobStateTransition`, and `WorkerIdentity` remain in the existing
`server.runtime` package during Phase 6. They are stable Worker protocol/read-model value types and moving them would add
REST DTO churn unrelated to the ownership problem being solved.

The `server.runtime` package is therefore redefined as a compatibility/read-model package, not an orchestration package.
A future protocol-focused phase may rename/move these types separately if there is a concrete benefit.

`JobSnapshotFactory` is changed to consume `JobExecutionState` plus live `JobMetrics`; it no longer reads framework
`JobExecution` directly.

## Compatibility

This phase preserves:

- Worker REST endpoints and JSON fields;
- `externalExecutionId` and idempotency semantics;
- job lifecycle status names and transition rules;
- queue-full error mapping;
- cancellation propagation to framework execution;
- terminal history behavior;
- shutdown timeout and `LOST` semantics;
- live metrics/read-model generation;
- control-plane registration payloads.

The current local admission semantics are deliberately retained. This architecture phase does not redefine the meaning of
`jobThreads` / `maxQueuedJobs`; scheduling policy can evolve later behind `JobRuntimeScheduler` without moving application
or domain code again.

## Architecture guards

Tests enforce that:

- `JobExecutionState` does not own Thread/Future/Semaphore/Executor/framework `JobExecution` types;
- `JobApplicationService` does not own local concurrency/runtime adapter types;
- the REST service does not depend directly on infrastructure implementations.

## Consequences

Positive:

- `LocalJobManager` and the mixed `JobExecutionHandle` are removed;
- lifecycle state can be tested without local threads;
- thread ownership has one explicit infrastructure role;
- application policies are replaceable/testable through ports;
- persistence/recovery can evolve without serializing local runtime objects;
- HTTP and registration adapters depend on a use-case boundary rather than a local runtime manager;
- `FluxServer` becomes the explicit composition root.

Trade-offs:

- stable read-model classes remain under the historical `runtime` package for compatibility;
- the local scheduler still reflects current single-node execution/admission behavior rather than introducing a new
  distributed or fixed-pool scheduler;
- durable recovery is not implemented in this phase.

## Non-goals

Phase 6 does not introduce:

- distributed Worker scheduling;
- durable repository storage;
- restart recovery;
- retry/failover policy;
- changes to REST/registration protocols;
- changes to framework `JobGraph` / `ExecutionGraph` runtime behavior.

Those can now be added behind the new application ports without rebuilding the Server control-plane boundary.
